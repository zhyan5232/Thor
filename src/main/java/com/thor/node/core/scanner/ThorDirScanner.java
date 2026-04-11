package com.thor.node.core.scanner;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thor.node.core.QueueManager;
import com.thor.node.core.entity.ThorNode;
import com.thor.node.core.entity.ThorTaskCfg;
import com.thor.node.core.entity.ThorTaskInstance;
import com.thor.node.core.mapper.ThorNodeMapper;
import com.thor.node.core.mapper.ThorTaskCfgMapper;
import com.thor.node.core.mapper.ThorTaskInstanceMapper;
import com.thor.node.core.model.TaskNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.*;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class ThorDirScanner {
    private static final Logger log = LoggerFactory.getLogger(ThorDirScanner.class);

    @Value("${thor.node.data-home}")
    private String dataHome;

    @Autowired private QueueManager queueManager;
    @Autowired private ThorTaskCfgMapper taskCfgMapper;
    @Autowired private ThorNodeMapper nodeMapper;
    @Autowired private ThorTaskInstanceMapper instanceMapper;

    private final ObjectMapper mapper = new ObjectMapper();
    // 【新增】：最近处理过的文件缓存，用于去重过滤 (使用线程安全的 Map)
    private final java.util.Map<String, Long> processedFiles = new java.util.concurrent.ConcurrentHashMap<>();
    @PostConstruct
    public void init() {
        File dir = new File(dataHome);
        if (!dir.exists() && dir.mkdirs()) {
            log.info("已自动创建数据监控目录: {}", dataHome);
        }
        Thread scannerThread = new Thread(this::startWatchService, "Thor-ScannerWorker");
        scannerThread.setDaemon(true);
        scannerThread.start();
        log.info(">>> [引信激活] Thor 自动发现引擎已挂载，全天候监听目录: {}", dataHome);
    }

    private void startWatchService() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(dataHome);
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                if (key == null) continue;
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                    Path changed = (Path) event.context();
                    String fileName = changed.toString();

                    if (fileName.endsWith(".ind")) {
                        handleIndFileReady(path.resolve(fileName).toFile());
                    }
                }
                if (!key.reset()) break;
            }
        } catch (Exception e) {
            log.error("目录监听引擎发生致命异常，监听已停止", e);
        }
    }

    private void handleIndFileReady(File indFile) {
        try {
            // 防抖缓冲
            Thread.sleep(500);
            if (indFile.length() == 0) return;

            // ==========================================
            // 【核心修复：事件去重过滤】
            // 如果这个文件在 3 秒内被处理过，直接抛弃后续的冗余事件
            String fileKey = indFile.getAbsolutePath();
            Long lastProcessTime = processedFiles.get(fileKey);
            if (lastProcessTime != null && (System.currentTimeMillis() - lastProcessTime < 3000)) {
                return; // 拦截重复事件
            }
            processedFiles.put(fileKey, System.currentTimeMillis());
            // ==========================================

            String datFileName = indFile.getName().substring(0, indFile.getName().lastIndexOf('.')) + ".dat";
            File datFile = new File(indFile.getParentFile(), datFileName);

            if (!datFile.exists() || !validateIndFile(indFile, datFile)) {
                return;
            }

            // ================== 【核心大脑调度逻辑】 ==================

            // 1. 匹配路由配置 (动态去数据库问：这个文件该怎么处理？)
            ThorTaskCfg routeCfg = taskCfgMapper.selectOne(new QueryWrapper<ThorTaskCfg>()
                    .eq("file_pattern", datFileName)
                    .eq("is_active", 1)
                    .last("LIMIT 1"));

            if (routeCfg == null) {
                log.warn(">>> [中心调度] 拦截！未在数据库找到文件 {} 的路由规则", datFileName);
                return;
            }

            // 2. 动态寻址目标节点 (去数据库问：目标节点在哪？)
            ThorNode targetNode = nodeMapper.selectOne(new QueryWrapper<ThorNode>()
                    .eq("node_name", routeCfg.getDstNode())
                    .eq("status", "ONLINE")
                    .last("LIMIT 1"));

            if (targetNode == null) {
                log.error(">>> [中心调度] 失败！目标节点 {} 离线或不存在", routeCfg.getDstNode());
                return;
            }

            // 3. 生成任务流水账单落盘 (记忆写入)
            String taskId = "TASK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            ThorTaskInstance instance = new ThorTaskInstance();
            instance.setTaskId(taskId);
            instance.setCfgId(routeCfg.getId());
            instance.setFileName(datFileName);
            instance.setTotalSize(datFile.length());
            instance.setStatus("PROCESSING");
            instance.setStartTime(new Date());
            instanceMapper.insert(instance);

            // 4. 组装动态发车指令
            ObjectNode cfg = mapper.createObjectNode();
            cfg.put("src_file_name", datFile.getAbsolutePath());
            cfg.put("dst_address", targetNode.getIpAddress());
            cfg.put("dst_port", targetNode.getPort());
            cfg.put("process_type", routeCfg.getProcessType());
            cfg.put("from_charset", routeCfg.getFromCharset());
            cfg.put("to_charset", routeCfg.getToCharset());

            TaskNode task = new TaskNode();
            task.setTaskId(taskId);
            task.setProcCfg(cfg);

            queueManager.pushToProcessQueue(task);
            log.info(">>> [中心调度] 任务 {} 已生成流水并落盘，路由至目标节点: {}", taskId, targetNode.getNodeName());

        } catch (Exception e) {
            log.error("处理 .ind 校验文件异常: {}", indFile.getName(), e);
        }
    }

    private boolean validateIndFile(File indFile, File datFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(indFile))) {
            String encoding = br.readLine();
            String metaLine = br.readLine();
            if (encoding == null || metaLine == null) return false;
            String expectedFileName = metaLine.trim().split("\\s+")[0];
            return expectedFileName.equals(datFile.getName());
        } catch (Exception e) {
            return false;
        }
    }
}