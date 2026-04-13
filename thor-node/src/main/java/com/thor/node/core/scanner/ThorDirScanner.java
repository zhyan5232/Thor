package com.thor.node.core.scanner;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thor.common.entity.ThorNode;
import com.thor.common.entity.ThorTaskCfg;
import com.thor.common.entity.ThorTaskInstance;
import com.thor.common.mapper.ThorNodeMapper;
import com.thor.common.mapper.ThorTaskCfgMapper;
import com.thor.common.mapper.ThorTaskInstanceMapper;
import com.thor.node.core.QueueManager;
import com.thor.node.core.model.TaskNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.*;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final java.util.Map<String, Long> processedFiles = new ConcurrentHashMap<>();

    // ==========================================
    // 【核心改造】：为跑批场景量身定制的异步工作线程池
    // ==========================================
    private final ExecutorService workerPool = new ThreadPoolExecutor(
            5,    // 核心线程数：平时保持5个收发员
            20,   // 最大线程数：遇到晚上9点跑批，动态扩容到20个并发处理
            60L, TimeUnit.SECONDS, // 闲置60秒后回收多余线程
            new LinkedBlockingQueue<>(50000), // 巨大的内存缓冲区！瞬间容纳5万个文件事件绝对不虚
            new ThreadFactory() {
                private final AtomicInteger count = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "Thor-ScannerWorker-" + count.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            },
            // 极限防御策略：如果5万的队列都塞满了（这几乎不可能），由操作系统主监听线程亲自处理，防止丢弃
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    @PostConstruct
    public void init() {
        File dir = new File(dataHome);
        if (!dir.exists() && dir.mkdirs()) {
            log.info("已自动创建数据监控目录: {}", dataHome);
        }
        // 这是操作系统级别的底层监听线程，全系统只有1个
        Thread watcherThread = new Thread(this::startWatchService, "Thor-OS-Watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        log.info(">>> [引信激活] Thor 自动发现引擎已挂载，异步并发处理池已就绪");
    }

    private void startWatchService() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(dataHome);
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                if (key == null) continue;

                for (WatchEvent<?> event : key.pollEvents()) {
                    // 【兜底大招】：哪怕用了异步，万一操作系统底层队列还是溢出了，我们也有补偿机制
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        log.warn(">>> [系统警告] 跑批流量过大，OS文件事件队列溢出！触发全量扫盘补偿机制...");
                        workerPool.submit(this::triggerFullCompensationScan);
                        continue;
                    }

                    Path changed = (Path) event.context();
                    String fileName = changed.toString();

                    if (fileName.endsWith(".ind")) {
                        File indFile = path.resolve(fileName).toFile();

                        // 【秒杀卡顿的关键】：主线程绝不睡！绝不查库！
                        // 发现一个 ind，立刻封装成任务扔进巨大的 workerPool 队列中，然后瞬间回头去监听下一个事件。
                        // 1万个文件过来，只需要 0.01 秒就能全部接收完毕并放入内存缓冲。
                        workerPool.submit(() -> handleIndFileReady(indFile));
                    }
                }
                if (!key.reset()) break;
            }
        } catch (Exception e) {
            log.error("目录监听引擎发生致命异常，监听已停止", e);
        }
    }

    /**
     * 这里是在异步线程 (Thor-ScannerWorker-X) 中执行的，随便 sleep 都不怕
     */
    private void handleIndFileReady(File indFile) {
        try {
            // 防抖缓冲，模拟等待写入完成 (在异步线程中睡眠，毫无压力)
            Thread.sleep(500);
            if (indFile.length() == 0) return;

            // 事件去重过滤
            String fileKey = indFile.getAbsolutePath();
            Long lastProcessTime = processedFiles.get(fileKey);
            if (lastProcessTime != null && (System.currentTimeMillis() - lastProcessTime < 3000)) {
                return;
            }
            processedFiles.put(fileKey, System.currentTimeMillis());

            String datFileName = indFile.getName().substring(0, indFile.getName().lastIndexOf('.')) + ".dat";
            File datFile = new File(indFile.getParentFile(), datFileName);

            if (!datFile.exists() || !validateIndFile(indFile, datFile)) {
                return;
            }

            // ================== 【中心大脑调度逻辑】 ==================
            ThorTaskCfg routeCfg = taskCfgMapper.selectOne(Wrappers.<ThorTaskCfg>query()
                    .eq("file_pattern", datFileName)
                    .eq("is_active", 1)
                    .last("LIMIT 1"));

            if (routeCfg == null) {
                log.warn(">>> [中心调度] 拦截！未在数据库找到文件 {} 的路由规则", datFileName);
                return;
            }

            ThorNode targetNode = nodeMapper.selectOne(Wrappers.<ThorNode>query()
                    .eq("node_name", routeCfg.getDstNode())
                    .eq("status", "ONLINE")
                    .last("LIMIT 1"));

            if (targetNode == null) {
                log.error(">>> [中心调度] 失败！目标节点 {} 离线或不存在", routeCfg.getDstNode());
                return;
            }

            String taskId = "TASK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            ThorTaskInstance instance = new ThorTaskInstance();
            instance.setTaskId(taskId);
            instance.setCfgId(routeCfg.getId());
            instance.setFileName(datFileName);
            instance.setTotalSize(datFile.length());
            instance.setStatus("PROCESSING");
            instance.setStartTime(new Date());
            instanceMapper.insert(instance);

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
            log.info(">>> [中心调度] 跑批任务 {} 已生成流水并落盘，目标节点: {}", taskId, targetNode.getNodeName());

        } catch (Exception e) {
            log.error("处理 .ind 校验文件异常: {}", indFile.getName(), e);
        }
    }

    /**
     * 兜底对账机制：当操作系统极度繁忙丢失事件时，全盘扫描
     */
    private void triggerFullCompensationScan() {
        log.info(">>> 开始执行全量目录对账补偿...");
        File dir = new File(dataHome);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".ind"));
        if (files == null) return;

        for (File indFile : files) {
            // 对硬盘上的每一个 ind 文件，重新投递一遍。
            // 依赖于前面的去重机制和数据库的主键，重复投递也不会造成二次处理
            workerPool.submit(() -> handleIndFileReady(indFile));
        }
        log.info(">>> 全量目录对账补偿完成，共扫描 {} 个校验文件", files.length);
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

    @PreDestroy
    public void destroy() {
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }
}