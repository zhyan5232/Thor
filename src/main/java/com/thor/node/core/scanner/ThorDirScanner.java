package com.thor.node.core.scanner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thor.node.core.QueueManager;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Thor 自动目录发现引擎
 * 职责：利用 NIO 监听数据目录，严格通过 .ind 校验文件来触发传输调度
 */
@Component
public class ThorDirScanner {
    private static final Logger log = LoggerFactory.getLogger(ThorDirScanner.class);

    @Value("${thor.node.data-home}")
    private String dataHome;

    @Autowired
    private QueueManager queueManager;

    private final ObjectMapper mapper = new ObjectMapper();

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

                    // 核心拦截规则：只对 .ind 就绪控制文件做出反应
                    if (fileName.endsWith(".ind")) {
                        log.info(">>> [触发] 检测到就绪控制文件落地: {}", fileName);
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
            // ==========================================
            // 【核心修复：工业级防抖机制】
            // 给予操作系统 500 毫秒的缓冲时间，等待用户把内容保存并落盘
            Thread.sleep(500);

            // 再次检查文件大小。如果还是 0 字节，说明用户只是刚刚“新建”了文件还没保存。
            // 我们直接 return 放弃这次处理，等待用户真正保存时触发下一次的 Modify 事件。
            if (indFile.length() == 0) {
                log.debug(">>> [防抖拦截] 忽略空文件创建事件，等待用户写入内容: {}", indFile.getName());
                return;
            }
            // ==========================================

            // 1. 推导对应的数据文件
            String datFileName = indFile.getName().substring(0, indFile.getName().lastIndexOf('.')) + ".dat";
            File datFile = new File(indFile.getParentFile(), datFileName);

            if (!datFile.exists()) {
                log.warn(">>> [拦截] 校验文件存在，但数据文件实体缺失: {}", datFileName);
                return;
            }

            // 2. 严格解析并校验 .ind 文件内容 (FEX 7.3 规范)
            if (!validateIndFile(indFile, datFile)) {
                log.error(">>> [拦截] .ind 校验文件内容不合法或与实体文件不匹配，拒绝调度: {}", indFile.getName());
                return;
            }

            // 3. 组装发往自身的临时测试指令
            String taskId = "TASK-AUTO-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            TaskNode task = new TaskNode();
            task.setTaskId(taskId);

            ObjectNode cfg = mapper.createObjectNode();
            cfg.put("src_file_name", datFile.getAbsolutePath());
            cfg.put("dst_address", "127.0.0.1");
            cfg.put("dst_port", 5599);

            // 配置加工流水线参数
            cfg.put("process_type", "iconv");
            cfg.put("from_charset", getEncodingFromInd(indFile)); // 动态读取源编码
            cfg.put("to_charset", "UTF-8");

            task.setProcCfg(cfg);

            // 4. 先推入处理队列 (ProcessQueue)，等待加工完成后再传输
            queueManager.pushToProcessQueue(task);
            log.info(">>> [调度成功] 数据文件 {} 已通过校验，推入【数据加工车间】准备转码", datFileName);

        } catch (Exception e) {
            log.error("处理 .ind 校验文件异常: {}", indFile.getName(), e);
        }
    }

    /**
     * 校验 .ind 文件内容合法性
     */
    private boolean validateIndFile(File indFile, File datFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(indFile))) {
            String encoding = br.readLine();
            String metaLine = br.readLine();

            if (encoding == null || metaLine == null) {
                log.warn("IND文件格式错误，行数不足");
                return false;
            }

            String[] metas = metaLine.trim().split("\\s+");
            if (metas.length < 2) {
                log.warn("IND文件第二行元数据格式错误: {}", metaLine);
                return false;
            }

            String expectedFileName = metas[0];
            if (!expectedFileName.equals(datFile.getName())) {
                log.warn("IND声明的文件名 [{}] 与实际文件名 [{}] 不符", expectedFileName, datFile.getName());
                return false;
            }

            log.info(">>> IND 校验通过 | 编码: {} | 声明目标: {}", encoding.trim(), expectedFileName);
            return true;
        } catch (Exception e) {
            log.error("读取 IND 文件失败", e);
            return false;
        }
    }

    /**
     * 获取编码格式
     */
    private String getEncodingFromInd(File indFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(indFile))) {
            return br.readLine().trim();
        } catch (Exception e) {
            return "UTF-8"; // 默认兜底
        }
    }
}