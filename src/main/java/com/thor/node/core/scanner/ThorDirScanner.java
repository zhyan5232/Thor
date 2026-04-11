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
import java.io.File;
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
        
        // 启动独立守护线程，避免阻塞 Spring 主线程
        Thread scannerThread = new Thread(this::startWatchService, "Thor-ScannerWorker");
        scannerThread.setDaemon(true);
        scannerThread.start();
        
        log.info(">>> [引信激活] Thor 自动发现引擎已挂载，全天候监听目录: {}", dataHome);
    }

    private void startWatchService() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(dataHome);
            // 仅监听文件的创建和修改事件
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

            while (!Thread.currentThread().isInterrupted()) {
                // 使用带超时的 poll，允许线程响应中断
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                if (key == null) continue;

                for (WatchEvent<?> event : key.pollEvents()) {
                    // 忽略目录创建等情况
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;

                    Path changed = (Path) event.context();
                    String fileName = changed.toString();

                    // 核心拦截规则：只对 .ind 就绪控制文件做出反应
                    if (fileName.endsWith(".ind")) {
                        log.info(">>> [触发] 检测到就绪控制文件落地: {}", fileName);
                        handleIndFileReady(path.resolve(fileName).toFile());
                    }
                }
                
                // 重置 Key，继续监听下一次事件
                if (!key.reset()) break; 
            }
        } catch (Exception e) {
            log.error("目录监听引擎发生致命异常，监听已停止", e);
        }
    }

    private void handleIndFileReady(File indFile) {
        try {
            // 1. 推导对应的数据文件 (如 a.ind -> a.dat)
            String datFileName = indFile.getName().substring(0, indFile.getName().lastIndexOf('.')) + ".dat";
            File datFile = new File(indFile.getParentFile(), datFileName);

            // 2. 物理校验
            if (!datFile.exists()) {
                log.warn("警告：校验文件存在，但数据文件实体缺失，拒绝发车: {}", datFileName);
                return;
            }

            // 3. 组装发往自身的临时测试指令 (后续版本将由 Center 调度组装)
            String taskId = "TASK-AUTO-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            TaskNode task = new TaskNode();
            task.setTaskId(taskId);
            
            ObjectNode cfg = mapper.createObjectNode();
            cfg.put("src_file_name", datFile.getAbsolutePath());
            // 默认测试靶点：发给 Node 自己
            cfg.put("dst_address", "127.0.0.1");
            cfg.put("dst_port", 5599); 
            task.setProcCfg(cfg);

            // 4. 直接推入传输队列，唤醒 ZeroCopySender
            queueManager.pushToTransferQueue(task);
            log.info(">>> [调度成功] 任务 {} 已生成，数据文件 {} 已推入极速传输引擎", taskId, datFileName);

        } catch (Exception e) {
            log.error("处理 .ind 校验文件异常: {}", indFile.getName(), e);
        }
    }
}