package com.thor.node.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.thor.node.core.QueueManager;
import com.thor.node.core.model.TaskNode;
import com.thor.node.transfer.sender.ZeroCopySender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Thor 传输队列消费者引擎
 * 负责从队列获取任务并驱动 ZeroCopySender 执行物理传输
 */
@Component
public class TransferQueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransferQueueConsumer.class);

    @Autowired
    private QueueManager queueManager;

    @Autowired
    private ZeroCopySender zeroCopySender;

    /**
     * 初始化消费者线程池
     */
    @PostConstruct
    public void startConsumer() {
        // 开启 8 个并行传输 Worker (可根据 CPU 核心数调整)
        for (int i = 0; i < 8; i++) {
            new Thread(this::runConsume, "Thor-TransferWorker-" + i).start();
        }
        log.info("Thor 传输消费者线程池已激活，并行度: 8");
    }

    private void runConsume() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 1. 从全局队列中获取任务 (阻塞式)
                TaskNode task = queueManager.takeFromTransferQueue();
                if (task == null) continue;

                log.info(">>> [引擎调度] 任务 ID: {} 进入物理发送环节", task.getTaskId());

                // 2. 解析任务配置 (不写死)
                JsonNode cfg = task.getProcCfg();
                String ip = cfg.path("dst_address").asText();
                int port = cfg.path("dst_port").asInt();
                String srcPath = cfg.path("src_file_name").asText();

                // 【新增】：提取原始的逻辑文件名 (比如 fex_test.dat)
                String originalFileName = srcPath;
                if (srcPath.endsWith(".utf8")) {
                    originalFileName = srcPath.substring(0, srcPath.length() - 5);
                }
                String logicalFileName = new java.io.File(originalFileName).getName();

                // 【修改】：给 sendFile 增加一个逻辑文件名的参数
                zeroCopySender.sendFile(ip, port, task.getTaskId(), srcPath, logicalFileName);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("传输 Worker 运行时异常: ", e);
            }
        }
    }
}