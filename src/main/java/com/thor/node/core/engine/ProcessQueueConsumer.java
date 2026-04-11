package com.thor.node.core.engine;

import com.thor.node.core.QueueManager;
import com.thor.node.core.model.TaskNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Thor 数据处理队列消费者引擎
 * 负责执行 CPU 密集型任务（如 iconv 字符集转换、数据清洗等）
 */
@Component
public class ProcessQueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProcessQueueConsumer.class);

    @Autowired
    private QueueManager queueManager;

    /**
     * 自动启动处理线程池
     */
    @PostConstruct
    public void startConsumer() {
        // 分配 4 个处理线程（对应阶段一、二的业务逻辑）
        for (int i = 0; i < 4; i++) {
            new Thread(this::runConsume, "Thor-ProcessWorker-" + i).start();
        }
        log.info("Thor 业务处理消费者线程池已激活，并行度: 4");
    }

    private void runConsume() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 1. 【核心修复点】调用 QueueManager 规范化后的新方法名
                TaskNode task = queueManager.takeFromProcessQueue();
                if (task == null) continue;

                log.info(">>> [业务加工] 任务 ID: {} 开始执行数据预处理...", task.getTaskId());

                // 2. 模拟处理耗时（如 iconv 转换等）
                Thread.sleep(100);

                // 3. 处理完成后，根据业务逻辑决定是否需要流转到传输队列
                // if (needTransfer) { queueManager.pushToTransferQueue(task); }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("业务处理 Worker 运行时异常: ", e);
            }
        }
    }
}