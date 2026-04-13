package com.thor.node.core;

import com.thor.node.core.model.TaskNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thor 全局队列管理器 (核心仓库)
 * 负责协调指令下发与任务执行之间的无锁缓冲
 */
@Component
public class QueueManager {
    private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

    // 1. 处理队列：存放需要进行 iconv 或数据清洗的任务
    private final BlockingQueue<TaskNode> processQueue = new LinkedBlockingQueue<>();

    // 2. 传输队列：存放准备进行极速物理传输的任务
    private final BlockingQueue<TaskNode> transferQueue = new LinkedBlockingQueue<>();

    /**
     * 将任务推入传输队列
     */
    public void pushToTransferQueue(TaskNode task) {
        if (task != null) {
            transferQueue.offer(task);
            log.debug("任务 [{}] 已进入传输就绪状态，当前积压: {}", task.getTaskId(), transferQueue.size());
        }
    }

    /**
     * 从传输队列获取任务 (阻塞式：如果没有任务，线程会在此安全挂起等待)
     */
    public TaskNode takeFromTransferQueue() throws InterruptedException {
        return transferQueue.take();
    }

    /**
     * 将任务推入处理队列
     */
    public void pushToProcessQueue(TaskNode task) {
        if (task != null) {
            processQueue.offer(task);
            log.debug("任务 [{}] 已进入本地处理队列，当前积压: {}", task.getTaskId(), processQueue.size());
        }
    }

    /**
     * 从处理队列获取任务
     */
    public TaskNode takeFromProcessQueue() throws InterruptedException {
        return processQueue.take();
    }

    /**
     * 获取当前系统总负载情况
     */
    public String getQueueStatus() {
        return String.format("QueueStatus -> Process:%d, Transfer:%d",
                processQueue.size(), transferQueue.size());
    }
}