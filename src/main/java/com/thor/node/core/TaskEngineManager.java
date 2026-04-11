package com.thor.node.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thor 任务引擎管理器
 * 负责定义全局线程池资源，协调各组件运行
 */
@Component
public class TaskEngineManager {
    private static final Logger log = LoggerFactory.getLogger(TaskEngineManager.class);

    // 只需要定义线程池资源即可，具体的消费逻辑已经搬移到各自的 Consumer 类中
    private static ExecutorService processThreadPool;
    private static ExecutorService transferThreadPool;

    @PostConstruct
    public void init() {
        log.info(">>> [引擎准备] 正在初始化 Thor 核心线程池...");

        // 处理线程池：负责 iconv 转换、数据清洗等 CPU 密集型任务
        processThreadPool = Executors.newFixedThreadPool(4);

        // 传输线程池：负责维护 NIO 连接和零拷贝调度
        // 注意：TransferQueueConsumer 现在由 Spring 自动启动，这里只需分配资源
        transferThreadPool = Executors.newFixedThreadPool(8);

        log.info(">>> [引擎准备] 线程池分配完毕 (Process:4, Transfer:8)");
        log.info("Thor 任务引擎全线启动完毕，等待调度指令接入...");
    }

    // 提供静态方法供其他组件获取线程池（如果需要）
    public static ExecutorService getProcessThreadPool() {
        return processThreadPool;
    }

    public static ExecutorService getTransferThreadPool() {
        return transferThreadPool;
    }
}