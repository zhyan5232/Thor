package com.thor.node.core.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thor 队列消费者抽象基类
 * 规范线程的启动、安全中断与异常捕获
 */
public abstract class AbstractQueueConsumer implements Runnable {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected volatile boolean running = true;

    @Override
    public void run() {
        log.info("{} 消费者线程已启动", getConsumerName());
        
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                consume();
            } catch (InterruptedException e) {
                log.warn("{} 收到中断信号，准备安全退出", getConsumerName());
                Thread.currentThread().interrupt(); // 恢复中断状态标志
                break;
            } catch (Exception e) {
                log.error("{} 运行期发生未捕获异常", getConsumerName(), e);
            }
        }
    }

    public void stop() {
        this.running = false;
    }

    protected abstract void consume() throws InterruptedException;
    protected abstract String getConsumerName();
}