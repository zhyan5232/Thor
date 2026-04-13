package com.thor.node.core.model;


import com.fasterxml.jackson.databind.JsonNode;
import com.thor.common.enums.TaskStatus;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thor 任务实体模型
 * 承载任务元数据、处理配置及当前状态
 */
public class TaskNode {

    private String taskId;
    private TaskStatus status;

    // 核心：这里必须定义为 JsonNode，以便在调度时动态读取配置
    private JsonNode procCfg;

    // 重试计数器（原子操作，确保多线程安全）
    private final AtomicInteger tryTimes = new AtomicInteger(0);

    /**
     * 无参构造函数 (解决 new TaskNode() 报错)
     */
    public TaskNode() {
        this.status = TaskStatus.WAITING;
    }

    // --- Getter and Setter ---

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public JsonNode getProcCfg() {
        return procCfg;
    }

    /**
     * 接收 Jackson 的 JsonNode 对象
     */
    public void setProcCfg(JsonNode procCfg) {
        this.procCfg = procCfg;
    }

    public int getTryTimes() {
        return tryTimes.get();
    }

    public int incrementAndGetTryTimes() {
        return tryTimes.incrementAndGet();
    }

    /**
     * 判断是否可以继续重试 (对标原 fex.retry_limit)
     */
    public boolean canRetry() {
        return tryTimes.get() < 3;
    }
}