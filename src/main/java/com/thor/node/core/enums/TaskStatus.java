package com.thor.node.core.enums;

/**
 * Thor 任务生命周期状态
 * 对标原 Lua 系统 st.tse 及 succ_flag
 */
public enum TaskStatus {
    WAITING(0, "等待处理"),
    PROCESSING(1, "处理中 (过滤/转码/拆分)"),
    TRANSFERRING(2, "网络传输中"),
    SUCCESS(3, "处理/传输成功"),
    FAILED(4, "处理失败"),
    PAUSED(5, "任务暂停");

    private final int code;
    private final String description;

    TaskStatus(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() { return code; }
    public String getDescription() { return description; }
}