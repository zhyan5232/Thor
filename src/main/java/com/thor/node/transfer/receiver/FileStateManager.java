package com.thor.node.transfer.receiver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thor 传输进度管理器：管理断点续传的偏移量
 */
public class FileStateManager {

    // 任务 ID -> 已写入字节数
    private static final Map<String, AtomicLong> offsetMap = new ConcurrentHashMap<>();

    public static long getOffset(String taskId) {
        return offsetMap.computeIfAbsent(taskId, k -> new AtomicLong(0)).get();
    }

    public static void saveOffset(String taskId, long newOffset) {
        offsetMap.computeIfAbsent(taskId, k -> new AtomicLong(0)).set(newOffset);
    }

    public static void clearState(String taskId) {
        offsetMap.remove(taskId);
    }
}