package com.thor.node.transfer.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thor 文件句柄中心：负责物理文件的创建与释放
 */
public class FileChannelCache {
    private static final Logger log = LoggerFactory.getLogger(FileChannelCache.class);
    private static final Map<String, FileChannel> cache = new ConcurrentHashMap<>();
    private static final Map<String, RandomAccessFile> rafCache = new ConcurrentHashMap<>();

    public static FileChannel getWriteChannel(String taskId, String fullPath) throws Exception {
        if (cache.containsKey(taskId)) {
            return cache.get(taskId);
        }

        File file = new File(fullPath);
        // 1. 关键修复：如果父目录（received 文件夹）不存在，立即创建
        if (!file.getParentFile().exists()) {
            boolean created = file.getParentFile().mkdirs();
            log.info("检测到接收目录不存在，创建结果: {}, 路径: {}", created, file.getParentFile().getAbsolutePath());
        }

        // 2. 关键修复：使用 rw 模式打开，Windows 下会自动创建空文件
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        rafCache.put(taskId, raf);
        cache.put(taskId, channel);

        log.info(">>> [物理建档] 任务 {} 已建立文件句柄，绝对路径: {}", taskId, file.getAbsolutePath());
        return channel;
    }

    public static void closeAndRemove(String taskId) {
        try {
            FileChannel channel = cache.remove(taskId);
            RandomAccessFile raf = rafCache.remove(taskId);
            if (channel != null) {
                channel.force(true); // 强制将元数据刷新到磁盘，解决 0KB 问题
                channel.close();
            }
            if (raf != null) raf.close();
            log.info(">>> [句柄释放] 任务 {} 对应的文件已安全关闭并保存", taskId);
        } catch (Exception e) {
            log.error("关闭文件句柄异常", e);
        }
    }
}