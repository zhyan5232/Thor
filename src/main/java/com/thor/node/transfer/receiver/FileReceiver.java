package com.thor.node.transfer.receiver;

import com.thor.node.transfer.cache.FileChannelCache;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FileReceiver {
    private static final Logger log = LoggerFactory.getLogger(FileReceiver.class);

    @Value("${thor.node.received-home}")
    private String receivedHome;

    private final Map<String, TransferContext> contextMap = new ConcurrentHashMap<>();

    public void initTransfer(String taskId, String fileName, long totalSize) {
        TransferContext ctx = new TransferContext();
        ctx.taskId = taskId;
        ctx.fileName = fileName;
        ctx.totalSize = totalSize;
        ctx.savePath = receivedHome + "/" + fileName;
        contextMap.put(taskId, ctx);

        log.info("======= [物理建档开始] =======");
        log.info("任务ID: {}", taskId);
        log.info("存入路径: {}", ctx.savePath);
        log.info("预期大小: {}", totalSize);
        log.info("============================");

        // 【新增】：完美兼容 FEX 7.5 规范的 0字节空文件场景
        if (totalSize == 0) {
            try {
                // 强制触发物理建档
                FileChannelCache.getWriteChannel(taskId, ctx.savePath);
                // 瞬间关闭并落盘
                FileChannelCache.closeAndRemove(taskId);
                FileStateManager.clearState(taskId);
                contextMap.remove(taskId);
                log.info(">>> [100%] 传输落地成功 (0字节空文件秒传): {}", ctx.savePath);
                // 【新增调用】
                generateIndFile(ctx.savePath, ctx.fileName, totalSize);
            } catch (Exception e) {
                log.error("处理0字节文件异常", e);
            }
        }
    }

    /**
     * 【新增】：严格按照 FEX 规范生成落地的 .ind 校验文件
     */
    private void generateIndFile(String datFilePath, String fileName, long totalSize) {
        try {
            String indFilePath = datFilePath.substring(0, datFilePath.lastIndexOf('.')) + ".ind";

            // 【新增】：精准统计真实行数 (替代硬编码的 1)
            long recordCount = 0;
            if (totalSize > 0) {
                // 使用 Java NIO 的懒加载 Stream 高效读取文件行数，防 OOM
                try (java.util.stream.Stream<String> lines = java.nio.file.Files.lines(java.nio.file.Paths.get(datFilePath), java.nio.charset.StandardCharsets.UTF_8)) {
                    recordCount = lines.count();
                } catch (Exception e) {
                    log.warn("行数统计异常，默认置为0", e);
                }
            }

            // 【核心修复】：放弃 PrintWriter，使用底层字节流强控格式
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(indFilePath)) {
                String line1 = "UTF-8\n";
                // 注意：末尾坚决不加 \n，彻底消灭幽灵第三行
                String line2 = fileName + "\t" + totalSize + "\t" + recordCount;

                fos.write(line1.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                fos.write(line2.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            log.info(">>> [系统协同] 目标端 .ind 校验文件已自动生成: {} (真实行数: {})", indFilePath, recordCount);
        } catch (Exception e) {
            log.error("生成 .ind 校验文件失败", e);
        }
    }
    public void processIncomingBlock(Channel channel, String taskId, byte[] payload) {
        if (taskId == null) {
            log.error("收到匿名流数据，无 TaskID 绑定，丢弃数据");
            return;
        }

        TransferContext ctx = contextMap.get(taskId);
        if (ctx == null) {
            log.error("上下文丢失，任务可能未初始化: {}", taskId);
            return;
        }

        try {
            FileChannel fileChannel = FileChannelCache.getWriteChannel(taskId, ctx.savePath);
            long offset = FileStateManager.getOffset(taskId);

            fileChannel.write(ByteBuffer.wrap(payload), offset);

            long newOffset = offset + payload.length;
            FileStateManager.saveOffset(taskId, newOffset);

            if (newOffset >= ctx.totalSize) {
                log.info(">>> [100%] 传输落地成功: {}", ctx.savePath);
                FileChannelCache.closeAndRemove(taskId);
                FileStateManager.clearState(taskId);
                contextMap.remove(taskId);
                // 【新增调用】
                generateIndFile(ctx.savePath, ctx.fileName, ctx.totalSize);
            }
        } catch (Exception e) {
            log.error("写入磁盘失败: {}", taskId, e);
        }
    }

    private static class TransferContext {
        String taskId, fileName, savePath;
        long totalSize;
    }
}