package com.thor.node.transfer.receiver;

import com.thor.node.transfer.cache.FileChannelCache;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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