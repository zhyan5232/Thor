package com.thor.node.transfer.sender;

import com.thor.common.entity.ThorTaskInstance;
import com.thor.common.mapper.ThorTaskInstanceMapper;
import com.thor.node.network.codec.ThorEncoder;
import com.thor.node.network.protocol.ThorMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

@Component
public class ZeroCopySender {
    private static final Logger log = LoggerFactory.getLogger(ZeroCopySender.class);
    private final EventLoopGroup group = new NioEventLoopGroup();

    // 【新增注入】：注入流水表 Mapper
    @Autowired
    private ThorTaskInstanceMapper instanceMapper;

    public void sendFile(String ip, int port, String taskId, String filePath, String logicalFileName) {
        File file = new File(filePath);
        if (!file.exists()) {
            log.error("待发送文件不存在: {}", filePath);
            return;
        }

        Bootstrap b = new Bootstrap();
        b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) {
                // 1. 核心修复：发送端也必须挂载编码器，否则 ThorMessage 对象发不出去
                ch.pipeline().addLast(new ThorEncoder());

                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        // 【修改】：在发送 JSON 启动指令时，强制使用 logicalFileName，而不是物理的 file.getName()
                        String json = String.format(
                                "{\"code\":\"thor.node.transfer_start\",\"task_id\":\"%s\",\"file_name\":\"%s\",\"file_size\":%d}",
                                taskId, logicalFileName, file.length()
                        );
                        log.info(">>> [1/2] 发送启动指令: {}", taskId);
                        ctx.write(new ThorMessage(ThorMessage.TYPE_JSON_CMD, json.getBytes(StandardCharsets.UTF_8)));

                        // 3. 发送流式协议头 (12字节：魔数+类型+长度)
                        // 这里我们直接利用编码器发送一个空 Payload 的 ThorMessage，更标准
                        ThorMessage streamHeader = new ThorMessage(ThorMessage.TYPE_FILE_STREAM, new byte[0]);
                        streamHeader.setLength((int) file.length());
                        log.info(">>> [2/2] 发送流式协议头，大小: {}", file.length());
                        ctx.write(streamHeader);

                        // 4. 零拷贝发射文件体
                        RandomAccessFile raf = new RandomAccessFile(file, "r");
                        FileRegion region = new DefaultFileRegion(raf.getChannel(), 0, file.length());

                        ctx.writeAndFlush(region).addListener((ChannelFutureListener) future -> {
                            ThorTaskInstance updateInstance = new ThorTaskInstance();
                            updateInstance.setTaskId(taskId);
                            updateInstance.setEndTime(new java.util.Date());

                            if (future.isSuccess()) {
                                log.info(">>> 任务 [{}] 物理传输成功落地！", taskId);
                                updateInstance.setStatus("SUCCESS");
                            } else {
                                log.error(">>> 任务 [{}] 物理传输失败: ", taskId, future.cause());
                                updateInstance.setStatus("FAILED");
                                updateInstance.setErrorMsg(future.cause().getMessage());
                            }
                            instanceMapper.updateById(updateInstance);

                            raf.close();
                            ctx.close();

                            // ==========================================
                            // 【核心修复：空间释放机制】阅后即焚临时产生的 .utf8 文件
                            if (file.getName().endsWith(".utf8")) {
                                boolean deleted = file.delete();
                                if (deleted) {
                                    log.info(">>> [空间释放] 临时转码文件已自动清除: {}", file.getName());
                                }
                            }
                            // ==========================================
                        });
                    }
                });
            }
        });

        b.connect(ip, port).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) log.error("无法连接到目标节点 {}:{}", ip, port);
        });
    }
}