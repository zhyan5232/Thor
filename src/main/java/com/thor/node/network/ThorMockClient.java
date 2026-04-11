package com.thor.node.network;

import com.thor.node.network.protocol.ThorMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import com.thor.node.network.codec.ThorEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Thor 验证引信：模拟中心节点下发任务
 */
public class ThorMockClient {
    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ThorEncoder());
                }
            });

            // 1. 连接本地 Thor 节点
            ChannelFuture f = b.connect("127.0.0.1", 5599).sync();

            // 2. 构造指令 (模拟中心下发传输任务)
            // 注意：state 设为 30 以上，直接进入传输队列
            // 修改后的验证指令
            String jsonCmd = "{" +
                    "\"code\":\"thor.center.call_tsk\"," +
                    "\"list\":[{" +
                    "\"id\":\"TASK-WIN-001\"," +
                    "\"state\":31," +
                    "\"proc_cfg\":{" +
                    // 指向你刚刚在 D 盘准备的文件
                    "\"src_file_name\":\"E:/thor_workspace/data/1.mkv\"," +
                    "\"dst_address\":\"127.0.0.1\"," +
                    "\"dst_port\":5599" +
                    "}" +
                    "}]" +
                    "}";

            ThorMessage msg = new ThorMessage(ThorMessage.TYPE_JSON_CMD, jsonCmd.getBytes(StandardCharsets.UTF_8));
            
            // 3. 发射！
            f.channel().writeAndFlush(msg);
            System.out.println(">>> 验证指令已发送，请观察 ThorNodeApplication 控制台日志");
            
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}