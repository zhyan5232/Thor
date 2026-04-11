package com.thor.node.network.codec;

import com.thor.node.network.protocol.ThorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Thor (雷神) 高性能状态机解码器
 * 支持 O(1) 内存占用处理 10GB+ 超大文件流
 */
public class ThorDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(ThorDecoder.class);
    // 将 0x12345678 修改为 0x54484f52 (即 "THOR" 的十六进制)
    private static final int MAGIC_NUMBER = 0x54484f52;

    // 解码器状态枚举
    private enum State {
        READ_HEADER,   // 读取协议头 (12字节)
        READ_PAYLOAD,  // 读取 JSON 指令内容
        READ_STREAM    // 透传二进制文件流
    }

    private State currentState = State.READ_HEADER;
    private int currentType;       // 记录当前帧类型
    private int currentLength;     // 记录当前帧总长度
    private int streamReadProgress = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        // 使用循环处理，防止在一个 TCP 包中包含多个逻辑帧
        while (true) {
            switch (currentState) {
                case READ_HEADER:
                    if (in.readableBytes() < 12) return; // 头长度不足，等待更多数据

                    in.markReaderIndex();
                    int magic = in.readInt();
                    if (magic != MAGIC_NUMBER) {
                        log.error("Thor 协议错误：非法魔数 0x{}, 连接强制关闭", Integer.toHexString(magic));
                        in.clear();
                        ctx.close();
                        return;
                    }

                    currentType = in.readInt();
                    currentLength = in.readInt();

                    // 根据类型切换状态机
                    if (currentType == ThorMessage.TYPE_FILE_STREAM) {
                        currentState = State.READ_STREAM;
                        streamReadProgress = 0;
                        log.debug("检测到文件流接入，预计长度: {} bytes，进入 STREAM 模式", currentLength);
                    } else {
                        currentState = State.READ_PAYLOAD;
                    }
                    break;

                case READ_PAYLOAD:
                    // 处理 JSON 指令 (需完整缓存后再处理)
                    if (in.readableBytes() < currentLength) return;

                    byte[] payload = new byte[currentLength];
                    in.readBytes(payload);
                    out.add(new ThorMessage(currentType, payload));

                    // 回到寻找下一个 Header 的状态
                    currentState = State.READ_HEADER;
                    break;

                case READ_STREAM:
                    // --- 核心优化：超大文件流式处理 ---
                    // 不再等待全部读完，而是有多少读多少，直接发给后面的 FileReceiver
                    int available = Math.min(in.readableBytes(), currentLength - streamReadProgress);

                    if (available > 0) {
                        // 提取当前缓冲区的数据块
                        byte[] chunk = new byte[available];
                        in.readBytes(chunk);

                        // 封装为块消息发送给业务 Handler
                        ThorMessage streamChunk = new ThorMessage(currentType, chunk);
                        out.add(streamChunk);

                        streamReadProgress += available;
                    }

                    // 检查文件流是否接收完毕
                    if (streamReadProgress >= currentLength) {
                        log.debug("当前文件流数据包接收完毕，切换回 HEADER 模式");
                        currentState = State.READ_HEADER;
                    } else {
                        // 数据还没传完，退出循环等待下一个 TCP 包
                        return;
                    }
                    break;
            }

            // 如果缓冲区读完了，退出循环
            if (!in.isReadable()) break;
        }
    }
}