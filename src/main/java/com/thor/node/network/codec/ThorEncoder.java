package com.thor.node.network.codec;

import com.thor.node.network.protocol.ThorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Thor 协议编码器 (出站)
 * 协议规范：魔数(4B) + 类型(4B) + 长度(4B) + 负载(N)
 */
public class ThorEncoder extends MessageToByteEncoder<ThorMessage> {

    // 正式使用 "THOR" 的十六进制 0x54484f52
    private static final int MAGIC_NUMBER = 0x54484f52;

    @Override
    protected void encode(ChannelHandlerContext ctx, ThorMessage msg, ByteBuf out) {
        // 1. 写入魔数 (直接写入 int，效率更高)
        out.writeInt(MAGIC_NUMBER);

        // 2. 写入类型 (修正：从 writeByte 改为 writeInt，保持 4 字节对齐)
        out.writeInt(msg.getType());

        // 3. 写入长度 (4 bytes)
        out.writeInt(msg.getLength());

        // 4. 写入负载数据 (增加判空保护)
        if (msg.getPayload() != null && msg.getPayload().length > 0) {
            out.writeBytes(msg.getPayload());
        }

        // 5. 性能日志 (调试阶段建议开启)
        // log.debug("Thor 帧编码完成：类型={}, 长度={}", msg.getType(), msg.getLength());
    }
}