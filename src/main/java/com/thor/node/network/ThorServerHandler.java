package com.thor.node.network.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thor.node.core.TaskDispatcher;
import com.thor.node.transfer.receiver.FileReceiver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.netty.channel.ChannelHandler;

@Component
@ChannelHandler.Sharable
public class ThorServerHandler extends SimpleChannelInboundHandler<ThorMessage> {
    private static final Logger log = LoggerFactory.getLogger(ThorServerHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // 关键：连接属性键
    private static final AttributeKey<String> ATTR_TASK_ID = AttributeKey.valueOf("taskId");

    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private FileReceiver fileReceiver;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ThorMessage msg) throws Exception {
        if (msg.getType() == ThorMessage.TYPE_JSON_CMD) {
            JsonNode root = mapper.readTree(msg.getPayload());
            String code = root.path("code").asText();

            // 如果是启动指令，提取 TaskID 并绑定到 Channel
            if ("thor.node.transfer_start".equals(code) || "thor.center.call_tsk".equals(code)) {
                String tid = root.has("list") ? root.path("list").get(0).path("id").asText() : root.path("task_id").asText();
                ctx.channel().attr(ATTR_TASK_ID).set(tid);
            }

            taskDispatcher.dispatch(code, root, ctx);
        }
        else if (msg.getType() == ThorMessage.TYPE_FILE_STREAM) {
            // 从 Channel 属性中取出绑定的 ID，实现全动态匹配
            String taskId = ctx.channel().attr(ATTR_TASK_ID).get();
            if (taskId != null) {
                fileReceiver.processIncomingBlock(ctx.channel(), taskId, msg.getPayload());
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("节点接入成功: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("节点连接断开: {}", ctx.channel().remoteAddress());
    }
}