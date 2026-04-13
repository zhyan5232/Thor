package com.thor.node.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.thor.common.enums.TaskStatus;
import com.thor.node.core.model.TaskNode;
import com.thor.node.transfer.receiver.FileReceiver;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;

@Component
public class TaskDispatcher {
    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    @Autowired
    private QueueManager queueManager;

    @Autowired
    private FileReceiver fileReceiver;

    public void dispatch(String code, JsonNode rootNode, ChannelHandlerContext ctx) {
        switch (code) {
            case "thor.center.call_tsk":
                handleNewTask(rootNode);
                break;

            case "thor.node.transfer_start":
                // 接收端逻辑：收到点火指令，初始化接收上下文
                String tid = rootNode.path("task_id").asText();
                String fname = rootNode.path("file_name").asText("unknown.dat");
                long fsize = rootNode.path("file_size").asLong();
                fileReceiver.initTransfer(tid, fname, fsize);
                break;

            default:
                log.warn("调度中心暂不支持该指令路由: {}", code);
        }
    }

    /**
     * 处理来自中心节点的新任务分发
     */
    private void handleNewTask(JsonNode rootNode) {
        JsonNode list = rootNode.path("list");
        if (list.isArray()) {
            for (JsonNode node : list) {
                // 1. 封装任务对象
                TaskNode task = new TaskNode();
                task.setTaskId(node.path("id").asText());
                task.setProcCfg(node.path("proc_cfg"));
                int state = node.path("state").asInt();

                // 2. 根据状态位决定入队逻辑 (30以上代表进入传输阶段)
                if (state >= 30) {
                    task.setStatus(TaskStatus.WAITING);
                    queueManager.pushToTransferQueue(task); // 真正推入传输队列
                    log.info("任务 [{}] 已成功挂载至传输引擎队列", task.getTaskId());
                } else {
                    task.setStatus(TaskStatus.WAITING);
                    queueManager.pushToProcessQueue(task); // 推入处理队列
                    log.info("任务 [{}] 已进入本地处理队列", task.getTaskId());
                }
            }
        }
    }
}