package com.thor.node.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thor.node.core.QueueManager;
import com.thor.node.core.model.TaskNode;
import com.thor.node.core.process.IconvProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Thor 数据处理队列消费者引擎
 * 负责执行 CPU 密集型任务（转码、拆分等），完成后将任务推入传输队列
 */
@Component
public class ProcessQueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProcessQueueConsumer.class);

    @Autowired
    private QueueManager queueManager;

    @Autowired
    private IconvProcessor iconvProcessor;

    @PostConstruct
    public void startConsumer() {
        for (int i = 0; i < 4; i++) {
            new Thread(this::runConsume, "Thor-ProcessWorker-" + i).start();
        }
        log.info("Thor 业务处理消费者线程池已激活，并行度: 4");
    }

    private void runConsume() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 1. 阻塞获取处理任务
                TaskNode task = queueManager.takeFromProcessQueue();
                if (task == null) continue;

                log.info(">>> [调度介入] 任务 ID: {} 进入本地数据加工车间", task.getTaskId());

                JsonNode cfg = task.getProcCfg();
                String srcFile = cfg.path("src_file_name").asText();
                // 如果没有配置 process_type，默认为 none (不处理，直接传)
                String processType = cfg.path("process_type").asText("none");

                boolean processSuccess = true;

                // 2. 执行【前转码】逻辑
                if ("iconv".equals(processType)) {
                    String fromCharset = cfg.path("from_charset").asText("GBK");
                    String toCharset = cfg.path("to_charset").asText("UTF-8");
                    String destFile = srcFile + ".utf8"; // 临时生成一个转码后的文件

                    processSuccess = iconvProcessor.convertEncoding(srcFile, destFile, fromCharset, toCharset);
                    
                    if (processSuccess) {
                        // 【核心流转】：转码成功后，将后续要传输的源文件路径，动态替换为刚刚转码生成的那个新文件
                        if (cfg instanceof ObjectNode) {
                            ((ObjectNode) cfg).put("src_file_name", destFile);
                        }
                    }
                }
                
                // (未来这里可以继续通过 else if 增加 "split" 拆分逻辑...)

                // 3. 加工完毕，判断流转去向
                if (processSuccess) {
                    log.info(">>> [车间放行] 任务 {} 加工合格，移交至极速传输队列", task.getTaskId());
                    queueManager.pushToTransferQueue(task); // 把任务扔给 TransferWorker
                } else {
                    log.error(">>> [加工质检] 任务 {} 数据处理失败，进入死信异常状态", task.getTaskId());
                    // TODO: 结合 FEX 文档要求，后续在这里加入 task.canRetry() 重试机制
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("业务处理 Worker 运行时异常: ", e);
            }
        }
    }
}