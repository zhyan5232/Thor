package com.thor.node.core;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.thor.common.entity.ThorNode;
import com.thor.common.mapper.ThorNodeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thor 任务引擎管理器
 * 负责定义全局线程池资源，协调各组件运行
 */
@Component
public class TaskEngineManager {
    private static final Logger log = LoggerFactory.getLogger(TaskEngineManager.class);

    // 只需要定义线程池资源即可，具体的消费逻辑已经搬移到各自的 Consumer 类中
    private static ExecutorService processThreadPool;
    private static ExecutorService transferThreadPool;

    @Autowired
    private ThorNodeMapper thorNodeMapper;
    @PostConstruct
    public void init() {
        log.info(">>> [引擎准备] 正在初始化 Thor 核心线程池...");

        // 处理线程池：负责 iconv 转换、数据清洗等 CPU 密集型任务
        processThreadPool = Executors.newFixedThreadPool(4);

        // 传输线程池：负责维护 NIO 连接和零拷贝调度
        // 注意：TransferQueueConsumer 现在由 Spring 自动启动，这里只需分配资源
        transferThreadPool = Executors.newFixedThreadPool(8);

        log.info(">>> [引擎准备] 线程池分配完毕 (Process:4, Transfer:8)");
        log.info("Thor 任务引擎全线启动完毕，等待调度指令接入...");

        // 【新增】：启动自我注册机制
        registerMyself();
    }

    private void registerMyself() {
        try {
            String myNodeName = "NODE-WIN-01"; // 本机节点名

            // 1. 先去数据库查一下，我是不是已经注册过了？
            ThorNode existingNode = thorNodeMapper.selectOne(new QueryWrapper<ThorNode>()
                    .eq("node_name", myNodeName));

            if (existingNode == null) {
                // 2. 如果数据库里没有我，执行首次注册 (Insert)
                ThorNode node = new ThorNode();
                node.setNodeName(myNodeName);
                node.setNodeType("NODE");
                node.setIpAddress("127.0.0.1");
                node.setPort(5599);
                node.setStatus("ONLINE");
                node.setCreateTime(new java.util.Date());
                node.setLastHeartbeat(new java.util.Date());

                thorNodeMapper.insert(node);
                log.info(">>> [中心注册] 节点首次上线成功，已写入数据库大盘！");
            } else {
                // 3. 如果我已经存在，执行“心跳续约” (Update)
                // 更新我的状态为 ONLINE，并刷新最后心跳时间 (防止被中心端判定为离线)
                existingNode.setStatus("ONLINE");
                existingNode.setIpAddress("127.0.0.1"); // 万一重启后 IP 变了，这里顺便刷新
                existingNode.setPort(5599);
                existingNode.setLastHeartbeat(new java.util.Date());

                thorNodeMapper.updateById(existingNode);
                log.info(">>> [中心注册] 节点重连成功，已刷新数据库在线状态与心跳！");
            }
        } catch (Exception e) {
            log.warn(">>> [中心注册] 节点交互失败: {}", e.getMessage());
        }
    }

    // 提供静态方法供其他组件获取线程池（如果需要）
    public static ExecutorService getProcessThreadPool() {
        return processThreadPool;
    }

    public static ExecutorService getTransferThreadPool() {
        return transferThreadPool;
    }
}