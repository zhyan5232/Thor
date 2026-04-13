一、建库语句
-- 1. 创建雷神专属数据库 (支持存储完整的 UTF-8 字符，包含 Emoji 等特殊符号)
CREATE DATABASE IF NOT EXISTS `thor_db` 
DEFAULT CHARACTER SET utf8mb4 
COLLATE utf8mb4_general_ci;

-- 2. 切换到该数据库
USE `thor_db`;


二、建表语句
1.节点注册表 (thor_node)
用于中心节点(Center)感知全网各个分发节点(Node)的存在、IP及健康状态。
CREATE TABLE `thor_node` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `node_name` varchar(64) NOT NULL COMMENT '节点名称 (如 node1, center)',
  `node_type` varchar(16) NOT NULL COMMENT '节点类型: CENTER(中心), NODE(边缘节点)',
  `ip_address` varchar(64) NOT NULL COMMENT '节点IP地址',
  `port` int(11) NOT NULL COMMENT '通信端口 (默认5599)',
  `status` varchar(16) NOT NULL DEFAULT 'ONLINE' COMMENT '状态: ONLINE(在线), OFFLINE(离线)',
  `last_heartbeat` datetime DEFAULT NULL COMMENT '最后一次心跳时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_node_name` (`node_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Thor 分布式节点注册表';

2.任务路由配置表 (thor_task_cfg)
替代老 FEX 复杂的 XML/JSON 配置。定义“谁把什么文件发给谁，中间要不要转码”。
CREATE TABLE `thor_task_cfg` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '配置主键',
  `task_group` varchar(64) NOT NULL COMMENT '任务组标识 (如 CBS_BILL)',
  `src_node` varchar(64) NOT NULL COMMENT '源节点名称',
  `dst_node` varchar(64) NOT NULL COMMENT '目标节点名称',
  `file_pattern` varchar(255) NOT NULL COMMENT '文件匹配正则或后缀 (如 *.dat)',
  `process_type` varchar(32) DEFAULT 'NONE' COMMENT '加工类型: NONE(不加工), ICONV(转码), SPLIT(拆分)',
  `from_charset` varchar(16) DEFAULT 'UTF-8' COMMENT '源编码',
  `to_charset` varchar(16) DEFAULT 'UTF-8' COMMENT '目标编码',
  `is_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用: 1是, 0否',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Thor 任务路由规则配置表';

3.任务实例流水表 (thor_task_instance)
核心表！记录每一次文件传输的完整生命周期，替代目前的 FileStateManager 内存管理，提供断点续传的数据库支持。
CREATE TABLE `thor_task_instance` (
  `task_id` varchar(64) NOT NULL COMMENT '全局唯一任务ID (如 TASK-AUTO-XXX)',
  `cfg_id` bigint(20) DEFAULT NULL COMMENT '关联的配置ID',
  `file_name` varchar(255) NOT NULL COMMENT '传输文件名',
  `total_size` bigint(20) NOT NULL DEFAULT '0' COMMENT '文件总大小(字节)',
  `current_offset` bigint(20) NOT NULL DEFAULT '0' COMMENT '当前已传输偏移量(用于断点续传)',
  `status` varchar(32) NOT NULL COMMENT '状态: INIT(初始化), PROCESSING(加工中), TRANSFERRING(传输中), SUCCESS(成功), FAILED(失败)',
  `error_msg` varchar(1000) DEFAULT NULL COMMENT '失败异常信息',
  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '任务开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '任务结束时间',
  PRIMARY KEY (`task_id`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Thor 任务传输实例流水表';