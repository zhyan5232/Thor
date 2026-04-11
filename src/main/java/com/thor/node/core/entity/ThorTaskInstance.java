package com.thor.node.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.util.Date;

@Data
@TableName("thor_task_instance")
public class ThorTaskInstance {
    // 任务流水号通常是 UUID 或雪花算法生成，非自增
    @TableId(type = IdType.INPUT)
    private String taskId;
    private Long cfgId;
    private String fileName;
    private Long totalSize;
    private Long currentOffset;
    private String status;
    private String errorMsg;
    private Date startTime;
    private Date endTime;
}