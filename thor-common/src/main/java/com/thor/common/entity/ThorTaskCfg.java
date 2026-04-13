package com.thor.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.util.Date;

@Data
@TableName("thor_task_cfg")
public class ThorTaskCfg {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String taskGroup;
    private String srcNode;
    private String dstNode;
    private String filePattern;
    private String processType;
    private String fromCharset;
    private String toCharset;
    private Integer isActive;
    private Date createTime;
}