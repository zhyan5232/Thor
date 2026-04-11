package com.thor.node.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.util.Date;

@Data
@TableName("thor_node")
public class ThorNode {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String nodeName;
    private String nodeType;
    private String ipAddress;
    private Integer port;
    private String status;
    private Date lastHeartbeat;
    private Date createTime;
}