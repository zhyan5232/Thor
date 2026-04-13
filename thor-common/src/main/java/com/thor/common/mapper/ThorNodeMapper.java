package com.thor.common.mapper; // 核心修改：包名变成 common.mapper

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.thor.common.entity.ThorNode; // 引入刚刚建好的新实体类
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ThorNodeMapper extends BaseMapper<ThorNode> {
    // 基础的 CRUD 已经由 BaseMapper 提供，如果有手写的 SQL 可以补充在这里
}