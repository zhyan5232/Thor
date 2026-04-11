package com.thor.node.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.thor.node.core.entity.ThorNode;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ThorNodeMapper extends BaseMapper<ThorNode> {
}