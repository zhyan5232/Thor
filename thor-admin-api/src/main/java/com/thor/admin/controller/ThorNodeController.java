package com.thor.admin.controller;

import com.thor.common.entity.ThorNode;
import com.thor.common.mapper.ThorNodeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/thor/node")
public class ThorNodeController {

    @Autowired
    private ThorNodeMapper thorNodeMapper;

    /**
     * 获取所有节点的实时状态列表
     * 该接口将对接 Vben 前端的 NodeMonitor.vue 页面
     */
    @GetMapping("/list")
    public Map<String, Object> getNodeList() {
        // 使用 MyBatis-Plus 查询全表节点数据
        List<ThorNode> nodeList = thorNodeMapper.selectList(null);

        // 严格按照 Vben / Ant Design 标准构建返回 JSON 结构
        Map<String, Object> response = new HashMap<>();
        response.put("code", 0); // 0 代表成功
        response.put("message", "获取节点列表成功");
        response.put("result", nodeList); // 前端表格的数据源

        return response;
    }
}