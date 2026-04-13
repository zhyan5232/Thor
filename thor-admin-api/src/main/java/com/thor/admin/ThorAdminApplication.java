package com.thor.admin;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
// 极度关键：让 Web 模块能扫描到 Common 模块里的 Mapper 接口
@MapperScan("com.thor.common.mapper")
public class ThorAdminApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(ThorAdminApplication.class, args);
        System.out.println("==================================================");
        System.out.println("  Thor Admin API (控制面) 启动成功！监听端口准备就绪  ");
        System.out.println("==================================================");
    }
}