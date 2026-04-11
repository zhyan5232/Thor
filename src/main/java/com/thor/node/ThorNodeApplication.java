package com.thor.node;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.thor.node.core.mapper")
public class ThorNodeApplication {

    private static final Logger log = LoggerFactory.getLogger(ThorNodeApplication.class);

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ThorNodeApplication.class);
        // 禁用内嵌 Web 容器，作为纯后台节点启动
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
        log.info("Thor (雷神) Distributed Node initialized successfully in Non-Web mode.");
        log.info("Ready for lightning-fast file transmission.");
    }
}