package org.chail;

import org.chail.liteflow.LiteflowService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext=SpringApplication.run(Application.class, args);
        LiteflowService liteflowService=applicationContext.getBean(LiteflowService.class);
        liteflowService.test();
    }

}
