package com.tj.jiangzh.kafka.wechat.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "template")
public class WechatTemplateProperties {

    private List<WechatTemplate> templates;
    //0-文件获取 1-数据库获取 2-ES获取
    private int templateResultType;
    private String templateResultFilePath;



    @Data
    public static class WechatTemplate{
        private String templateId;
        private String templateFilePath;
        private boolean active;
    }
}
