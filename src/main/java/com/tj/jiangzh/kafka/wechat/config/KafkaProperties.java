package com.tj.jiangzh.kafka.wechat.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("wechat.kafka")
public class KafkaProperties {

    private String  bootstrapServersConfig;
    private String  acksConfig;
    private String  retriesConfig;
    private String  batchSizeConfig;
    private String  lingerMsConfig;
    private String  bufferMemoryConfig;
    private String  keySerializerClassConfig;
    private String  valueSerializerClassConfig;
    private String  partitionerClassConfig;
}
