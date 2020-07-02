package com.tj.jiangzh.kafka.wechat.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Resource
    private KafkaProperties kafkaProperties;

    @Bean
    public Producer kafkaProducer(){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaProperties.getBootstrapServersConfig());
        properties.put(ProducerConfig.ACKS_CONFIG,kafkaProperties.getAcksConfig());
        properties.put(ProducerConfig.RETRIES_CONFIG,kafkaProperties.getRetriesConfig());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaProperties.getBatchSizeConfig());
        properties.put(ProducerConfig.LINGER_MS_CONFIG,kafkaProperties.getLingerMsConfig());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,kafkaProperties.getBufferMemoryConfig());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaProperties.getKeySerializerClassConfig());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,kafkaProperties.getValueSerializerClassConfig());
        //这里指定了partition的内容
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,kafkaProperties.getPartitionerClassConfig());

        //Producer的主对象
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        return producer;
    }
}
