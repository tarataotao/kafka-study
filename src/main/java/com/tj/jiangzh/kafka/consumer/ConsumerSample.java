package com.tj.jiangzh.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.units.qual.K;

import java.time.Duration;
import java.util.*;

/**
 * @author taojie
 */
@Slf4j
public class ConsumerSample {


    private final static String TOPIC_NAME="jz_topic";

    public static void main(String[] args) {
//        helloworld();
//        commitedOffset();
//        commitedOffsetWithPartition();
        commitedOffsetWithPartitionAssign();

    }


    /**
     * 自动提交offset
     * 这里不推荐使用这个，自动提交的话，如果消息没有处理完，但是offset已经提交，kafka下次不会重复发送
     */
    private static void helloworld(){

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.213.161:9092");
        //消费者组
        properties.setProperty("group.id","test");
        //自动提交
        properties.setProperty("enable.auto.commit","true");
        //多长时间自动提交
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);
        //消费订阅哪一个Topic或者几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while(true){
            //定时去拉去批量的数据
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(10000));
            for(ConsumerRecord<String,String> record:records){
                System.out.printf("topic = %s , partition = %s ,offset= %d ,key = %s ,value = %s%n",
                       record.topic(),record.partition(), record.offset(),record.key(),record.value());
            }
        }

    }

    /**
     * 手动提交offset
     *
     */
    private static void commitedOffset(){

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.213.161:9092");
        //消费者组
        properties.setProperty("group.id","test");
        //自动提交,这里设置成false则为手动提交
        properties.setProperty("enable.auto.commit","false");
        //多长时间自动提交
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);
        //消费订阅哪一个Topic或者几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while(true){
            //定时去拉去批量的数据
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(10000));
                try{
                    //注意，这里数据是批量拉取的，不要把提交的东西放入循环体内
                    for(ConsumerRecord<String,String> record:records) {
                        //数据入库：成功...,失败....
                        //TODO record to db
                        System.out.printf("topic = %s , partition = %s ,offset= %d ,key = %s ,value = %s%n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    }
                    //如果成功，手动通知offset提交
                    consumer.commitAsync();
                }catch (Exception e){
                    //如果失败则回滚，不要提交offset,则下次可以重复消费
                }

        }

    }


    /**
     * 手动提交offset，并且手动控制partition
     */
    private static void commitedOffsetWithPartition(){
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.213.161:9092");
        //消费者组
        properties.setProperty("group.id","test");
        //自动提交,这里设置成false则为手动提交
        properties.setProperty("enable.auto.commit","false");
        //多长时间自动提交
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);
        //消费订阅哪一个Topic或者几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while(true){
            //定时去拉去批量的数据
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(10000));
            try{
                //获取partition,每个partition单独处理
                //目标：对每个partition单独提交offset
                for(TopicPartition partition:records.partitions()){
                    List<ConsumerRecord<String, String>> partitionRecords=records.records(partition);
                    //注意，这里数据是批量拉取的，不要把提交的东西放入循环体内(这里可以多线程处理业务逻辑)
                    for(ConsumerRecord<String,String> record:partitionRecords) {
                        //数据入库：成功...,失败....
                        //TODO record to db
                        System.out.printf("topic = %s , partition = %s ,offset= %d ,key = %s ,value = %s%n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    //如果成功，手动通知offset提交
                    //注意：这里使用的是对每个分区的单独提交
                    //获取当前批次的最后一个offset（kafka的消息是顺序的）
                    long lastOffset=partitionRecords.get(partitionRecords.size()-1).offset();
                    //单个partition中的offset，并且进行提交
                    Map<TopicPartition, OffsetAndMetadata> offset=new HashMap<>();
                    //lastOffset+1 为下次读取的开始位置，如果不加1的情况下会发生重复消费的情况
                    offset.put(partition,new OffsetAndMetadata(lastOffset+1));
                    //提交offset
                    consumer.commitSync(offset);
                    System.out.println("================partition - "+partition+"  end ===================");
                }
            }catch (Exception e){
                //如果失败则回滚，不要提交offset
            }

        }


    }


    /**
     * 手动提交offset，并且手动控制partition,更高级
     * 只订阅某个topic下的某个分区的数据：
     * 前面的那个是把数据都拉取下来之后再做处理，但是这个程序是只拉取想要拉取的数据的分区的数据
     *
     *
     *
     */
    private static void commitedOffsetWithPartitionAssign(){

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.213.161:9092");
        //消费者组
        properties.setProperty("group.id","test");
        //自动提交,这里设置成false则为手动提交
        properties.setProperty("enable.auto.commit","false");
        //多长时间自动提交
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);

        //这里指定拉取某个或者某些分区的数据
        //jiangzh-topic -0,1两个partition
        TopicPartition p0=new TopicPartition(TOPIC_NAME,0);
//        TopicPartition p0=new TopicPartition(TOPIC_NAME,1);

        //消费订阅哪一个Topic或者几个Topic
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        //消费订阅某个topic的某个分区
        consumer.assign(Arrays.asList(p0));
        while(true){
            //定时去拉去批量的数据
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(10000));
            try{
                //获取partition,每个partition单独处理
                //目标：对每个partition单独提交offset
                for(TopicPartition partition:records.partitions()){
                    List<ConsumerRecord<String, String>> partitionRecords=records.records(partition);
                    //注意，这里数据是批量拉取的，不要把提交的东西放入循环体内(这里可以多线程处理业务逻辑)
                    for(ConsumerRecord<String,String> record:partitionRecords) {
                        //数据入库：成功...,失败....
                        //TODO record to db
                        System.out.printf("topic = %s , partition = %s ,offset= %d ,key = %s ,value = %s%n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    //如果成功，手动通知offset提交
                    //注意：这里使用的是对每个分区的单独提交
                    //获取当前批次的最后一个offset（kafka的消息是顺序的）
                    long lastOffset=partitionRecords.get(partitionRecords.size()-1).offset();
                    //单个partition中的offset，并且进行提交
                    Map<TopicPartition, OffsetAndMetadata> offset=new HashMap<>();
                    //lastOffset+1 为下次读取的开始位置，如果不加1的情况下会发生重复消费的情况
                    offset.put(partition,new OffsetAndMetadata(lastOffset+1));
                    //提交offset
                    consumer.commitSync(offset);
                    System.out.println("================partition - "+partition+"  end ===================");
                }
            }catch (Exception e){
                //如果失败则回滚，不要提交offset
            }

        }
    }


}
