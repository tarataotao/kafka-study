package com.tj.jiangzh.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample  {

    private final static String TOPIC_NAME="jz_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        for (int i=0;i<100;i++){
            //Produer 异步发送演示
//            producerSend(TOPIC_NAME,"k"+i,"v"+i);

            //Producer异步阻塞发送演示（同步）
//            producerSendSyncSend(TOPIC_NAME,"k"+i,"v"+i);

            //Producer异步发送带回调函数
//            producerSendWithCallback(TOPIC_NAME,"key-"+i,"value-"+i);

            //Producer异步发送带回调函数partition负载均衡(注意，这里要看topic有几个分区，分区只有一个的情况下不要用这个)
            producerSendWithCallbackAndPartition(TOPIC_NAME,"key-"+i,"value-"+i);

        }
    }

    /**
     * producer异步发送模式
     */
    public static void producerSend(String topicName,String k,String v){

        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.213.161:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //Producer的主对象
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        //消息对象-ProducerRecoder
        long timestamp=System.currentTimeMillis();
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,k,v);

        producer.send(record);

        //所有的通道打开都需要关闭
        producer.close();
    }

    /**
     * producer同步发送
     * @param topicName
     * @param k
     * @param v
     */
    public static void producerSendSyncSend(String topicName,String k,String v) throws ExecutionException, InterruptedException {

        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.213.161:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //Producer的主对象
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        //消息对象-ProducerRecoder
        long timestamp=System.currentTimeMillis();
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,k,v);

        //返回值
        Future<RecordMetadata> recordMetadataFuture= producer.send(record);
        //主要的区别
        RecordMetadata recordMetadata=recordMetadataFuture.get();
        System.out.println("partition:"+recordMetadata.partition()+", offset"+recordMetadata.offset());
        //所有的通道打开都需要关闭
        producer.close();
    }

    /**
     * producer异步发送带回调函数
     * 如果发送的结果需要做记录，则可以使用带回调参数的用法
     */
    public static void producerSendWithCallback(String topicName,String k,String v){

        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.213.161:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //Producer的主对象
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        //消息对象-ProducerRecoder
        long timestamp=System.currentTimeMillis();
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,k,v);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if(null==exception){
                    //注意，每个分区下的offset的值是唯一的
                    System.out.println("partition: "+metadata.partition()+" ,offset:"+metadata.offset());
                }else{
                    //出错的情况下处理的逻辑
                    System.out.println("出错了========="+exception);
                }


            }
        });

        //所有的通道打开都需要关闭
        producer.close();
    }

    /**
     * Producer异步发送带回调函数，自定义分区
     * @param topicName
     * @param k
     * @param v
     */
    public static void producerSendWithCallbackAndPartition(String topicName,String k,String v){

        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.213.161:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //这里指定了partition的内容
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.tj.jiangzh.kafka.producer.PartitionSample");

        //Producer的主对象
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        //消息对象-ProducerRecoder
        long timestamp=System.currentTimeMillis();
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,k,v);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if(null==exception){
                    //注意，每个分区下的offset的值是唯一的
                    System.out.println("partition: "+metadata.partition()+" ,offset:"+metadata.offset());
                }else{
                    //出错的情况下处理的逻辑
                    System.out.println("出错了========="+exception);
                }


            }
        });

        //所有的通道打开都需要关闭
        producer.close();
    }
}
