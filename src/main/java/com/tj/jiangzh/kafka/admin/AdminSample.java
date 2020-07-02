package com.tj.jiangzh.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.admin.NewPartitions.increaseTo;

/**
 * @author taojie
 */
public class AdminSample {

//    public static AdminClient adminClient;
    public final static String TOPIC_NAME  ="jz_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        AdminClient adminClient=AdminSample.adminClient();
//        System.out.println(adminClient);
//        创建topic实例
//        createTopic();
//        delTopics("aa");
//        topicList();
//          描述topic
//        describeTopics(TOPIC_NAME);
//        alterConfig(TOPIC_NAME);
//        查看config
//        describeConfig(TOPIC_NAME);

//        增加topic分区
//        increPartitions(TOPIC_NAME,2);
        describeTopics(TOPIC_NAME);
    }



    /**
     * 设置adminClient
     * @return
     */
    public static AdminClient adminClient(){
        Properties properties=new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.213.161:9092");
        AdminClient adminClient=AdminClient.create(properties);
        return adminClient;
    }


    /**
     * 创建Topic实例
     */
    public static void createTopic(){
        AdminClient adminClient=adminClient();
        /**
         * String name  名字
         * int numPartitions 分区数
         * short replicationFactor 副本因子
         */
        short rs=1;
        NewTopic newTopic=new NewTopic(TOPIC_NAME,1,rs);
        CreateTopicsResult topicsResult=adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("CreateTopicsResult:"+topicsResult);
    }

    /**
     * 获取topic列表
     */
    public static void topicList() throws ExecutionException, InterruptedException {

        AdminClient adminClient=adminClient();
        //是否查看internal选项；内部的；内部的；体内的
        ListTopicsOptions options=new ListTopicsOptions();
        options.listInternal(true);
//        ListTopicsResult listTopicsResult=adminClient.listTopics();
        ListTopicsResult listTopicsResult=adminClient.listTopics(options);
        Set<String> names= listTopicsResult.names().get();
        Collection<TopicListing> topicListings= listTopicsResult.listings().get();
        Map<String,TopicListing> mapKafkaFuture= mapKafkaFuture=listTopicsResult.namesToListings().get();
        //打印names
        names.stream().forEach(System.out::println);
        topicListings.stream().forEach(System.out::println);
        System.out.println(mapKafkaFuture);
    }

    /**
     * 删除topics
     */
    public static void delTopics(String ... topics ) throws ExecutionException, InterruptedException {
        AdminClient adminClient=adminClient();
        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(topics));
        deleteTopicsResult.all().get();
    }

    /**
     * 描述topic
     * name:jz_topic,
       desc(name=jz_topic,
        internal=false,
        partitions=(partition=0,
        leader=192.168.213.161:9092 (id: 0 rack: null),
        replicas=192.168.213.161:9092 (id: 0 rack: null),
        isr=192.168.213.161:9092 (id: 0 rack: null)),
        authorizedOperations=null)
     */
    public static void describeTopics(String ... topics) throws ExecutionException, InterruptedException {
        AdminClient adminClient=adminClient();
        DescribeTopicsResult describeTopicsResult=adminClient.describeTopics(Arrays.asList(topics));
        Map<String,TopicDescription> stringTopicDescriptionMap=describeTopicsResult.all().get();
        Set<Map.Entry<String,TopicDescription>> entries=stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entry)->{
            System.out.println("name:"+entry.getKey()+",desc"+entry.getValue());
        });
    }

    /**
     * 查看config
     * name:ConfigResource(type=TOPIC, name='jz_topic'),
     * descConfig(entries=
     *  [ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=message.format.version, value=2.1-IV2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *  ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])

     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException
     * name:ConfigResource(type=TOPIC, name='jz_topic'),
     * descConfig(entries=[
     *              ConfigEntry(name=compression.type,
     *                          value=producer,
     *                             source=DEFAULT_CONFIG,
     *                             isSensitive=false,
     *                             isReadOnly=false,
     *                             synonyms=[]),
     *              ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=message.format.version, value=2.1-IV2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *              ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])

     */
    public static void describeConfig(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient=adminClient();
        //TODO 这个做个预留，集群是会讲到
//        ConfigResource configResource=new ConfigResource(ConfigResource.Type.BROKER,topicName);
        ConfigResource configResource=new ConfigResource(ConfigResource.Type.TOPIC,topicName);
        DescribeConfigsResult describeConfigsResult=adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource,Config> configResourceConfigMap=describeConfigsResult.all().get();
        configResourceConfigMap.entrySet().stream().forEach((entry)->{
            System.out.println("name:"+entry.getKey()+",desc"+entry.getValue());
        });
    }

    /**
     * 修改config信息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void alterConfig(String name) throws ExecutionException, InterruptedException {
      //第一个版本
        AdminClient adminClient=adminClient();
        Map<ConfigResource,Config> configMaps=new HashMap<>();
        //组织两个参数
        ConfigResource configResource=new ConfigResource(ConfigResource.Type.TOPIC,name);
        Config config=new Config(Arrays.asList(new ConfigEntry("preallocate","false")));
        configMaps.put(configResource,config);

        AlterConfigsResult alterConfigsResult= adminClient.alterConfigs(configMaps);
        alterConfigsResult.all().get();

      // ncrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>

        /**
         * 从2.3 以上的版本才支持，我虚拟机装的版本低，不支持
         */

      /*
      AdminClient adminClient=adminClient();
      ConfigResource configResource=new ConfigResource(ConfigResource.Type.TOPIC,name);
      //AlterConfigOp(ConfigEntry configEntry, AlterConfigOp.OpType operationType)
      ConfigEntry configEntry=new ConfigEntry("preallocate","false");
      AlterConfigOp alterConfigOp=new AlterConfigOp(configEntry,AlterConfigOp.OpType.SET) ;
      Map<ConfigResource, Collection<AlterConfigOp>> configResourceListMap=new HashMap<>();
      configResourceListMap.put(configResource,Arrays.asList(alterConfigOp));
      AlterConfigsResult alterConfigsResult=adminClient.incrementalAlterConfigs(configResourceListMap);
      alterConfigsResult.all().get();*/
    }


    /**
     * 增加分区
     * kafka的分区只能添加不能删除，减少
     */
    public static void increPartitions(String topicName,int partitionNum) throws ExecutionException, InterruptedException {
        AdminClient adminClient=adminClient();
        Map<String,NewPartitions> partitionsMap=new HashMap<>();
        NewPartitions newPartitions= NewPartitions.increaseTo(partitionNum);
        partitionsMap.put(topicName,newPartitions);

        CreatePartitionsResult createPartitionsResult=adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }

}
