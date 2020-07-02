package com.tj.jiangzh.kafka.wechat.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Optional;
import com.tj.jiangzh.kafka.wechat.config.WechatTemplateProperties;
import com.tj.jiangzh.kafka.wechat.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Service
public class WeChatTemplateServiceImpl implements WeChatTemplateService {

    @Resource
    private WechatTemplateProperties properties;

    @Resource
    private Producer producer;

    /**
     * 获取目前active为true的调查问卷模版
     * @return
     */
    @Override
    public WechatTemplateProperties.WechatTemplate getWechatTemplate() {
        List<WechatTemplateProperties.WechatTemplate> templates=properties.getTemplates();
        Optional<WechatTemplateProperties.WechatTemplate> wechatTemplateOptional=templates.stream().filter((template)->template.isActive()).findFirst();

        return wechatTemplateOptional.isPresent()?wechatTemplateOptional.get():null;
    }

    @Override
    public void templateReported(JSONObject reportInfo) {

        // kafka Producer 将数据推送至kafka Topic
        log.info("templateReported:[{}]",reportInfo);

        String topicName="jiangzh-topic";

        //发送kafka数据
        String templateId=reportInfo.getString("templateId");
        JSONArray reportData=reportInfo.getJSONArray("result");
        //如果templateid相同，后续在统计分析时，可以考虑将相同的ID的内容放入同一个partition，便于分析使用
        ProducerRecord<String,Object> record=
                new ProducerRecord<>(topicName,templateId,reportData);

        /**
         * 1.kafka producer是线程安全的，建议多线程复用，如果每个线程都创建，则出现大量的上下文
         * 切换或争抢的情况，影响kafka效率
         * 2.kafka producer 的key是一个很重的内容
         *     2.1  我们可以根据key完成的Partition的负载均衡（相同的模版进入相同的partition），注意不要产生热点
         *     2.2  合理的key设计，可以让Flink、Spark Streaming之类的试试分析工具做更快速的处理
         * 3. ack=all ,kafka层面上就已经有了只有一次的消息投递保障，但是如果真的不丢数据，最好自行处理异常
         */

        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //保证发送成功
                    if(null==exception){
                        //发送成功
                    }else{
                        //出了异常，引入缓存redis、es..或其他方式保存数据进行重发等

                    }
                }
            });
        }catch (Exception e){
            //将数据加入重发队列（redis、es.....）
        }



    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        // 判断数据结果获取类型
        if(properties.getTemplateResultType()==0){
            //文件获取
            return FileUtils.readFile2JsonObject(properties.getTemplateResultFilePath()).get();
        }else if(properties.getTemplateResultType()==1){
            //DB...

        }else if(properties.getTemplateResultType()==2){

        }
        return null;
    }
}
