package com.tj.jiangzh.kafka.wechat.service;

import com.alibaba.fastjson.JSONObject;
import com.tj.jiangzh.kafka.wechat.config.WechatTemplateProperties;

public interface WeChatTemplateService {

    //获取微信调查问卷模版 -获取目前active为true的模版就可以了
    WechatTemplateProperties.WechatTemplate getWechatTemplate();

    //上报调查问卷填写结果
    void templateReported(JSONObject reportInfo);


    //获取调查问卷的统计结果
    JSONObject templateStatistics(String templateId);


}
