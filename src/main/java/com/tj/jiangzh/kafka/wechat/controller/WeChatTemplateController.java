package com.tj.jiangzh.kafka.wechat.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.tj.jiangzh.kafka.wechat.common.BaseResponseVO;
import com.tj.jiangzh.kafka.wechat.config.WechatTemplateProperties;
import com.tj.jiangzh.kafka.wechat.service.WeChatTemplateService;
import com.tj.jiangzh.kafka.wechat.utils.FileUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;

@RestController
@RequestMapping("/v1")
public class WeChatTemplateController {

    @Resource
    private WechatTemplateProperties properties;

    @Resource
    private WeChatTemplateService weChatTemplateService;


    @RequestMapping(value = "/template",method = RequestMethod.GET)
    public BaseResponseVO getTemplate(){

        WechatTemplateProperties.WechatTemplate wechatTemplate=weChatTemplateService.getWechatTemplate();
        Map<String,Object> result= Maps.newHashMap();
        result.put("templateId",wechatTemplate.getTemplateId());
        result.put("template", FileUtils.readFile2JsonArray(wechatTemplate.getTemplateFilePath()));
        return BaseResponseVO.success(result);
    }


    @RequestMapping(value = "/template/result",method = RequestMethod.GET)
    public BaseResponseVO templateStatistics(@RequestParam(value = "templateId",required = true)String templateId){

        JSONObject statistics=weChatTemplateService.templateStatistics(templateId);

        return BaseResponseVO.success(statistics);
    }

    @RequestMapping(value = "/template/report",method = RequestMethod.POST)
    public BaseResponseVO dataReportied(
            @RequestBody String reportData){

        weChatTemplateService.templateReported(JSON.parseObject(reportData));
        return BaseResponseVO.success();
    }

}
