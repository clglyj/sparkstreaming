package com.atguigu.gmalllogger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String ,String >  kafkaTemplate ;




    @RequestMapping(value = "log",method = RequestMethod.POST)
    public String  test(@RequestParam("logString")String para){
        //打印日志
        JSONObject  jsonStr = JSON.parseObject(para);
        jsonStr.put("ts",System.currentTimeMillis());
        String tsJson = jsonStr.toString();
        log.info(tsJson);

        String topic  = "";
        if(tsJson.contains("startup")){
            //kafka发送消息数据
            topic = GmallConstants.GMALL_STARTUP_TOPIC ;
            log.info("------------------");
        }else{
            topic = GmallConstants.GMALL_EVENT_TOPIC;
            log.info("++++++++++++++++++");
        }
        kafkaTemplate.send(topic,tsJson);
        return   "success";
    }

}
