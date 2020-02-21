package com.atguigu.constants.gmalllogger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
   @GetMapping("test")
   // @ResponseBody
    public String test(){
        System.out.println(1111111);
        return  "success";
    }
    @PostMapping("log")
    public String logger(@RequestParam("logString") String logstr){
//添加时间戳字段
        JSONObject jsonObject = JSON.parseObject(logstr);
        jsonObject.put("ts",System.currentTimeMillis());
        //打印日志到控制台
        String tsJson = jsonObject.toString();
        log.info(tsJson);
        //使用kafka生产者将数据发送到kafka集群
        if(tsJson.contains("startup")){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,tsJson);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, tsJson);
        }
        return "success";

   }
}
