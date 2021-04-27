package xyz.hyhy.stayalert.api.controller;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import xyz.hyhy.stayalert.api.entity.CountData;
import xyz.hyhy.stayalert.api.entity.UserData;
import xyz.hyhy.stayalert.api.service.CountDataService;
import xyz.hyhy.stayalert.api.service.UserDataService;

import javax.annotation.Resource;

/**
 *
 */
@Controller
@CrossOrigin
public class TestController {

    @Resource
    private CountDataService countDataService;

    @GetMapping("/test/generate-fake-count-data")
    @ResponseBody
    public String generateFakeCountData() {
        countDataService.generateFakeCountData();
        return "OK";
    }

}
