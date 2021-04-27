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
public class StayAlertController {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private SimpMessagingTemplate simpMessagingTemplate;
    @Resource
    private UserDataService userDataService;
    @Resource
    private CountDataService countDataService;

    @MessageMapping("/send-device-data")
    public void receive(String deviceData) throws Exception {
        kafkaTemplate.send("stayalert-api_device-data", deviceData);
    }

    /**
     * 将预测和统计结果发发送给订阅者
     *
     * @param record
     * @throws InterruptedException
     */
    @KafkaListener(topics = {"stayalert-api_user-data"})
//    @KafkaListener(topics = {"stayalert-device-data"})
    public void subscribeUserData(ConsumerRecord<?, ?> record) throws InterruptedException {
        String value = (String) record.value();
        JSONObject obj = JSONObject.parseObject(value);
        String userId = obj.getString("id");
        simpMessagingTemplate.convertAndSend("/topic/user-data/" + userId, obj);
        //        System.out.println(value);
    }

    @GetMapping("/api/user-data")
    @ResponseBody
    public UserData getUserData(@RequestParam String userId) {
//        System.out.println("get");
        return userDataService.getUserData(userId);
    }

    /**
     * 将预测和统计结果发发送给订阅者
     *
     * @param record
     * @throws InterruptedException
     */
    @KafkaListener(topics = {"stayalert-api_count-data"})
    public void subscribeCountData(ConsumerRecord<?, ?> record) {
        String value = (String) record.value();
        simpMessagingTemplate.convertAndSend("/topic/count-data", JSONObject.parseObject(value));
//        System.out.println(value);
    }

    @GetMapping("/api/count-data")
    @ResponseBody
    public CountData getCountData() {
        return countDataService.getTodayCountDataDAO();
    }

}
