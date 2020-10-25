package com.huawei.controller;

import com.huawei.KafkaLearn02Application;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;

/**
 * Author:wxc
 * project: kafka_learn_02
 * ClassName: OneController
 * Date: 2020/10/25 22:16 周日
 * yy:猥琐别浪，等我发育。
 */

@RequestMapping
@RestController
public class OneController {
//    private static final Logger LOGGER= LoggerFactory.getLogger(kafkaLearn02Application.class);

    @Autowired
    private KafkaTemplate template;
    private static final String topic = "csfq";



    //1. 测试
    @RequestMapping("/index")
    public  String index(){
        return "hello,kafka";
    }

    /**
     *2.1消息发送者 producer
     * 没加事务配置时生效
     * 事务配置：spring.kafka.producer.transaction-id-prefix=kafka_tx.
     */
    @RequestMapping("/send/{input}")
    public  String sendToKafka(@PathVariable String input){
        this.template.send(topic,input);
        return "发送成功！消息为:"+input;
    }

    /**
     * 2.2 消息的接收者 consumer
     * @param input
     * @KafkaListener(id = "",topics = 主题名,groupId = "分组名")
     */
    @KafkaListener(id = "",topics = topic,groupId = "group.csfq")
    public void listener(String input){
        System.out.println("消息结果是："+input);
    }

    /**
     * 3.1 事务控制，方法一:
     *  template.executeInTransaction
     * 加了事务配置才生效,spring.kafka.producer.transaction-id-prefix=kafka_tx.
     */
    @RequestMapping("/transaction/{input}")
    public  String sendToKafka_transaction(@PathVariable String input){
        //加入事务的支持，共模拟二条消息
        template.executeInTransaction(t->{
            t.send(topic,input); //第一条消息
            if ("error".equals(input)) {//判断第一条消息
                throw new RuntimeException("input is error");
            }
            t.send(topic,input+"aaaaa");//第二条消息
            return true;
        });
        return "发送成功！消息为:"+input;
    }

    /**
     * 3.2 事务控制，方法二 (注解版本)
     *   @Transactional(rollbackFor = RuntimeException.class)
     *   事务配置spring.kafka.producer.transaction-id-prefix=kafka_tx.
     */
    @RequestMapping("/transaction2/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public  String sendToKafka_transaction2(@PathVariable String input){
            //加入事务的支持，共模拟二条消息
            template.send(topic,input); //第一条消息
            if ("error".equals(input)) {//判断第一条消息
                throw new RuntimeException("input is error");
            }
            template.send(topic,input+"aaaaa");//第二条消息

        return "发送成功！消息为:"+input;
    }
}
