package com.huawei.one;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 */
public class 自定义生产者拦截器 implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
//        if (record.value().length() < 5) {
//            throw new RuntimeException();
//        }
//        return record;
    }

    //计数
    @Override
    public void onAcknowledgement(
            RecordMetadata recordMetadata,
            Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    //统计成功率
    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率="
                + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
