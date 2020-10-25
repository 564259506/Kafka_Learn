package com.heima.kafka.chapter3;

import com.heima.kafka.ConsumerClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 再均衡监听器
 */
public class CommitSyncInRebalance extends ConsumerClientConfig {
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 劲量避免重复消费
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //do nothing.
            }
        });

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.offset() + ":" + record.value());
                    // 异步提交消费位移，在发生再均衡动作之前可以通过再均衡监听器的onPartitionsRevoked回调执行commitSync方法同步提交位移。
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }

    }
}
