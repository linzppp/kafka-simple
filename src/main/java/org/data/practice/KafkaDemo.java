package org.data.practice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class KafkaDemo {
    public static void main(String[] args) {
        // 1. 配置消费者属性
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "202.205.176.58:7080"); //切换正式/测试的ip port
        // 消费者组ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "beijing-cmis"); // 消费者组名固定

        // 键值反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 从何处开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

        // 是否自动提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 自动提交间隔 (毫秒)
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            listTopics(consumer);
            consumeMsg(consumer);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void listTopics(KafkaConsumer<String, String> consumer) {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        List<String> topicNames = topics.keySet().stream().collect(Collectors.toList());
        for (String topicName : topicNames) {
            log.info("Topic: " + topicName);
            List<PartitionInfo> partitions = topics.get(topicName);
            for (PartitionInfo partitionInfo : partitions) {
                log.info("partitionInfo:" + partitionInfo.toString());
            }
        }
    }

    private static void consumeMsg(KafkaConsumer<String, String> consumer) {
        // 订阅主题
        String topic = "qgxj-zjdj-beijing"; // topic固定了
        consumer.subscribe(Collections.singletonList(topic));

        log.info("开始消费主题: " + topic);

        // 持续轮询消息
        while (true) {
            // 等待消息，最长等待1000毫秒
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // 处理接收到的消息
            for (ConsumerRecord<String, String> record : records) {
                log.info("\n--- 收到新消息 ---\n" +
                                "主题: %s\n" +
                                "分区: %d\n" +
                                "偏移量: %d\n" +
                                "键: %s\n" +
                                "值: %s\n" +
                                "时间戳: %d\n",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value(),
                        record.timestamp());
            }
        }
    }
}