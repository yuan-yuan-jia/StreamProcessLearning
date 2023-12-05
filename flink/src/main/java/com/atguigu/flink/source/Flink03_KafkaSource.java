package com.atguigu.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka Connector -> Kafka Source
 * Kafka 消费者:
 * 1. 消费方式：拉取
 * 2. 消费者对象：KafkaConsumer
 * 3. 消费原则：
 *   一个主题的一个分区，只能被一个消费者组中的一个消费者消费
 *   一个消费组中的一个消费者可以消费一个主图的多个分区
 * 4. 消费者相关的配置：
 * (1) key.deserializer  key的反序列化器
 * (2) value.deserializer value的反序列化器
 * (3) bootstrap.servers  集群的位置
 * (4) group.id           消费者组id
 * (5) auto.commit.interval
 * (6) enable.auto.commit
 * (7) auto.offset.reset offset重置
 * 重置的情况：
 *   1. 之前没有消费过
 *   2. 当前要消费的offset在kafka中已经不能存在了，可能是因为时间久数据被删除
 * 重置策略：
 *  1. earliest:头
 *  2. latest:尾
 * (8) isolation.level 隔离级别（生产者事务）
 *  1. 读已经提交
 *  2. 读未提交
 * 5. 消费者存在的为题
 *  1. 漏消费，导致数据丢失
 *  2. 重复消费，导致数据重复
 */
public class Flink03_KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("flink")
                .setTopics("topicA")
                //优先使用当前消费者组记录的offset进行消费，如果需要offset重置，
                // 按照指定的策略重置
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 如果还有其他配置
                .setProperty("isolation.level", "read_committed")
                .build();

        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        kafkaDs.print();


        env.execute();
    }
}
