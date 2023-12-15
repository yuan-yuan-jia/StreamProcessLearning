package com.atguigu.flink.checkpoint;


import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Kafka到Flink到Kafka精准一次
 */
public class Flink02_KafkaToFlinkToKafkaEOS {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启检查点
        env.enableCheckpointing(2000L);

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


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topicB")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // AT_LEAST_ONCE: 至少一次，表示数据可能会重复，需要考虑去重
                // EXACTLY_ONCE: 精确一次
                //.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flinkPro" + RandomUtils.nextInt(0,1000000))
                .setProperty(ProducerConfig.RETRIES_CONFIG, "10")
                // 生成者事务超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"600000")
                .build();

        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        kafkaDs.print();
        kafkaDs.sinkTo(kafkaSink);
    }
}
