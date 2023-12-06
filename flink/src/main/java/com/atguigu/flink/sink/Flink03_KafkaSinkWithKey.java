package com.atguigu.flink.sink;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Kafka Sink
 * Kafka 生产者
 * 1. 生产者对象:KafkaProducer
 * 2. Kafka生产者分区策略
 * （1）如果明确指定分区号，直接使用
 * （2）如果没有指定分区号，但Record中带了key就按照key的hash取余得到分区号
 * （3） 如果没有分区号和key，使用默认的粘性分区策略。
 * 3. 生产者的相关配置
 * (1) key.serializer
 * (2) value.serializer
 * (3) bootstrap.servers
 * (4) retries        重试次数
 * (5) batch.size     批次大小
 * (6) linger.ms      批次停留时间
 * (7) acks           应答级别
 * (8) transactional.id  事务id
 */
public class Flink03_KafkaSinkWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);


        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Event>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Event element, KafkaSinkContext context, Long timestamp) {
                                byte[] key = element.getUser().getBytes();
                                byte[] value = JSON.toJSONString(element).getBytes();
                                return new ProducerRecord<>("topicA",key,value);
                            }
                        }
                )
                // AT_LEAST_ONCE: 至少一次，表示数据可能会重复，需要考虑去重
                // EXACTLY_ONCE: 精确一次
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //.setTransactionalIdPrefix()
                .setProperty(ProducerConfig.RETRIES_CONFIG, "10")
                .build();

        ds.sinkTo(kafkaSink);

        env.execute();
    }
}
