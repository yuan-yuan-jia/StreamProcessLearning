package com.atguigu.flink.practice;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;

/**
 * 1. Flink提供的转换算子有哪些,分别解释每个算子的功能
 * map:  转换一个数据
 * filter： 根据条件过滤数据
 * flatmap： 可以过滤和转换
 * process： 底层算子，做自定义的操作
 * source: 提供数据源
 * sink: 写入数据源
 * connect: 合并流
 * union: 合并相同类型的流
 * 2. 分流和合流怎么做
 * 分流：
 * (1)通过filter算子过滤
 * (2)使用process算子开测流
 * 3. 简述富函数的功能
 * 富函数不仅包含数据处理操作，还有open和close方法，在任务实例开始时open方法执行一次
 * 用于打开需要的资源，close函数在有界流的环境中，结束时调用用于关闭资源
 * 4. 編程题:从Kafka TopicA主题中消费Event数据(json格式),统计每个URL的点击次数,并将结果写出到Kafka TopicB主题中
 */
public class UrlCountStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("flinkb")
                .setTopics("topicA")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KafkaRecordSerializationSchema<String>() {
                                         @Nullable
                                         @Override
                                         public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                             byte[] bytes = element.getBytes();
                                             ProducerRecord<byte[], byte[]> producerRecord =
                                                     new ProducerRecord<byte[], byte[]>("topicB", bytes, bytes);

                                             return producerRecord;
                                         }
                                     }
                ).build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String value) throws Exception {
                        Event event = JSON.parseObject(value, Event.class);
                        String url = event.getUrl();
                        return new WordCount(url, 1);
                    }
                }).keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                }).sum("count")
                .map(JSON::toJSONString)
                .sinkTo(kafkaSink);

        env.execute();
    }


}
