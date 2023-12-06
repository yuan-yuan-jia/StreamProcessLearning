package com.atguigu.flink.practice;

import com.alibaba.fastjson2.JSONObject;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 1. 一个作业的并行度如何确定?一个作业需要多少个slot如何确定?
 *  一个作业的并行度由合并后的算子 * 该算子的并行度
 *  一个作业需要多少个slot，算子之间可以共享slot,一般是任务中算子的最大并度
 * 2. Yarn应用模式作业提交流程
 *  程序提交后，AM启动Dispatcher和ResourceManager,Dispatcher启动JobMaster，
 *  JobMaster生成流逻辑图->作业图->执行流图之后,JobMaster向ResourceManager申请资源
 *  ResourceManager向YarnResourceManager申请资源，申请成功后，启动TaskManager,
 *  JobMaster将执行流图发给TaskManager，TaskManager根据图生成物理图，来部署任务
 * 3. Flink StreamExecutionEnvironment的执行模式有几种,分别是什么?
 *三种，流模式、批处理和自动模式
 * 4. Flink提供的基本转换算子有哪些?分别解释每个算子的功能
 *Filter:过滤想要的数据
 *Map: 将数据映射成另外一种格式
 *FlatMap：过滤并映射
 * 5. 編程题:从Kafka TopicA主题中消费Event数据(json格式),并统计每个人的点击次数
 */
public class ClickCountStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("flink")
                .setTopics("topicA")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setProperty("isolation_level", "read_committed")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkasource", Types.STRING)
                        .map(new MapFunction<String, Tuple2<String,Integer>>() {
                            @Override
                            public Tuple2<String,Integer> map(String value) throws Exception {
                                Event event = JSONObject.parseObject(value, Event.class);
                                return Tuple2.of(event.getUser(),1);
                            }
                        }).keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String,Integer> value) throws Exception {
                        return value.f0;
                    }
                }).sum(1).print();

        env.execute();
    }
}
