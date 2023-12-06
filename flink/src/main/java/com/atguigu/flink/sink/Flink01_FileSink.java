package com.atguigu.flink.sink;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * FileSystem Connector -> FileSink
 *
 * Sink算子：
 * 1. 旧的addSink(SinkFunction)
 * 2. 新的 sinkTo(Sink);
 *
 * 目前使用的大多数sink，都是基于2pc(两阶段提交)的方式来保证状态一致性，
 * 2pc需要flink的检查点机制，因此使用对应的sink的时候要开启Flink的检查点机制
 */
public class Flink01_FileSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);
        // FileSink
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("output"), new SimpleStringEncoder<>())
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                // 多大滚动
                                .withMaxPartSize(MemorySize.parse("10", MemorySize.MemoryUnit.MEGA_BYTES))
                                // 多久滚动
                                .withRolloverInterval(Duration.ofSeconds(10))
                                // 多久不活跃滚动
                                .withInactivityInterval(Duration.ofHours(8))
                                .build()
                )
                // 目录滚动策略
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm"))
                // 检查间隔
                .withBucketCheckInterval(1000L)
                .withOutputFileConfig(new OutputFileConfig("pre", "log"))
                .build();

        ds.map(JSON::toJSONString)
          .sinkTo(fileSink);

        env.execute();
    }
}
