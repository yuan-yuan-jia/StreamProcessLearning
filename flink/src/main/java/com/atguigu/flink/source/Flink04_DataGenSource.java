package com.atguigu.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

/**
 * 数据生成Source
 * DataGen Connector -> Source
 *
 * 用于模拟生成测试数据
 */
public class Flink04_DataGenSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return UUID.randomUUID() + "->" + value;
            }
        }, 1000L,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> dataStreamSource = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "datagen");
        dataStreamSource.print();


        env.execute();
    }

}
