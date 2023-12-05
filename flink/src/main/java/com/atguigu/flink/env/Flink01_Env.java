package com.atguigu.flink.env;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 执行环境：
 * 1. 本地环境
 * LocalStreamEnvironment：主要看程序执行的位置，即为当前的执行环境
 * 2. 远程环境
 * RemoteStreamEnvironment：明确指定远程的环境
 *
 * 环境的获取：
 * 1. StreamExecutionEnvironment.getExecutionEnvironment():按照实际情况获取执行环境
 * 2. 明确获取
 *    直接创建本地环境: StreamExecutionEnvironment.createLocalEnvironment()
 *    直接创建远程环境    StreamExecutionEnvironment.createRemoteEnvironment()
 *
 */
public class Flink01_Env {

    public static void main(String[] args) throws Exception {
       // StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("hadoop104",37522,"jar/1.jar");
        remoteEnv.setParallelism(1);

        remoteEnv.socketTextStream("hadoop102",8888)
                        .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                for (String s : value.split(" ")) {
                                    out.collect(Tuple2.of(s,1));
                                }
                            }
                        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).sum(1)
                        .print();

        remoteEnv.execute();


    }

}
