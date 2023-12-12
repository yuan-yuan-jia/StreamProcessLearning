package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 原始状态：就相当于自己定义的一些普通变量(非Flink状态类型)，用于维护一些
 * 计算过程中的数据，Flink不会提供任何的帮助，也就意味着
 * 跟状态相关的所有操作都需要自己来完成，例如状态创建、管理、
 * 备份、恢复。
 */
public class Flink01_RawState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 开启了检查点，会无限重启
        env.enableCheckpointing(2000);

        //设置恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(1)));

        env.socketTextStream("hadoop102", 8888)
                .map(
                        //将读取到的数据:能维护到一个集合中,最后输出到控制台
                        new MyMapFunction()
                ).addSink(
                        //输出到控制台,如果数据中包含字母x抛出异常
                        new MySink()
                );


        env.execute();
    }


    static class MySink implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            if (value.contains("x")) {
                throw new RuntimeException("抛异常");
            }
            System.out.println("sink: " + value);
        }
    }

    static class MyMapFunction implements MapFunction<String, String> {

        // 定义集合，用于维护每个数据
        // 原始状态
        private List<String> data = new ArrayList<>();


        @Override
        public String map(String value) throws Exception {
            data.add(value);
            return data.toString();
        }
    }

}
