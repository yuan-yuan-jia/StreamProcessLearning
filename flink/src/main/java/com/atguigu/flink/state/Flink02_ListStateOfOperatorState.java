package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 托管状态
 * 算子状态：
 *  列表状态： 可以当成一个List来使用，状态管理、备份、恢复都
 *  由flink完成。
 *  使用列表状态：需要实现CheckpointedFunction接口
 *   每个算子并行实例初始时调用
 *   void initializeState(FunctionInitializationContext context)
 *    状态快照备份，随着检查点周期来进行调用(触发一次检查点，就调用一次)
 *   void snapshotState(FunctionSnapshotContext context)
 *
 */
public class Flink02_ListStateOfOperatorState {

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

    static class MyMapFunction implements MapFunction<String, String> , CheckpointedFunction {

        // 定义集合，用于维护每个数据
        // 列表状态
        private ListState<String> listState;



        @Override
        public String map(String value) throws Exception {
            listState.add(value);
            return listState.toString();
        }

        /*
        状态备份，将状态备份到检查点里。
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState");
        }

        /*
        初始化状态:
          状态初始化：
            1. 第一个次启动程序，没有历史的状态，直接创建一个状态处理
            2. 恢复启动：有历史的状态，将历史的状态数据恢复到当前的状态中。
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState", String.class);
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        }
    }

}
