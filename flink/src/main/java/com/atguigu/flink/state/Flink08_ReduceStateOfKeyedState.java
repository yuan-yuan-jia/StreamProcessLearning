package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *按键分区状态：
 *   规约状态 : 类似值状态，将数据放入状态时，会先进行一次规约处理
 *   将处理结果放到状态中
      OUT get():获取状态中的值
 *    void add(IN value): 加入新值
 *    void clear()
 */
public class Flink08_ReduceStateOfKeyedState {

    // 案例需求：计算每种传感器水位值
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //id,vc,ts
        //s1,100,1000
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {

                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                });



        ds.keyBy(WaterSensor::getId)
                        .process(new KeyedProcessFunction<String, WaterSensor, String>() {


                            private ReducingState<Integer> reducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<>("reduceState", new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                }, Integer.class);
                                reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                reducingState.add(value.getVc());
                                Integer i = reducingState.get();
                                out.collect("id: " + value.getId() + ",规约后的状态为:" + i);
                            }
                        }).print();




        env.execute();

    }

}
