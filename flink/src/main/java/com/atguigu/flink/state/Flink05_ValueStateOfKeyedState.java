package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *按键分区状态：
 *   值状态 : 维护具体的一个值
 *   ValueState方法:
 *   T value(): 从状态回去维护的数据
 *   void update(T value): 把指定的数据更新到状态中
 */
public class Flink05_ValueStateOfKeyedState {

    // 案例需求：检测每种传感器的水位值，如果连续的两个水位值相差超过10，就输出报警
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

                            // 声明状态
                            private ValueState<Integer> valueState;




                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Integer.class);
                                valueState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                // 从状态中获取上一次的水位值
                                Integer oldState = valueState.value();
                                Integer newVc = value.getVc();
                                if (oldState == null) {
                                    out.collect("本次VC:" + newVc);
                                }else if(Math.abs(oldState - newVc) > 10) {
                                    out.collect("本次VC:" + newVc + ",上次vc:" + oldState + ",相差超过10");
                                }

                                valueState.update(newVc);

                            }
                        }).print();




        env.execute();

    }

}
