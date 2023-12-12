package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *TTL
 */
public class Flink10_TTL {

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

                                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                                        // 对于state的该操作类型，在ttl时间内没有该操作
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                        // 设置过期后的状态的可见性
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();

                                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
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
