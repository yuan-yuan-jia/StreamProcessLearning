package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 *按键分区状态：
 *   列表状态 : 维护多个值
 *    void update(List<T> values)
 *    void addAll(List<T> values)
 *    Out get(): 获取状态中的值
 *    void add(IN value) : 将指定的值添加到状态中
 *    void clear()
 */
public class Flink06_ListStateOfKeyedState {

    // 案例需求：检测每种传感器的水位值，输出最高的水位值
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
                            private ListState<Integer> listState;




                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("valueState", Integer.class);
                                listState = getRuntimeContext().getListState(listStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                listState.add(value.getVc());
                                ArrayList<Integer> allVc = new ArrayList<>();

                                Iterable<Integer> integers = listState.get();
                                for (Integer integer : integers) {
                                    allVc.add(integer);
                                }
                                allVc.sort(new Comparator<Integer>() {
                                    @Override
                                    public int compare(Integer o1, Integer o2) {
                                        return Integer.compare(o2,o1);
                                    }
                                });


                                if (allVc.size() > 3) {
                                    allVc.remove(3);
                                }

                                out.collect(value.getId() + "最高的3个水位值:" + StringUtils.join(allVc,","));

                            }
                        }).print();




        env.execute();

    }

}
