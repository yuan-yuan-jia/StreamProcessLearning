package com.atguigu.flink.practice;


import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 1. Flink中的状态由哪些
 * 算子状态
 * listState、unionListState、broadcastState
 * 按键分区状态
 * valueState、listState、mapState、reduceState、aggregatorState
 * <p>
 * 2. Flink状态后端有哪些,区别是什么
 * 1. HashmapBackend
 * 数据存在TaskManager的内存中，读取速度快
 * 2. EmbeddedBackend
 * 数据存在TaskManager的数据目录中，
 * <p>
 * 3. 编程题:求每个传感器最高的3个水位值
 */
public class SensorTop3 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");

                    return new WaterSensor(split[0],
                            Integer.parseInt(split[1]),
                            Long.parseLong(split[2])
                    );
                });

        ds.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer,Integer> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, Integer> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor",
                                Integer.class, Integer.class
                        );

                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer vc = value.getVc();
                        mapState.put(vc,null);

                        List<Integer> list = new ArrayList<>();
                        Iterable<Integer> keys = mapState.keys();
                        for (Integer key : keys) {
                            list.add(key);
                        }

                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return Integer.compare(o2,o1);
                            }
                        });
                        List<Integer> top3 = new ArrayList<>();
                        for (int i = 0; i < list.size() && i < 3; i++) {
                            top3.add(list.get(i));
                        }


                        if (!top3.isEmpty()) {
                            out.collect("传感器id:" + value.getId() + ",最高的3个水位值是:" + top3);
                        }


                    }
                }).print();


        env.execute();
    }
}
