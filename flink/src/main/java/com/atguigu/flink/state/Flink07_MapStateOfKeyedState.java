package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 *按键分区状态：
 *   Map状态 : 可以当初一个map使用用以维护多个kv值
 UV get(UK key) ;
 void put(UK key, UV value) ;
 void putAll(Map<UK, UV> map) ;
 void remove(UK key) ;
 boolean contains(UK key) ;
 Iterable<Map.Entry<UK, UV>> entries() ;
 Iterable<UK> keys() ;
 Iterable<UV> values() ;
 boolean isEmpty() ;
 */
public class Flink07_MapStateOfKeyedState {

    // 案例需求：去每种传感器重复的水位值
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


                            private MapState<Integer,Integer> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                MapStateDescriptor<Integer, Integer> mapStateDescriptor = new MapStateDescriptor<>("mapState", Integer.class, Integer.class);
                                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                Integer vc = value.getVc();
                                mapState.put(vc,null);
                                // 输出
                                out.collect("传感器:" + value.getId() + ",值:" + mapState.keys());
                            }
                        }).print();




        env.execute();

    }

}
