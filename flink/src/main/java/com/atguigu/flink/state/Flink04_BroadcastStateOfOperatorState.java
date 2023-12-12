package com.atguigu.flink.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 算子状态：
 *  广播状态；一般用于广播很少量的数据，一些通用的配置
 *  需要获取得到一条广播流(由一条流进行广播操作后得到)，
 *  通过广播流与数据流进行connect操作，对数据进行处理的过程中
 *  使用广播状态
 */
public class Flink04_BroadcastStateOfOperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据liu
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 8888);

        // 广播配置
        //配置流
        DataStreamSource<String> confDs = env.socketTextStream("hadoop102", 9999);
        // 处理成广播流
        MapStateDescriptor<String, String> mapState = new MapStateDescriptor<>("mapState", String.class, String.class);
        BroadcastStream<String> bcDs = confDs.broadcast(mapState);

        // 数据流与广播流
        ds1.connect(bcDs)
                        .process(new BroadcastProcessFunction<String, String, String>() {
                            /*
                            处理数据流
                             */
                            @Override
                            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                // 从广播状态中获取广播状态
                                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);

                                String flag = broadcastState.get("flag");
                                if ("1".equals(flag)) {
                                    System.out.println("执行1号逻辑");
                                }else if ("2".equals(flag)) {
                                    System.out.println("执行2号逻辑");
                                }else {
                                    System.out.println("执行默认逻辑");
                                }
                                out.collect(value);
                            }

                            /*
                            处理广播流
                             */

                            @Override
                            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);
                                broadcastState.put("flag",value);
                            }
                        }).print();

        env.execute();
    }

}
