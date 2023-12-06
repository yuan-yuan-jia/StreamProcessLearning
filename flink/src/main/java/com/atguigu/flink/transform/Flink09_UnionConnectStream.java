package com.atguigu.flink.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 合流操作：
 * 1. union(联合)
 *  被union的流中的数据类型必须一致，
 * 2. connect(连接)
 *   允许被connect的两个流中数据类型可以不一致
 *   通过connect后得到ConnectedStream，在ConnectedStream中会将
 *   两条流独立的维护，需要经处理，让两条流中的数据变成同一的类型，最终输出的一条流中
 */
public class Flink09_UnionConnectStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> ds2 = env.fromElements(6,7,8,9);
        DataStreamSource<String> ds3 = env.fromElements("a","b","c");

        //union
        DataStream<Integer> unionDs = ds1.union(ds2);
        //unionDs.print("union");

        //connect
        ConnectedStreams<Integer, String> connectedStreams = ds1.connect(ds3);

        SingleOutputStreamOperator<String> processedStream = connectedStreams.process(new CoProcessFunction<Integer, String, String>() {

            /*
              处理第一条流中的数据
             */
            @Override
            public void processElement1(Integer value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(String.valueOf(value));
            }
            /*
              处理第二条流的数据
             */

            @Override
            public void processElement2(String value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toUpperCase());
            }
        });

        processedStream.print("connected");

        env.execute();
    }

}
