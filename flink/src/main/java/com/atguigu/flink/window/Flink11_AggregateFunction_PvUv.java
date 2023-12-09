package com.atguigu.flink.window;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Set;

/**

 在电商网站中,PV(页面固览量)和UV(独立访客数)是非常重要的两个流量指标。
 一般来说,PV统计的是所有的点击量:而对用户id进行去重之后,得到的就是UV。
 所以有时我们会用PV/UV这个比值,来表示“人均重复访问量”,也就是平均每个用户全访问多少次页面。这在一定程度上代表了用户的粘度。
 */
public class Flink11_AggregateFunction_PvUv {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((ev, ts) -> ev.getTs()));


        // 统计，每10秒内的uv(独立访客数)
        // 可以使用Tuple2<Integer,Set<String>> 来作为累加器类型，f0用于累加pv
        // f1用户记录uv,集合的长度集合uv
        ds1.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new AggregateFunction<Event, Tuple2<Integer,Set<String>>, Double>() {
                            // 创建累加器，该方法只会被调用一次
                            @Override
                            public Tuple2<Integer,Set<String>> createAccumulator() {
                                return Tuple2.of(0,new HashSet<>());
                            }

                            // 每来一条数据，会被调用
                            @Override
                            public Tuple2<Integer,Set<String>> add(Event value, Tuple2<Integer,Set<String>> accumulator) {
                                 accumulator.f0 = accumulator.f0 + 1;
                                 accumulator.f1.add(value.getUser());
                                return Tuple2.of(accumulator.f0,accumulator.f1);
                            }

                            // 获取结果,触发窗口计算时，调用一次
                            @Override
                            public Double getResult(Tuple2<Integer,Set<String>> accumulator) {

                                Integer pv = accumulator.f0;
                                int uv = accumulator.f1.size();

                                return pv / (double)uv;
                            }

                            // 合并累加器
                            @Override
                            public Tuple2<Integer,Set<String>> merge(Tuple2<Integer,Set<String>>  a, Tuple2<Integer,Set<String>> b) {

                                return null;
                            }
                        }).print("window");

        env.execute();
    }


}
