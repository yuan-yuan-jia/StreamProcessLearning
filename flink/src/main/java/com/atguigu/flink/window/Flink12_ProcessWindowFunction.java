package com.atguigu.flink.window;

import com.atguigu.flink.pojo.UserViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * ProcessWindowFunction:
 * 1. 将数据收集齐后，统一计算输出结果
 * 2. 获取窗口信息，开始时间和结束时间等
 * 3. 泛型:
 *    IN: 输入数据类型
 *    OUT: 输出的数据类型
 *    KEY: key的类型
 *    W: 窗口的类型
 * 4.   process(
 *             KEY key, Context context, Iterable<IN> elements, Collector<OUT> out)
 *            (1) :KEY key ,当前的key
 *            (2): Iterable<IN> elements,当前窗口的数据
 *
 */
public class Flink12_ProcessWindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((ev, ts) -> ev.getTs()));


        // 统计，每10秒内每用户的点击次数

        ds1.map(ev -> new WordCount(ev.getUser(), 1))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WordCount, UserViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WordCount, UserViewCount, String, TimeWindow>.Context context, Iterable<WordCount> elements, Collector<UserViewCount> out) throws Exception {
                        // user: key
                        // count: 迭代elements,求count
                        long count = 0;
                        for (WordCount element : elements) {
                            count++;
                        }
                        // 窗口信息
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        //封装结果
                        out.collect(new UserViewCount(s,count,start,end));
                    }
                }).print("window");
        env.execute();
    }


}
