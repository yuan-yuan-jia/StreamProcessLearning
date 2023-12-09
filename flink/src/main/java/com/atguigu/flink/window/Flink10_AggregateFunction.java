package com.atguigu.flink.window;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Set;

/**
 * 增量聚合: 通过窗口分配器分配好窗口后，窗口中每来一条数据都要执行一次聚合处理，
 * 等到窗口结束的时候，将聚合的结果输出。

 * 全量聚合： 通过窗口分配器分配好窗口后，窗口中收集的数据，先不进行任何的聚合处理，而是维护起来，
 * 等到窗口触发计算时，一次将所有的数据进行一次计算然后输出结果。
 *  AggregateFunction:
 *    两两聚合，窗口中第一个数据不执行聚合
 *    输入类型和输出类型不一样
 *    泛型：
 *      IN： 输入数据的类型
 *      OUT：输出的数据类型
 *      ACC: 累加类型,按照实际的统计需求，定义对应的类型，储存相应的数据，最终计算得出数据
 *    方法
 *    ACC createAccumulator(); 创建累加器
 *    ACC add(IN value,ACC accumulator) : 累加过程
 *    OUT getResult(ACC accumulator): 获取结果
 *    ACC merge(ACC a,Acc b) 合并多个累加器,一般不用
 */
public class Flink10_AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((ev, ts) -> ev.getTs()));


        // 统计，每10秒内的uv(独立访客数)
        // 通过将每条数据中user提取存入到set集合中（ACC），集合会将重复的数据去重处理
        // 等到窗口结束时，直接取set集合的长度即为uv
        ds1.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new AggregateFunction<Event, Set<String>, Integer>() {
                            // 创建累加器，该方法只会被调用一次
                            @Override
                            public Set<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            // 每来一条数据，会被调用
                            @Override
                            public Set<String> add(Event value, Set<String> accumulator) {
                                accumulator.add(value.getUser());
                                return accumulator;
                            }

                            // 获取结果,触发窗口计算时，调用一次
                            @Override
                            public Integer getResult(Set<String> accumulator) {
                                return accumulator.size();
                            }

                            // 合并累加器
                            @Override
                            public Set<String> merge(Set<String> a, Set<String> b) {
                                a.addAll(b);
                                return a;
                            }
                        }).print("window");

        env.execute();
    }
}
