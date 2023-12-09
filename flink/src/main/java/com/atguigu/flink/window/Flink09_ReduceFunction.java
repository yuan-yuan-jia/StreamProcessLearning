package com.atguigu.flink.window;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 增量聚合: 通过窗口分配器分配好窗口后，窗口中每来一条数据都要执行一次聚合处理，
 * 等到窗口结束的时候，将聚合的结果输出。

 * 全量聚合： 通过窗口分配器分配好窗口后，窗口中收集的数据，先不进行任何的聚合处理，而是维护起来，
 * 等到窗口触发计算时，一次将所有的数据进行一次计算然后输出结果。
 * ReduceFunction:
 *   两两聚合: 窗口来的第一个数据不参与
 *   输入类型和输出类型一致
 *   T reduce(T value1,T value2) throws Exception
 */
public class Flink09_ReduceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((ev, ts) -> ev.getTs()));


        // 统计，每10秒内每个用户的点击次数
        ds1.map(ev -> new WordCount(ev.getUser(), 1))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                        System.out.println("Flink07_Reducing...");
                        return new WordCount(value1.getWord(), value1.getCount() + value2.getCount());
                    }
                }).print("window");

        env.execute();
    }
}
