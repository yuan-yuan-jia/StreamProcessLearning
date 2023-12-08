package com.atguigu.flink.window;

import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义水位线生成策略
 * 水位线的生成位置：可以在流处理的任意个环节生成水位线
 * 一般建议在数据源附近生成水位线(越靠近数据源越好)。
 *
 * WatermarkStrategy: 水位线策略对象
 * 1. WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
 * WatermarkGenerator: 水位线生成器
 *   void onEvent(T event,long eventTimestamp,WatermarkOutput output);
 *   给每个数据生成水位线，每来一条数据，该方法会被调用一次
 *   void onPeriodicEmit(WatermarkOutput output);
 *   按照设定的周期，生成水位线
 *
 * 2. default TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context)
 * TimestampAssigner: 时间戳分配器
 *   long extractTimestamp(T element,long recordTimestamp)
 *   从数据中提取时间戳
 */
public class Flink02_UnSortUserDefineWaterMarkStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置生成水位线的周期为500ms
        env.getConfig().setAutoWatermarkInterval(500);

        //tom,/home,1000
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {

                    String[] split = line.split(",");
                    Event event = new Event();
                    event.setUser(split[0]);
                    event.setUrl(split[1]);
                    event.setTs(Long.parseLong(split[2]));
                    return event;

                });

        ds.print("input");

        // 分配时间戳和水位线
        ds.assignTimestampsAndWatermarks(new MyWatermarkStrategy())
                        .print();


        env.execute();
    }

    static class MyWatermarkStrategy implements WatermarkStrategy<Event> {

        /*
          WatermarkGenerator: 用于生成水位线
         */
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new MyWatermarkGenerator();
        }

        /*
          创建TimestampAssigner，用户从数据中提取时间戳
         */
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {

            return new MyTimestampAssigner();
        }
    }

    static class MyTimestampAssigner implements TimestampAssigner<Event> {

        /*
          从数据中提取时间戳
         */
        @Override
        public long extractTimestamp(Event element, long recordTimestamp) {
            return element.getTs();
        }
    }

    static class MyWatermarkGenerator implements WatermarkGenerator<Event> {

        private Long maxTs = Long.MIN_VALUE;


        /*
        每来一条数据会调用一次
        可以用于给每条数据生成水位线
         */
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

            // 取最大时间戳
            maxTs = Math.max(maxTs,eventTimestamp);
            // 乱序流，每条数据生成水位线
            //output.emitWatermark(new Watermark(maxTs));
        }

        /*
         按照指定的周期来调用
         可以用于周期性生成水位线
         默认是200ms
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 有序性，周期生成水位线
            System.out.println("乱序流，周期生成水位线 " + maxTs);
            output.emitWatermark(new Watermark(maxTs));
        }
    }
}
