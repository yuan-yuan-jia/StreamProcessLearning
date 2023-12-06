package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流操作
 * 1. 简单操作:使用filter算子
 * 2. 使用侧输出流
 */
public class Flink08_SplitStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 简单分流操作
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);
        // 将主流中的数拆分到多条流中，zhangsan一条流，lisi一条流
        SingleOutputStreamOperator<Event> zhangsanDs = ds.filter(e -> "zhangsan".equals(e.getUser()));
        SingleOutputStreamOperator<Event> lisiDs = ds.filter(e -> "lisi".equals(e.getUser()));
        SingleOutputStreamOperator<Event> otherDs = ds.filter(e -> !("lisi".equals(e.getUser()) || "zhangsan".equals(e.getUser())));
        //zhangsanDs.print("zhangsan");
        //lisiDs.print("lisi");
        //otherDs.print("other");



        // 使用测输出流
        /// 创建输出标签
        OutputTag<Event> zhangsanTag = new OutputTag<>("zhangsanDs", Types.POJO(Event.class));
        OutputTag<Event> lisiTag = new OutputTag<>("lisiDs", Types.POJO(Event.class));
        SingleOutputStreamOperator<Event> mainDs = ds.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                // 通过对每个数据的判断，决定将数据发到哪个流中
                if ("zhangsan".equals(value.getUser())) {
                    // zhangsanDs
                    ctx.output(zhangsanTag, value);
                } else if ("lisi".equals(value.getUser())) {
                    // lisiDs
                    ctx.output(lisiTag, value);
                } else {
                    // 主流
                    out.collect(value);
                }
            }
        });
        mainDs.print("other");
        // 主流中捕获测流
        SideOutputDataStream<Event> zhangsanDs1 = mainDs.getSideOutput(zhangsanTag);
        SideOutputDataStream<Event> lisiDs1 = mainDs.getSideOutput(lisiTag);
        zhangsanDs1.print("zhangsan");
        lisiDs1.print("lisi");

        env.execute();
    }
}
