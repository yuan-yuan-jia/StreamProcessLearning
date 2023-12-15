package com.atguigu.flink.sql.time;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 流转表，指定时间字段
 */
public class Flink01_StreamToTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                        .withIdleness(Duration.ofSeconds(5))
                );


        // 流转表
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("vc",DataTypes.INT())
                .column("ts",DataTypes.BIGINT())
                // 时间字段
                //处理时间
                // 调用函数获取系统时间，作为赋给pt处理时间
                .columnByExpression("pt", "proctime()")

                // 事件时间
                // 将ts转为事件时间
                .columnByExpression("et","To_TIMESTAMP_LTZ(ts,3)")
                // 指定生成水位线的字段，如果流中已经提供了水位线，可以直接沿用流中的水位线
                //.watermark("et","source_watermark()")
                // 单独生成水位线，不使用流中的水位线,延迟2两秒
                .watermark("et","et - INTERVAL '2' SECOND")
                .build();

        Table table = tableEnv.fromDataStream(ds, schema);
        table.printSchema();


        env.execute();
    }

}
