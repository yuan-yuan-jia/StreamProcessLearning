package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 简单查询
 */
public class Flink01_SimpleQuery {

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
                .column("vc", DataTypes.INT())
                .column("ts", DataTypes.BIGINT())
                // 时间字段
                //处理时间
                // 调用函数获取系统时间，作为赋给pt处理时间
                .columnByExpression("pt", "proctime()")

                // 事件时间
                // 将ts转为事件时间
                .columnByExpression("et", "To_TIMESTAMP_LTZ(ts,3)")
                // 指定生成水位线的字段，如果流中已经提供了水位线，可以直接沿用流中的水位线
                //.watermark("et","source_watermark()")
                // 单独生成水位线，不使用流中的水位线,延迟2两秒
                .watermark("et", "et - INTERVAL '0' SECOND")
                .build();

        Table table = tableEnv.fromDataStream(ds, schema);
        tableEnv.createTemporaryView("t1", table);

        //select where
        String querySQl = "select id ,vc,ts,pt,et from t1";
        //tableEnv.sqlQuery(querySQl).execute().print();


        //with: 公共表达式
        tableEnv.sqlQuery("with t2 as (" +
                "select id ,vc,ts,pt,et from t1 where vc >= 100" +
                ")" +
                "select * from t2").execute();//.print();


        //distinct
//        tableEnv.sqlQuery("select distinct id from t1")
//                .execute()
//                .print();


        //group by
//        tableEnv.sqlQuery("select id,sum(vc) sum_vc from t1 group by id ")
//                .execute()
//                .print();

        //order by 在流模式中，只能基于时间字段进行升序排序
        // limit:只支持批模式

        //SQL Hits:
        //  用于临时改连接器配置
        //tableEnv.sqlQuery("select id,vc,ts from t3 /** OPTIONS('topic' = 'topic_b')  */")


        // 集合操作
        // 并集 union 和union all

        //

        // 交集 Intersect 和Intersect all

        // 差集 Except 和 Except all


        env.execute();
    }

}
