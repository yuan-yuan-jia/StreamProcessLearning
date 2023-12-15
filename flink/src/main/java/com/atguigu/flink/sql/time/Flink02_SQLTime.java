package com.atguigu.flink.sql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 连接器，指定时间字段
 */
public class Flink02_SQLTime {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String sourceTable = "create table t1 (" +
                "id string," +
                "vc int," +
                "ts bigint," +
                // 指定处理时间
                "pt as PROCTIME()," +
                //事件时间
                "et as to_timestamp_ltz(ts,3)," +
                // 水位线
                "watermark for et as et - INTERVAL  '2' SECOND" +
                ") WITH (" +
                "'connector' = 'filesystem'," +
                "'path' = 'input/ws.txt'," +
                "'format' = 'csv'" +
                ")";

        tableEnv.executeSql(sourceTable);
        Table table = tableEnv.from("t1");

        table.printSchema();

    }

}
