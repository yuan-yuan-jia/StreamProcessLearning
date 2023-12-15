package com.atguigu.flink.sql.api;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.source.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink02_TableToStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");

                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                });

        Table table = tableEnv.fromDataStream(ds);

        tableEnv.createTemporaryView("t1",table);
        //sql
        String appendSQL = "select user,url,ts from t1 where user <> 'zhangsan'";
        String updateSQL = "select user,count(*) cnt group by user";
        Table resultTable = tableEnv.sqlQuery(appendSQL);

        // 不支持更新查询转流
        DataStream<Row> resultDs = tableEnv.toDataStream(resultTable);

        Table updateResultTable = tableEnv.sqlQuery(updateSQL);
        // 支持更新和追加
        DataStream<Row> updateResultDs = tableEnv.toChangelogStream(updateResultTable);

        resultDs.print();
        updateResultDs.print();


        env.execute();



    }
}
