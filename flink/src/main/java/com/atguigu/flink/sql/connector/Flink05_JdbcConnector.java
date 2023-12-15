package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * KafkaConnector
 */
public class Flink05_JdbcConnector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String createTable =
                "create table t1 (" +
                        "url string," +
                        "cnt bigint," +
                        "window_start bigint,"+
                        "window_end bigint" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                       "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                        "'url' = 'jdbc:mysql://localhost/demo0717'," +
                        "'username'='root'," +
                        "'password'='root'," +
                        "'table-name'='url_count'"+
                        ")";


        tableEnv.executeSql(createTable);

        tableEnv.sqlQuery("select * from t1").execute().print();


        //jdbc sink
        String sinkTable = "create table t2 (" +
                "url string," +
                "cnt bigint," +
                "window_start bigint,"+
                "window_end bigint" +
               // 更新撤销(upsert)
                // "primary key(url) not enforced"
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = 'jdbc:mysql://localhost/demo0717'," +
                "'username'='root'," +
                "'password'='root'," +
                "'table-name'='url_count1'"+
                ")";

        tableEnv.executeSql(sinkTable);
        tableEnv.executeSql("insert into  t2 select url,cnt,window_start,window_end from t1");






    }
}
