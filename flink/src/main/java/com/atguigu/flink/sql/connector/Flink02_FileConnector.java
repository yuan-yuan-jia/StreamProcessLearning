package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_FileConnector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String createTable =
                "create table t1 (" +
                        "id string," +
                        "vc int," +
                        "ts bigint," +
                        // 获取文件的名称
                       // "file.name string not null METADATA," +
                        // 获取文件的大小
                        "`file.size` bigint not null METADATA" +
                        ") WITH (" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'input/ws.txt'," +
                        "'format' = 'csv'" +
                        ")";


        tableEnv.executeSql(createTable);

        tableEnv.sqlQuery("select * from t1").execute().print();

        //File Sink
        String sinkTable = "create table t1 (" +
                "id string," +
                "vc int," +
                "ts bigint," +
                // 获取文件的名称
                // "file.name string not null METADATA," +
                // 获取文件的大小
                "file_size bigint" +
                ") WITH (" +
                "'connector' = 'filesystem'," +
                "'path' = 'output/wd.txt'," +
                "'format' = 'csv'" +
                ")";

        tableEnv.executeSql(sinkTable);

        tableEnv.executeSql("insert into t2 select id,vs,ts from t1").await();



        env.execute();
    }
}
