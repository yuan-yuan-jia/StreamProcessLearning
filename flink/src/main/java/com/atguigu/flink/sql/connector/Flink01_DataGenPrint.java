package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DataGen Connector: 模拟生成测试数据
 * Print Connector: 将数据打印到控制台
 */
public class Flink01_DataGenPrint {

    public static void main(String[] args) throws Exception {
       // TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //DataGen
        String createTable =
                "create table t1 (" +
                "id string," +
                "vc int," +
                "ts bigint" +
                ") WITH (" +
                "'connector' = 'datagen'," +
                "'rows-per-second' = '1'," +
                "'fields.id.kind' = 'random'," +
                "'fields.id.length' = '6'," +
                "'fields.vc.kind' = 'random'," +
                "'fields.vc.min' = '100'," +
                "'fields.vc.max' = '1000'," +
                "'fields.ts.kind' = 'sequence'," +
                "'fields.ts.start' = '10000'," +
                "'fields.ts.end' = '100000000'" +
                ")";


        tableEnv.executeSql(createTable);

        //Table table = tableEnv.sqlQuery("select * from t1");
        //table.execute().print();


        // print
        String sinkTable = "create table t2(" +
                "id string," +
                "vc int," +
                "ts bigint" +
                ")with (" +
                "'connector' = 'print'," +
                "'print-identifier' = 'PRINT>>>>>')";

        tableEnv.executeSql(sinkTable);
        tableEnv.executeSql("insert into t2 select id,vc,ts from t1 where vc >= 200");

    }
}
