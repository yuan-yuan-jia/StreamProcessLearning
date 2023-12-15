package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * KafkaConnector
 */
public class Flink04_UpsertKafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String createTable =
                "create table t1 (" +
                        "id string," +
                        "vc int," +
                        "ts bigint," +
                        "`topic` string not null metadata,"+
                        "`partition` int not null metadata," +
                        "`offset` bigint not null metadata" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'topic' = 'topic_b'," +
                        "'properties.group.id' = 'finksql'," +
                        "'value.format' = 'csv'," +
                        "'scan.startup.mode' = 'group-offsets'," +
                        // 没有偏移量是重置偏移量为
                        "'properties.auto.offset.reset' = 'latest'" +
                        ")";


        tableEnv.executeSql(createTable);

        tableEnv.sqlQuery("select * from t1").execute().print();



        // kafkaSink upsert模式
        String sinkTable = "create table t2 (" +
                "id string," +
                 "sumvc int," +
                // 声明主键,主键会作为kafka send的key
                "primary key(id) not enforced" +
                ") WITH (" +
                "'connector' = 'upsert-kafka'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'topic' = 'first'," +
                "'value.format' = 'json'," +
                "'key.format'='json'" +
               // "'sink.delivery-guarantee' = 'at-least-once'" +
                //"'sink.transactional-id-prefix' = ''"
                //"'properties.transaction.timeout.ms' = '3000'"
                ")";

        tableEnv.executeSql(sinkTable);


        tableEnv.executeSql("insert into t2 select id,sum(vc) sumvc from t1 group by id");






        env.execute();
    }
}
