package com.atguigu.flink.sql.api;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 程序架构:
 * 1. 准备环境
 * 1.1 流表环境
 * 1.2 表环境，从操作层面与流独立，底层处理还是流
 * 2. 创建表
 * 2.1 将流转换成表
 * 2.2 连接器表，直接对数据源读取数据
 * 3. 转换处理
 * 3.1 基于Table对象，使用API进行处理
 * 3.2 基于SQL的方式，直接写SQL
 * 4. 输出
 * 4.1 基于Table或连接器，输出结果
 * 4.2 表转流，基于流的方式输出
 */
public class Flink01_Api {

    public static void main(String[] args) throws Exception {
         //*1. 准备环境
         //       * 1.1 流表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
         //       * 1.2 表环境，从操作层面与流独立，底层处理还是流
        TableEnvironment tableEnvironment = TableEnvironment
                .create(EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build());

        //* 2. 创建表
        // * 2.1 将流转换成表
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");

                    return new WaterSensor(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                });
        Table table = tableEnv.fromDataStream(ds);
        // * 2.2 连接器表，直接对数据源读取数据
        //tableEnvironment.executeSql(建表语句)
        //tableEnv.executeSql(建表语句)


        // * 3. 转换处理
        // * 3.1 基于Table对象，使用API进行处理
        //table.select().where().join().groupBy()
        // * 3.2 基于SQL的方式，直接写SQL
        //tableEnvironment.sqlQuery();
        //  tableEnv.sqlQuery()


        // * 4. 输出
        // * 4.1 基于Table或连接器，输出结果
        //table.execute().print();
        //tableEnv.executeSql("insert into 连接器表  .. select")
        // * 4.2 表转流，基于流的方式输出
        //DataStream<Row> dataStream = tableEnv.toDataStream(table);
        //DataStream<Row> changelogStream = tableEnv.toChangelogStream(table);


        env.execute();
    }
}
