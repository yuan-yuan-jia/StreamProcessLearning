package com.atguigu.flink.sql;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Table Api 入门
 * 编码步骤
 * 1. 准备表环境
 * 2. 创建表
 * 3. 对表进行查询
 * 4. 输出结果
 */
public class Flink01_TableApi_HelloWorld {

    public static void main(String[] args) throws Exception {
        //1. 准备表环境
        // 基于流环境创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建表
        // 流转表
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");

                    return new WaterSensor(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                });

        Table table = tableEnv.fromDataStream(ds);
        // 打印表结构
        // Flink会通过流中的数据类型来确定表的结构，
        //会用POJO的属性名作为表的列名，POJO的属性类型作为列的类型
        // 也可指定表的结构
        table.printSchema();

        //3.对表进行查询
        table.select(Expressions.$("id"),
                Expressions.$("vc"),
                Expressions.$("ts")
        ).where(Expressions.$("vc")
                .isGreaterOrEqual(100))
                .execute().print();

        env.execute();


    }


}
