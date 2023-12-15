package com.atguigu.flink.sql;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinkSQl入门
 */
public class Flink02_SQLHellloWorld {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");

                    return new WaterSensor(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                });

        Table table = tableEnv.fromDataStream(ds);

        // 在环境中注册表
        tableEnv.createTemporaryView("t1",table);
        // 使用流注册表
        //tableEnv.createTemporaryView("t1",ds);
        //SQL
        String sql = "select id,vc,ts from t1 where vc >= 100";
        // 执行SQL
        //查询
        Table tableResult = tableEnv.sqlQuery(sql);
        tableResult.execute().print();

        // 增删改
        //tableEnv.executeSql()


        env.execute();


    }

}
