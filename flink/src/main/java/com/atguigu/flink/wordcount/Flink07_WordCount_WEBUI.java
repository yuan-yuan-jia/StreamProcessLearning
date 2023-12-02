package com.atguigu.flink.wordcount;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Flink 流批一体
 * (DataSet) WordCount
 * Flink程序的编码套路
 * 1. 创建运行环境（批处理、流处理、表处理）
 * 2. 对接数据源，读取数据
 * 3. 对数据进行转换处理
 * 4. 将结果写出
 */
public class Flink07_WordCount_WEBUI {
    public static void main(String[] args) throws Exception {

        // 配置webui

        Configuration configuration = new Configuration();
        configuration.setString("rest.address","localhost");
        configuration.setInteger("rest.port",12345);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 8888);

        dataStreamSource.flatMap((String line, Collector< Tuple2<String,Integer> > out) ->{

            for (String s : line.split(" ")) {
                out.collect(Tuple2.of(s,1));
            }


            // 指定类型
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).
                keyBy((Tuple2<String,Integer> in) -> in.f0)
                .sum(1)
                .print();




        env.execute();

    }
}
