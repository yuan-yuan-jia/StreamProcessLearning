package com.atguigu.flink.architecture;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 并发：多个线程需要在同一个cpu中调度，线程之间存在争抢
 * 并行：多个线程在不同的CPU中同时被调度，线程之间不存在争抢
 * 并行度：一个作业并行执行的程度（数量）
 *
 * 设置并行度：
 * 1. 在IDEA中，如果没有设置并行度，默认并行度为CPU的核心数
 * 2. 代码中设置并行度：env.setParallelism(1); 不推荐
 * 3. 提交作业到集群时指定并行度
 */
public class Flink01_Parallelism {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 设置全局并行度
        //env.setParallelism(1);
        env.socketTextStream("hadoop102",8888)
                        .map((t) -> t)
                                .returns(Types.STRING).name("map1")
                                   // 单独为算子设置并行度
                                        .map(t -> t).name("map2").setParallelism(2)
                                                .print();


        env.execute();
    }
}
