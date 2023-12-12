package com.atguigu.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态后端
 * 1. 本地状态管理
 * 2. 持久化状态，基于检查点机制，将本地状态存到持久化存储设备中
 * 分类 :
 *   HashmapStateBackend
 *   将状态存到TaskManager的内存中，读写效率高，不支持大状态
 *   EmbeddedRocksDBBackend
 *   一种kv的存储介质，将状态存储到TaskManager的磁盘中，读写效率
 *   比HashMap低，支持超大状态
 *
 *   // 代码中设置使用状态后端
 *         env.setStateBackend(new EmbeddedRocksDBStateBackend());
 *
 *
 *         // 获取当前正在使用的状态后端
 *         //null: 默认使用HashmapStateBackend
 *         System.out.println(env.getStateBackend())
 */
public class Flink11_StateBackend {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 代码中设置使用状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend());


        // 获取当前正在使用的状态后端
        //null: 默认使用HashmapStateBackend
        System.out.println(env.getStateBackend());
        env.socketTextStream("hadoop102",8888)
                        .print();

        env.execute();
    }
}
