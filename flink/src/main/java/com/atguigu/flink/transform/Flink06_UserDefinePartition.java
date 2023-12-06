package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_UserDefinePartition {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);


        // zhangsan和lisi发往一个分区，其他另外的分区
        ds.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if ("zhangsan".equals(key) ||
                    "lisi".equals(key)
                ) {
                    return 0;
                }
                return 1;
            }
        }, Event::getUser).print();


        env.execute();
    }

}
