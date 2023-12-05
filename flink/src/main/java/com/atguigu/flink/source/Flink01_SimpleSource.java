package com.atguigu.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Flink提供的source
 * 1. 旧source  addSource(SourceFunction)
 * 2. 新的算子   fromSource(Source)
 *
 * Kafka Connector  ->   Source(重点掌握)
 * FileSystem Connector -> Source
 *  DataGen Source(用于测试)
 *
 */
public class Flink01_SimpleSource {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 简单Source，一般都有预定义方法，不需要addSource,fromSource
        // 从集合中读取数据
        ArrayList<String> strings = new ArrayList<>();

        strings.add("1");
        strings.add("2");
        strings.add("3");
        strings.add("4");
        strings.add("5");
        DataStreamSource<String> dataStreamSource1 = env.fromCollection(strings);
        dataStreamSource1.print();
        DataStreamSource<String> dataStreamSource2 = env.fromElements("a", "b", "c", "d", "e");
        dataStreamSource2.print();

        // 从端口读数据
        DataStreamSource<String> dataStreamSource3 = env.socketTextStream("hadoop102", 8888);
        dataStreamSource3.print();

        // 从文件读数据
        DataStreamSource<String> dataStreamSource4 = env.readTextFile("input/word.txt");
        dataStreamSource4.print();




        env.execute();
    }

}
