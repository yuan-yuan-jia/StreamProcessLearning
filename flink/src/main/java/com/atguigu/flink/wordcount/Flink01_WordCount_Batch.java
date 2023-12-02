package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理
 * (DataSet) WordCount
 * Flink程序的编码套路
 * 1. 创建运行环境（批处理、流处理、表处理）
 * 2. 对接数据源，读取数据
 * 3. 对数据进行转换处理
 * 4. 将结果写出
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //将每行单词数据切分成单词，并且处理成（word,1）的格式

        FlatMapOperator<String, Tuple2<String, Integer>> flatMapDs = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });


        // 按照单词分组
        UnsortedGrouping<Tuple2<String, Integer>> groupDs = flatMapDs.groupBy(0);

        // 汇总每个单词出现的次数
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = groupDs.sum(1);
        // 将结果写出
        aggregateOperator.print();

    }
}
