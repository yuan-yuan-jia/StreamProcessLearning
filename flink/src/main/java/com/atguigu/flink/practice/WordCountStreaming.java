package com.atguigu.flink.practice;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 1. 一句话解释Flink
 * 流批处理一体的框架
 * 2. Flink集群的角色有哪些，分别解释
 * JobManager: 管理集群资源和任务调度
 *    JobMaster：负责监控一个Job以及生成JobGraph
 *    ResourceManager: 资源管理
 *    Dispatcher: WebUI的启动以及任务的派发
 * TaskManager: 具体执行任务的节点
 * 3. Flink的三种部署模式及区别
 * (1) 会话模式： 一个集群可以多次提交以及运行多个作业
 * (2) 单作业模式：提交任务时，生成集群。任务结束，集群退出，在客户端提交任务的依赖
 * (3)  应用模式： 提交任务时，生成集群。任务结束，集群退出，在JobManager提交任务的依赖
 * 4. 简述并行度的概念及设置方式
 * 并行度：一个算子在运行时，可以多个算子实例并行运行
 * (1) 全局设置
 * (2) 单个算子设置
 * (3) 提交任务时，在命令行中指定
 * (4) 集群的默认设置
 * 5. 简述算子链的概念、合并算子链的条件、如何禁用算子链
 * 算子链是算子之间数据传递的上下游关系
 * 合并算子链的条件
 * 1. 上下游算子的并行度要相同
 * 2. 算子直接的通道选择器是ForwardSelector
 * 6. 编程题：基于流式处理，读取文本数据，完成wordcount
 */
public class WordCountStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.readTextFile("input/word.txt")
                        .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                                for (String s : line.split(" ")) {
                                    out.collect(Tuple2.of(s,1));
                                }
                            }
                        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).sum(1)
                        .print();
        env.execute();
    }

}
