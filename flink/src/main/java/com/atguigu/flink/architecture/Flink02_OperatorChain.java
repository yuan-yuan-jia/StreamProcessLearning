package com.atguigu.flink.architecture;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 上下游数据分发规则（数据分发规则）:
 * ChannelSelector
 * RebalancePartitioner : 绝对负载均衡，按照轮询的方式
 *           如果上下游并行度不一样，默认是rebalance
 * RescalePartitioner :   相对负载均衡,按照轮询的方式，发送到下游一个特定组的subtasks中，特定组中subtask轮询接收
 * ShufflePartitioner : 按照随机方式将数据发送到下游的并行度中
 * BroadcastPartitioner :  广播，每个数据都给下游Task的每个并行度发一份
 * GlobalPartitioner : 所有的数据只会发送到下游Task中的第一个并行度中，相当于强制并行度为1
 * KeyGroupStreamPartitioner :  keyBy，按照key的hash值决定发往下游task中的哪个并行度，本质是Hash
 * ForwardPartitioner :  直连，上下游并行度一致，才能使用直连
 *                       如果上下游并行度一致，没有设置分区规则，默认是forward
 * 自定义分区:
 * 算子链: 将上下游的task合并成一个大的task，形成算子链
 *合并算子链的条件：
 * 1. 上下游并行度必须一样
 * 2. 数据的分发规则必须是forward
 *
 * 为什么要合并算子链？
 * 1. 减少线程间切换，缓冲开销，并且减少延迟的同时增加整体吞吐量
 * * 能不能不合并? 能。
 * 1. 全局禁用算子链合并。
 * 2. 针对算子设置
 * .startNewChain(): 开启新链。
 * .disableChaining(: 当前算子不参与算子链合并
 *
 * env.disableOperatorChaining() ;
 */
public class Flink02_OperatorChain {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.socketTextStream("hadoop102",8888)
                .map(str ->str).name("map1").disableChaining()
               // .map(str ->str).name("map1").startNewChain()
                // 使用Rebalance通道选择器
                //.rebalance()
                // 使用Rescale通道选择器
                // .rescale()
                // 使用Shuffle通道选择器
                //.shuffle()
                // 使用Broadcast通道选择器
                //.broadcast()
                //使用Global通道选择器
                //.global()
                // 使用KeyBy通道选择器
                .keyBy(str -> str)
                // forward通道选择器
                //.forward()
                .map(str -> str).name("map2").setParallelism(2)

                .print();


        env.execute();
    }
}
