package com.atguigu.flink.architecture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TaskSlots: 任务槽,TaskManager提供用于执行Task的资源
 * (CPU + 内存)
 * TaskManger的TaskSlot的个数: 主要由TaskManager所在机器的CPU的核心数
 * 来决定，不能超过核心数,如果是yarn，得看yarn中对container的资源配置，
 * 不能超过container最大资源。
 * <property>
 * <name>yarn.scheduler.maximum-allocation-vcores</name>
 * <value>4</value>
 * <final>false</final>
 * <source>yarn-default.xml</source>
 * </property>
 *
 * 一个作业的task数如何确定？
 * 主要由算子数、算子链数、并行度共同来决定的
 *   如果禁用算子链合并： task的数量 = 算子数 * 并行度
 *   如果存在算子链合并:  task的数量 = 合并后的算子链数（包含不合并的算子） * 并行度
 * Slot共享：
 *  Flink允许将上下游Task共享到同一个Slot，同一个Task的并行子任务不能共享同一个Slot
 *  一个作业的并行度如何确定？
 *  作业的并行度，由当前作业中的并行度最大的算子的并行度决定。
 * 一个作业需要多少个taskSlot如何确定？
 * 由作业的并行度决定（前提是slot共享）。
 *
 * 为什么要共享Slot？
 * 1. 当我们将资源密集型和非密集型的任务同时放到一个slot中,它们就可以自行分配对资源占用的比例,从而保证最重的活平均分配给所有的TaskMonager。
 * 2. Slot共享另一个好处就是允许我们保存完整的作业管道
 *能不能不共享？
 *  通过设置共享组来实现共享或者不共享，默认的共享组为default，从source往后传递，
 *  如果下游的算子没有设置共享组，默认应用前面的共享组。
 *  通过算子().slotSharingGroup("共享组名称")
 *  map(s -> s).name("map2").slotSharingGroup("g1")
 */
public class Flink03_TaskSlots {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        env.socketTextStream("hadoop102",8888)
                        .map(str -> str).name("map1").disableChaining()
                        // 设置新的共享组
                        .map(s -> s).name("map2").slotSharingGroup("g1")
                        .print();


        env.execute();
    }
}
