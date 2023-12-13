package com.atguigu.flink.checkpoint;


import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Flink01_CheckPoint {

    public static void main(String[] args) throws Exception {

        // 设置Hadoop的使用用户
        System.setProperty("HADOOP_USE_NAME","atguigu");
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",12345);

        //最终检查点
        configuration.setBoolean(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 开启检查点
        // 每5秒保存一次，保存语义是精确一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 指定一致性语义
        //checkpointConfig.setCheckpointingMode();

        //检查点存储
        //1 JobManagerCheckPointStorage
        //将检查点存到JobManager内存中
        //2 FileSystemCheckPointStorage
        //将检查点存到指定的文件系统中
        //checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink-ck"));

        // 状态后端
       // true 表示启动catalog
        // env.setStateBackend(new EmbeddedRocksDBStateBackend(true))

        // 检查点超时时间
        //10秒超时
        //checkpointConfig.setCheckpointTimeout(10L * 1000);

        // 同时存在检查点的个数
        // 同时存在两个检查点
        //checkpointConfig.setMaxConcurrentCheckpoints(2);

        // 两次检查点之间的间隔
        // 完成之后多久开启下一次检查点
       // checkpointConfig.setMinPauseBetweenCheckpoints(1000);


        // 检查点清理
        // 作业取消后，检查点的保留策略
        //checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);


        // 检查点允许的失败次数
        //checkpointConfig.setTolerableCheckpointFailureNumber(2);

        // 开启非对齐检查点
        // 非对齐检查点同时的检查点个数只能是1
        //checkpointConfig.enableUnalignedCheckpoints();


        //对齐超时后，自动开启非对齐
        //checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(5));






        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print();


        env.execute();
    }

}
