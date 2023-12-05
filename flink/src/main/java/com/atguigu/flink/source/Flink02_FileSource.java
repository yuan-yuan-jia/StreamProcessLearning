package com.atguigu.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FileSystem Connector -> File Source
 */
public class Flink02_FileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //FileSource
        FileSource.FileSourceBuilder<String> fileSourceBuilder = FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt"));
        FileSource<String> fileSource = fileSourceBuilder.build();

        DataStreamSource<String> dataStreamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");

        dataStreamSource.print();
        env.execute();
    }
}
