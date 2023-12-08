package com.atguigu.flink.sink;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Jdbc Connector -> JdbcSink
 *  去重,存在相同主键的数据,只更新特定的字段
 *    CREATE TABLE `url_count` (
 *   `url` VARCHAR(100) NOT NULL primary key,
 *   `cnt` INT DEFAULT NULL
 * ) ENGINE=INNODB DEFAULT CHARSET=utf8
 */
public class Flink06_JBDCSinkReplaceForSpecField {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        // wordCount
        SingleOutputStreamOperator<WordCount> countDs = ds.map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(WordCount::getWord)
                .sum("count");


        //jdbcSink
        SinkFunction<WordCount> jdbcSink = JdbcSink.<WordCount>sink(
                // 相同主键的情况下，只更新cnt字段
                "insert into url_count(url,cnt) values(?,?) on duplicate key update cnt=values(cnt)",
                new JdbcStatementBuilder<WordCount>() {

                    @Override
                    public void accept(PreparedStatement preparedStatement, WordCount wordCount) throws SQLException {
                        preparedStatement.setString(1, wordCount.getWord());
                        preparedStatement.setInt(2,wordCount.getCount());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(20)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .withConnectionCheckTimeoutSeconds(300)
                        .build()
        );


        countDs.addSink(jdbcSink);


        env.execute();
    }
}
