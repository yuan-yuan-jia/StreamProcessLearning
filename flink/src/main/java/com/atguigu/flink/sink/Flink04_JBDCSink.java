package com.atguigu.flink.sink;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Jdbc Connector -> JdbcSink
 *
 */
public class Flink04_JBDCSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);


        //jdbcSink
        SinkFunction<Event> jdbcSink = JdbcSink.<Event>sink(
                "insert into clicks(user,url,ts) values(?,?,?)",
                new JdbcStatementBuilder<Event>() {

                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTs());
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


        ds.addSink(jdbcSink);


        env.execute();
    }
}
