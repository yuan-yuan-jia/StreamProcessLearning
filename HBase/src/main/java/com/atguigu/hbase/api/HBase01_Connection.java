package com.atguigu.hbase.api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBase01_Connection {

    private static Connection connection = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,2);

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(connection);
    }

    public static Connection getConnection() {
        return connection;
    }
    public static void closeConnection() {
        IOUtils.closeQuietly(connection);
    }


    public static void main(String[] args) throws IOException {

        Connection connection = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102");


        try {
            connection = ConnectionFactory.createConnection();
            System.out.println(connection);
        } finally {
            IOUtils.closeQuietly(connection);
        }

    }

}
