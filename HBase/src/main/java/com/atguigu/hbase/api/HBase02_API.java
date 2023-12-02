package com.atguigu.hbase.api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase02_API {
    private static Connection connection = HBase01_Connection.getConnection();

    /*
     * DDL
     * 创建表
     */
    public static void testCreateTable(String namespaceName,
                                       String tableName,
                                       String ... columnFs
                                       ) throws IOException {
        // 基本的校验工作
        if (namespaceName == null || namespaceName.trim().isEmpty()) {
            namespaceName = "default";
        }

        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("表名不能为空");
        }

        if (columnFs == null || columnFs.length == 0) {
            throw new RuntimeException("至少需要一个列族");
        }

        // 判断表是否存在
        Admin admin = connection.getAdmin();

        try {
            TableName tableNameObj = TableName.valueOf(namespaceName, tableName);
            boolean isTableExist = admin.tableExists(tableNameObj);
            if (isTableExist) {
                throw new RuntimeException(namespaceName + ":" + tableName + ",已经存在");
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String columnF : columnFs) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnF))
                        .build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
        }finally {
            IOUtils.closeQuietly(admin);
        }
    }


    public static void main(String[] args) throws IOException {
        //testCreateTable("default","t1","cf1","cf2");
        //InsertOrUpdateData("default","t1","101","cf1","name","张三");
        //InsertOrUpdateData("default","t1","102","cf1","age","10");
        //InsertOrUpdateData("default","t1","101","cf1","name","zz");
       // InsertOrUpdateData("default","t1","101","cf1","age","zz");
        //ScanTable("default","t1");
        GetResult("default","t1");
        //Delete("default","t1","101","cf1","age");
       // DeleteAll("default","t1","101");
        HBase01_Connection.closeConnection();
    }





    /*
     * DML
     * 写入/修改
     */
    public static void InsertOrUpdateData(
            String namespace,
            String tableName,
            String rk,
            String cf,
            String cl,
            String value
    ) throws IOException {

        // 获取table对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        Table table = connection.getTable(tableNameObj);

        Put put = new Put(Bytes.toBytes(rk));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    private static Table getTable(String namespace,String tableName) {
        // 获取table对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try {
            return connection.getTable(tableNameObj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     *DML
     * scan
     */
    public static void ScanTable(String namespace,String tableName) {
        Table table = getTable(namespace, tableName);
        try {
            ResultScanner resultScanner = table.getScanner(new Scan());
            for (Result result : resultScanner) {
                System.out.println(Bytes.toString(result.value()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     *DML
     * GET: 查询
     */
    public static void GetResult(String namespace,String tableName) {
        Table table = getTable(namespace, tableName);
        try {
            Result result = table.get(new Get(Bytes.toBytes("102")));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] familyArray = cell.getFamilyArray();
                String familyName = Bytes.toString(familyArray);
                byte[] qualifierArray = cell.getQualifierArray();
                String qualifierName = Bytes.toString(qualifierArray);
                byte[] value = cell.getValueArray();
                String valueString = Bytes.toString(value);
                Cell.Type type = cell.getType();
                System.out.println("family:" + familyName + ",qualifier:" + qualifierName + "," + valueString + ",type:" + type.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            IOUtils.closeQuietly(table);
        }
    }

    /*
     *DML
     * delete/deleteall
     */
    public static void Delete(String namespace,String tableName,String rowKey,String columnFamily,String column) {
        Table table = getTable(namespace, tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            IOUtils.closeQuietly(table);
        }
    }

    public static void DeleteAll(String namespace,String tableName,String rowKey) {
        Table table = getTable(namespace, tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }





}
