package com.cx.tool.hbaseclient.operating;

import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * 阿里 Hbase 增删
 * @Author cx
 * @Date 2019-09-10 17:26
 */
public class AlHbaseAddOperating {

    /**
     * 创建表
     * @param tableName 表名
     * @param family 列族
     * @throws IOException
     */
    public static void createTable(String tableName,String family) throws IOException {
        Connection connection = AlHbaseConnection.getConnection();
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
        descriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        admin.createTable(descriptorBuilder.build());
        admin.close();
    }

    /**
     * 删除表
     * @param tableName 表名
     * @throws IOException
     */
    public static void delTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Connection connection = AlHbaseConnection.getConnection();
        Admin admin = connection.getAdmin();
        admin.disableTable(name);
        admin.deleteTable(name);
        admin.close();
    }

    /**
     * 插入单条数据
     * @param tableName 表名
     * @param put 列族名
     * @throws IOException
     */
    public void insert(String tableName, Put put) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        table.put(put);
        table.close();
    }

    /**
     * 插入多条数据
     * @param tableName 表名
     * @param putList 列族名
     * @throws IOException
     */
    public void insertBatch(String tableName, List<Put> putList) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        table.put(putList);
        table.close();

    }
}
