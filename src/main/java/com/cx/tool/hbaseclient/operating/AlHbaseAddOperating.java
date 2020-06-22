package com.cx.tool.hbaseclient.operating;

import com.alibaba.fastjson.JSON;
import com.cx.tool.hbaseclient.common.exception.ReflectionException;
import com.cx.tool.hbaseclient.common.kit.HbaseKit;
import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import com.cx.tool.hbaseclient.data.bo.QueryMatchParam;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    public static void insert(String tableName, Put put) throws IOException {
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
    public static void insertBatch(String tableName, List<Put> putList) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        table.put(putList);
        table.close();

    }

    /**
     * 插入单个实体
     * @param tableName 表名
     * @param t 对象
     * @param rowKey rowKey
     * @param family 列族名
     * @param noAttributesPutMap 不需要存储的属性
     * @param reflectionParentFalg 是否要反射父类属性
     * @param <T> 对象
     * @return
     * @throws ReflectionException 反射异常
     * @throws IOException IO 异常
     */
    public static <T> T insertClass(String tableName,T t,
                                    String rowKey,String family,
                                    HashMap<String,String> noAttributesPutMap,boolean reflectionParentFalg) throws ReflectionException, IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        Put put = new Put(rowKey.getBytes());
        HbaseKit.addColumn(family,t,put,noAttributesPutMap,reflectionParentFalg);
        table.put(put);
        table.close();
        return t;
    }

    /**
     * 插入多条数据
     * @param tableName 表名
     * @param t 插入实体
     * @param rowKey
     * @param family
     * @param noAttributesPutMap
     * @param reflectionParentFalg
     * @param <T>
     * @return
     * @throws IOException
     * @throws ReflectionException
     */
//    public static <T> List<T> insertBatchClass(String tableName, List<T> t,
//                                         String rowKey,String family,
//                                         HashMap<String,String> noAttributesPutMap,boolean reflectionParentFalg) throws IOException, ReflectionException {
//        Table table = AlHbaseConnection.getTable(tableName);
//        List<Put> putList = new ArrayList<>();
//        Put put = new Put(rowKey.getBytes());
//        HbaseKit.addArrayColumn(family,t,put,null);
//        table.put(putList);
//        table.close();
//        return t;
//
//    }

    /**
     * 删除行
     * @param tableName 表名
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(String tableName,String rowKey) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close();
    }

    /**
     * 删除多行
     * @param tableName 表名
     * @param rowKeyList
     * @throws IOException
     */
    public static void deleteRows(String tableName,List<String> rowKeyList) throws IOException {
        if(null != rowKeyList && !rowKeyList.isEmpty()){
            Table table = AlHbaseConnection.getTable(tableName);
            List<Delete> deleteList = new ArrayList<Delete>();
            rowKeyList.forEach(rowKey ->{
                deleteList.add(new Delete(rowKey.getBytes()));
            });
            table.delete(deleteList);
            table.close();
        }
    }
}
