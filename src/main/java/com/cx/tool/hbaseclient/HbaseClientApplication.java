package com.cx.tool.hbaseclient;


import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import com.cx.tool.hbaseclient.operating.AlHbaseAddOperating;
import com.cx.tool.hbaseclient.operating.AlHbaseQueryOperating;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


public class HbaseClientApplication {

    public static void main(String[] args) throws IOException {
        String tableName = "photo";
        String family = "test";
        String rowKey = "c5c00ad6f4ee3e729dedc11d4042db6d";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM,"hb-proxy-pub-bp14m3b9dkuimkd34-master2-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master1-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master3-001.hbase.rds.aliyuncs.com:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();
//        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
//        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
//        descriptorBuilder.setColumnFamily(columnFamilyDescriptor);
//        admin.createTable(descriptorBuilder.build());
//        Table table = connection.getTable(TableName.valueOf(tableName));
//        Put put = new Put(rowKey.getBytes());
//        put.addColumn(family.getBytes(),"cx".getBytes(),"cx".getBytes());
//        table.put(put);
//        table.close();
//        admin.disableTable(TableName.valueOf(tableName));
//        admin.deleteTable(TableName.valueOf(tableName));
        AlHbaseConnection.createConnection("hb-proxy-pub-bp14m3b9dkuimkd34-master2-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master1-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master3-001.hbase.rds.aliyuncs.com:2181");
//        Put put = new Put("c5c00ad6f4ee3e729dedc11d4042db6d".getBytes());
//        put.addColumn("test".getBytes(),"cx".getBytes(),"cx".getBytes());
//        AlHbaseAddOperating.insert("test",put);
        String  phto = AlHbaseQueryOperating.selectByQualifierData("test", "test", "cx", "c5c00ad6f4ee3e729dedc11d404");
        System.out.println(phto);
    }

}
