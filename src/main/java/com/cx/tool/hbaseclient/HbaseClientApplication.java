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

        AlHbaseConnection.createConnection("hb-proxy-pub-bp14m3b9dkuimkd34-master2-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master1-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master3-001.hbase.rds.aliyuncs.com:2181");
        String  phto = AlHbaseQueryOperating.selectByQualifierData("merchantphoto", "merchantphotofamily", "photo", "98001");
        System.out.println(phto);
    }

}
