package com.cx.tool.hbaseclient;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import com.cx.tool.hbaseclient.operating.AlHbaseAddOperating;
import com.cx.tool.hbaseclient.operating.AlHbaseQueryOperating;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;


public class HbaseClientApplication {

    public static void main(String[] args) throws IOException {

        AlHbaseConnection.createConnection("hb-proxy-pub-bp14m3b9dkuimkd34-master2-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master1-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp14m3b9dkuimkd34-master3-001.hbase.rds.aliyuncs.com:2181");
        List<JSONObject> resultList = AlHbaseQueryOperating.selectByTableNameDataAll("userpaycommon", "userpaycommonfamily");
        for (JSONObject st : resultList){
            System.out.println(st);
        }
    }

}
