package com.cx.tool.hbaseclient.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 *  阿里 Hbase 创建连接
 *  获取连接
 *  获取table
 *  这里不希望使用者自行获得连接做增删查
 * @Author cx
 * @Date 2019-09-10 16:46
 */
public class AlHbaseConnection {

    private static String zkAddres;
    private static Connection connection;

    /**
     * 创建zk连接
     * 创建一次之后无需在创建
     * @param zkAddRes zk 地址
     * @return
     * @throws IOException
     */
    public static Connection createConnection(String zkAddRes) throws IOException {
        if(null == connection){
            zkAddres = zkAddRes;
            Configuration configuration = HBaseConfiguration.create();
            configuration.set(HConstants.ZOOKEEPER_QUORUM,zkAddRes);
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    /**
     * 得到ZK 连接
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        if(null == connection){
            return createConnection(zkAddres);
        }
        return connection;
    }

    /**
     * 获取Table
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return  connection.getTable(TableName.valueOf(tableName));
    }
}
