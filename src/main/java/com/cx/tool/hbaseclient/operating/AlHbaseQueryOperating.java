package com.cx.tool.hbaseclient.operating;

import com.alibaba.fastjson.JSONObject;
import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 阿里 hbase 查询
 * @Author cx
 * @Date 2019-09-10 17:21
 */
public class AlHbaseQueryOperating {


    /**
     * 查询某一列数据
     * @param tableName 表名
     * @param family 列族名
     * @param qualifier 列名
     * @param rowKey rowKey
     * @return
     * @throws IOException
     */
    public static String selectByQualifierData(String tableName,String family,String qualifier ,String rowKey) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(),qualifier.getBytes());
        Result result = table.get(get);
        String resultSt = new String(result.getValue(family.getBytes(),qualifier.getBytes()),"UTF-8");
        table.close();
        return resultSt;
    }

    /**
     * 读取rowKey 所有列数据
     * @param tableName 表名
     * @param rowKey
     * @param family 列族名
     * @return
     * @throws IOException
     */
    public static JSONObject selectData(String tableName, String rowKey, String family) throws IOException {
        JSONObject jsonObject = new JSONObject();
        Table table = AlHbaseConnection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        get.addFamily(family.getBytes());
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells){
            String qualifier = new String(CellUtil.cloneQualifier(cell),"UTF-8");
            String value = new String(CellUtil.cloneValue(cell),"UTF-8");
            if(StringUtils.isNotBlank(value) && !value.equals("null")){
                jsonObject.put(qualifier,value);
            }
        }
        table.close();
        return jsonObject;
    }
}
