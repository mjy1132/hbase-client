package com.cx.tool.hbaseclient.operating;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cx.tool.hbaseclient.connection.AlHbaseConnection;
import com.cx.tool.hbaseclient.data.bo.QueryMatchParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 阿里 hbase 查询
 *
 * @Author cx
 * @Date 2019-09-10 17:21
 */
public class AlHbaseQueryOperating {


    /**
     * 查询某一列数据
     *
     * @param tableName 表名
     * @param family    列族名
     * @param qualifier 列名
     * @param rowKey    rowKey
     * @return
     * @throws IOException
     */
    public static String selectByQualifierData(String tableName, String family, String qualifier, String rowKey) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(), qualifier.getBytes());
        Result result = table.get(get);
        String resultSt = null;
        if(result.rawCells().length > 0 ){
            resultSt = new String(result.getValue(family.getBytes(), qualifier.getBytes()), "UTF-8");
        }
        table.close();
        return resultSt;
    }

    /**
     * 读取rowKey 所有列数据
     *
     * @param tableName 表名
     * @param rowKey
     * @param family    列族名
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
        if(cells.length > 0 ){
            for (Cell cell : cells) {
                String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                if (StringUtils.isNotBlank(value) && !value.equals("null")) {
                    jsonObject.put(qualifier, value);
                }
            }
        }

        table.close();
        return jsonObject;
    }

    /**
     * 表名列族名列名查询所有的数据
     * @param tableName 表名
     * @param family 列族名
     * @param qualifier 列名
     * @return List<String>
     * @throws IOException
     */
    public static List<String> selectByTableNameDataAll(String tableName,String family,String qualifier) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        ResultScanner resultScanner = table.getScanner(family.getBytes(),qualifier.getBytes());
        List<String> returnResult = new ArrayList<String>();
        for (Result result: resultScanner){
            if(result.rawCells().length > 0){
                returnResult.add(new String(result.getValue(family.getBytes(), qualifier.getBytes()), "UTF-8"));
            }
        }
        table.close();
        return returnResult;
    }

    /**
     * 表名列族名查询所有的数据
     * @param tableName 表名
     * @param family 列族名
     * @return List<JSONObject> key :列名 val:列对应的值
     * @throws IOException
     */
    public static List<JSONObject> selectByTableNameDataAll(String tableName,String family) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        ResultScanner resultScanner = table.getScanner(family.getBytes());
        List<JSONObject> returnResult = new ArrayList<JSONObject>();
        for (Result result: resultScanner){
            Cell[] cells = result.rawCells();
            if(cells.length > 0){
                JSONObject jsonObject = new JSONObject();
                for (Cell cell : cells) {
                    String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                    String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                    if (StringUtils.isNotBlank(value) && !value.equals("null")) {
                        jsonObject.put(qualifier, value);
                    }
                }
                returnResult.add(jsonObject);
            }
        }
        table.close();
        return returnResult;
    }

    /**
     * 表名列族名列名查询所有的数据
     * @param tableName 表名
     * @param family 列族名
     * @param qualifier 列名
     * @return List<String>
     * @throws IOException
     */
    public static List<String> testSelectByTableNameDataAll(String tableName,String family,String qualifier) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        ResultScanner resultScanner = table.getScanner(family.getBytes(),qualifier.getBytes());
        List<String> returnResult = new ArrayList<String>();
        for (Result result: resultScanner){
            if(result.rawCells().length > 0){
                returnResult.add(new String(result.getValue(family.getBytes(), qualifier.getBytes()), "UTF-8"));
            }
        }
        table.close();
        return returnResult;
    }

    /**
     * 根据rowkey查询单条数据
     * @param tableName 表名
     * @param family 列族名
     * @param rowKey
     * @param clazz 返回类型
     * @return 返回传进来的类对象
     * @throws IOException
     */
    public static <T> T queryByRowKeyEntity(String tableName,String family,
                                            String rowKey,Class<T> clazz) throws IOException {
        JSONObject jsonObject = selectData(tableName,rowKey,family);
        return jsonObject.toJavaObject(clazz);
    }

    /**
     * 根据列值查询
     * @param tableName 表名
     * @param family 列族名
     * @param clazz 返回对象
     * @param queryMatchParamList 需要匹配的列值
     * @param operator and or
     * @param <T> 返回对象 null null 为未查询到数据
     * @return
     * @throws IOException
     */
    public static <T> List<T> queryByQualifierEntityList(String tableName, String family,
                                                         Class<T> clazz, List<QueryMatchParam> queryMatchParamList,
                                                         FilterList.Operator operator) throws IOException {
        Table table = AlHbaseConnection.getTable(tableName);
        Scan scan = new Scan();
        FilterList filterList = new FilterList(operator);
        for (QueryMatchParam queryMatchParam : queryMatchParamList){
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family.getBytes(),queryMatchParam.getQualifier(), queryMatchParam.getOp(),queryMatchParam.getValue());
            filterList.addFilter(singleColumnValueFilter);
        }
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        JSONArray jsonArray = scanData(resultScanner);
        if(jsonArray.size() > 0){
            return jsonArray.toJavaList(clazz);
        }
        return null;
    }



    private static JSONArray scanData(ResultScanner resultScanner) throws UnsupportedEncodingException {
        JSONArray jsonArray = new JSONArray();
        for (Result result: resultScanner){
            Cell[] cells = result.rawCells();
            JSONObject jsonObject = new JSONObject();
            boolean jsonObjectIsNullFlag = false;
            for (Cell cell : cells) {
                String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                if (StringUtils.isNotBlank(value) && !value.equals("null")) {
                    jsonObject.put(qualifier, value);
                    jsonObjectIsNullFlag = true;
                }
            }
            if(jsonObjectIsNullFlag){
                jsonArray.add(jsonObject);
            }

        }
        return jsonArray;
    }

}
