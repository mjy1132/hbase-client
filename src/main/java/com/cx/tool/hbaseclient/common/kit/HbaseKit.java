package com.cx.tool.hbaseclient.common.kit;

import com.alibaba.fastjson.JSON;
import com.cx.tool.hbaseclient.common.ObjectDynamicCreator;
import com.cx.tool.hbaseclient.common.exception.ReflectionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * hbase 反射工具类
 *
 * @Author cx
 * @Date 2020-06-19
 */
public class HbaseKit {

    /**
     * 插入数据
     *
     * @param columnFamily 列族
     * @param obj          对象
     * @param put
     * @param attributes   不put的数据
     * @param falg         是否要反射父类的数据 true:是;false:否
     * @throws ReflectionException
     */
    public static void addColumn(String columnFamily, Object obj, Put put, Map<String, String> attributes, boolean falg) throws ReflectionException {

        Map<String, Object> map;
        try {
            if (falg) {
                map = ObjectDynamicCreator.getFieldVlaueAndSupper(obj);
            } else {
                map = ObjectDynamicCreator.getFieldVlaue(obj);
            }
        } catch (Exception e) {
            throw new ReflectionException("反射出现异常", e);
        }


        for (Map.Entry<String, Object> entry : map.entrySet()) {
            boolean putFlag = true;
            if(null != attributes){
                if (attributes.containsKey(entry.getKey())) {
                    putFlag = false;
                }
            }
            if(putFlag){
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }

        }

    }

    /**
     * 将LIst 转成json格式的数据插入hbase
     *
     * @param columnFamily 列族
     * @param obj          对象
     * @param put
     * @param attributes   不put的数据
     * @throws ReflectionException
     */
    public static void addArrayColumn(String columnFamily, Object obj, Put put, Map<String, String> attributes) throws ReflectionException {

        Map<String, Object> map;
        try {
            map = ObjectDynamicCreator.getFieldVlaue(obj);

        } catch (Exception e) {
            throw new ReflectionException(e.getMessage(), e);
        }


        for (Map.Entry<String, Object> entry : map.entrySet()) {
            boolean putFlag = true;
            if(null != attributes){
                if (attributes.containsKey(entry.getKey())) {
                    putFlag = false;
                }
            }
            if(putFlag){
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), JSON.toJSONBytes(entry.getValue()));
            }

        }

    }
}
