package com.cx.tool.hbaseclient.common;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *  对象反射工具类
 * @author cx
 *
 */
public class ObjectDynamicCreator {
    /**
     * 返回由对象的属性为key,值为map的value的Map集合
     * 
     * @param obj
     *            Object
     * @return mapValue Map<String,String>
     * @throws Exception
     */
    public static Map<String, Object> getFieldVlaue(Object obj) throws Exception {
        Map<String, Object> mapValue = new LinkedHashMap<String, Object>();
        Class<?> cls = obj.getClass();
        Field[] fields = cls.getDeclaredFields();
        buildFieldVlaue(cls, fields, obj, mapValue);
        return mapValue;
    }
    
    /**
     * 返回由对象以及父类的属性为key,值为map的value的Map集合
     * 
     * @param obj
     *            Object
     * @return mapValue Map<String,String>
     * @throws Exception
     */
    public static Map<String, Object> getFieldVlaueAndSupper(Object obj) throws Exception {
        Map<String, Object> mapValue = new LinkedHashMap<String, Object>();
        Class<?> cls = obj.getClass();
        Field[] fields = cls.getDeclaredFields();
        buildFieldVlaue(cls, fields, obj, mapValue);
        Class<?> clas = cls.getSuperclass();
        fields = clas.getDeclaredFields();
        buildFieldVlaue(clas, fields, obj, mapValue);
        return mapValue;
    }
    
    /**
     * 组装反射数据
     * @param cls
     * @param fields
     * @param obj
     * @param mapValue
     * @throws Exception
     */
    private static void buildFieldVlaue(Class<?> cls, Field[] fields, Object obj, Map<String, Object> mapValue) throws Exception{
    	  for (Field field : fields) {
          	
          	if(Modifier.isStatic(field.getModifiers())&&Modifier.isFinal(field.getModifiers())){
          		
          		continue;
          	}
          	
              String name = field.getName();
              String strGet = "get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
              Method methodGet = cls.getDeclaredMethod(strGet);
              Object object = methodGet.invoke(obj);
              
              if(null != object && StringUtils.isNotBlank(object.toString())){
              	mapValue.put(name, object);          	
              }
              
              
          }
    }

    /**
     * 返回由Map的key对属性，value对应值组成的对应
     * 
     * @param map
     *            Map<String,String>
     * @param cls
     *            Class
     * @return obj Object
     * @throws Exception
     */
    public static Object setFieldValue(Map<String, String> map, Class<?> cls) throws Exception {
        Field[] fields = cls.getDeclaredFields();
        Object obj = cls.newInstance();
        for (Field field : fields) {
            Class<?> clsType = field.getType();
            String name = field.getName();
            String strSet = "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
            Method methodSet = cls.getDeclaredMethod(strSet, clsType);
            if (map.containsKey(name)) {
                Object objValue = typeConversion(clsType, map.get(name));
                methodSet.invoke(obj, objValue);
            }
        }
        return obj;
    }

    /**
     * 将Map里面的部分值通过反射设置到已有对象里去
     * 
     * @param obj
     *            Object
     * @param data
     *            Map<String,String>
     * @return obj Object
     * @throws Exception
     */
    public static Object setObjectFileValue(Object obj, Map<String, String> data) throws Exception {
        Class<?> cls = obj.getClass();
        Field[] fields = cls.getDeclaredFields();
        for (Field field : fields) {
            Class<?> clsType = field.getType();
            String name = field.getName();
            String strSet = "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
            Method methodSet = cls.getDeclaredMethod(strSet, clsType);
            if (data.containsKey(name)) {
                Object objValue = typeConversion(clsType, data.get(name));
                methodSet.invoke(obj, objValue);
            }
        }
        return obj;
    }

    /**
     * 把对象的值用Map对应装起来
     * 
     * @param map
     *            Map<String,String>
     * @param obj
     *            Object
     * @return 与对象属性对应的Map Map<String,String>
     */
    public static Map<String, String> compareMap(Map<String, String> map, Object obj) {
        Map<String, String> mapValue = new HashMap<String, String>();
        Field[] fields = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            if (map.containsKey(name)) {
                mapValue.put(name, map.get(name));
            }
        }
        return mapValue;
    }

    /**
     * 把临时对象的值复制到持久化对象上
     * 
     * @param oldObject
     *            Object 持久化对象
     * @param newObject
     *            Object 临时对象
     * @return 持久化对象
     * @throws Exception
     */
    public static Object mergedObject(Object oldObject, Object newObject) throws Exception {
        Class<?> cls = newObject.getClass();
        Field[] fields = cls.getDeclaredFields();
        for (Field field : fields) {
            Class<?> clsType = field.getType();
            String name = field.getName();
            String method = name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
            String strGet = "get" + method;
            Method methodGet = cls.getDeclaredMethod(strGet);
            Object object = methodGet.invoke(newObject);
            if (object != null) {
                String strSet = "set" + method;
                Method methodSet = cls.getDeclaredMethod(strSet, clsType);
                Object objValue = typeConversion(clsType, object.toString());
                methodSet.invoke(oldObject, objValue);
            }
        }
        return oldObject;
    }

    public static Object typeConversion(Class<?> cls, String str) {
        Object obj = null;
        String nameType = cls.getSimpleName();
        if ("Integer".equals(nameType)) {
            obj = Integer.valueOf(str);
        }
        if ("String".equals(nameType)) {
            obj = str;
        }
        if ("Float".equals(nameType)) {
            obj = Float.valueOf(str);
        }
        if ("Double".equals(nameType)) {
            obj = Double.valueOf(str);
        }

        if ("Boolean".equals(nameType)) {
            obj = Boolean.valueOf(str);
        }
        if ("Long".equals(nameType)) {
            obj = Long.valueOf(str);
        }

        if ("Short".equals(nameType)) {
            obj = Short.valueOf(str);
        }

        if ("Character".equals(nameType)) {
            obj = str.charAt(1);
        }
        
        

        return obj;
    }
    
}