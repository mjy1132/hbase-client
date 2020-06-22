package com.cx.tool.hbaseclient.data.bo;

import org.apache.hadoop.hbase.CompareOperator;

/**
 * @Author cx
 * @Date 2020-06-22
 */
public class QueryMatchParam {

    /**
     * 列名
     */
    private byte [] qualifier;

    /**
     *  运算符
     */
    private CompareOperator op;

    /**
     * 值
     */
    private byte[] value;


    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public CompareOperator getOp() {
        return op;
    }

    public void setOp(CompareOperator op) {
        this.op = op;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
