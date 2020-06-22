package com.cx.tool.hbaseclient.common.exception;

/**
 * 反射异常类
 * @author cx
 *
 */
public class ReflectionException extends Exception{

	
	public ReflectionException(){
		super();
	}
	
	public ReflectionException(String msg){
		super(msg);
	}
	
	public ReflectionException(String msg, Throwable cause){
		super(msg,cause);
	}
}
