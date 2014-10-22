package com.busap.logJobCommitJ.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

public class SparkUtils {
	
	
	
	public static SparkConf getConf(){
		return new SparkConf();
	}
	
	public static JavaSparkContext getContext(SparkConf conf){
		return new JavaSparkContext(conf);
	}
	
	public static JavaSparkContext getContext(){
		SparkConf conf = SparkUtils.getConf();
		return new JavaSparkContext(conf);
	}
	
	
	public static JavaSQLContext getSQLContext(JavaSparkContext context){
		return new JavaSQLContext(context);
	}
	
	
	public static JavaSQLContext getSQLContext(){
		SparkConf conf = SparkUtils.getConf();
		JavaSparkContext context = SparkUtils.getContext(conf);
		return new JavaSQLContext(context);
	}

	
	public static void main(String[] args){
		
		String str = "aaaaaaaaaaaaaa|bbbbbbbbbbbbbb|ccccccccccc|||ddddddddddd";
		String[] sarg = str.split("\\|");
		System.out.println(sarg[0]);
		System.out.println(sarg[1]);
		System.out.println(sarg[2]);
		System.out.println(sarg[3]);
		System.out.println(sarg[4]);
		System.out.println(sarg[5]);
	}
}

