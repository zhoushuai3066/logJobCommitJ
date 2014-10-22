package com.busap.logJobCommitJ.job;

import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.busap.logJobCommitJ.table.Table;
import com.busap.logJobCommitJ.table.TableDependencies;
import com.busap.logJobCommitJ.table.TableLocation;
import com.busap.logJobCommitJ.utils.DateRangeUtils;
import com.busap.logJobCommitJ.utils.ParameterUtils;
import com.busap.logJobCommitJ.utils.SparkUtils;

public abstract class Job {
	
	protected static String year;
	
	protected static  String month;
	
	protected static String day;
	
	protected static String hour;
	
	protected static JavaSparkContext ctx;
	
	protected static JavaSQLContext sqlCtx ;
	
	protected static TableDependencies tDependencies;
	
	protected static Table tab;
	
	protected static TableLocation  tableLocation = null;
	
	protected static String tablename;
	
	protected static Class entityClass;
	
	
	protected static void setUp(Class clz){
		tDependencies = new TableDependencies();
		ctx = SparkUtils.getContext();
	    sqlCtx = new JavaSQLContext(ctx);
	    entityClass =clz;
	    tablename = entityClass.getSimpleName();
    	tab = new Table(entityClass);
		Date dt = DateRangeUtils.generateDate(year, month, day, hour);
		String begin = DateRangeUtils.getDate(dt);
		tab.loadHourPartition(tablename, begin);
		tableLocation = (TableLocation) tab.getHourPartition(tablename).get(0);
	}
	
	protected static void init(String[] args){
		parseHourArgs(args);
	}
	
	
	protected static void parseHourArgs(String[] args){
		 year = ParameterUtils.getYear(args);
		 month = ParameterUtils.getMonth(args);
		 day = ParameterUtils.getDay(args);
		 hour = ParameterUtils.getHour(args);
	}
	
	
	
	public static void addHourTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	public static void addDayTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	
	public static void addMonthTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	/**
	 * 
	 * @param key
	 * @param table
	 */
	public static void addTableDependency(String key,Table table){
		Date beginDate = DateRangeUtils.generateDate(year, month, day, hour);
		String begin = DateRangeUtils.getDate(beginDate);
		tDependencies.addHourDependencies(key, table, begin);
	}
	
	
	public static JavaRDD<String> getTableDependencyTxt(String key){
		List<TableLocation> ls = tDependencies.getHour(key);
		if(ls.size()>0){
			TableLocation tl = ls.get(0);
			String txtlocation = tl.getTxt();
			JavaRDD<String> sourceRDD = ctx.textFile(txtlocation);
			return sourceRDD;
		}else{
			return null;
		}
	}
	
	public static JavaSchemaRDD getTableDependencyParquet(String key){
		List<TableLocation> ls = tDependencies.getHour(key);
		if(ls.size()>0){
			TableLocation tl = ls.get(0);
			String parquetlocation = tl.getParquet();
			JavaSchemaRDD sourceRDD = sqlCtx.parquetFile(parquetlocation);
			return sourceRDD;
		}else{
			return null;
		}
	}
	
	
	public static JavaSchemaRDD getTableDependencyJSON(String key){
		List<TableLocation> ls = tDependencies.getHour(key);
		if(ls.size()>0){
			TableLocation tl = ls.get(0);
			String jsonparquetlocation = tl.getJson();
			JavaSchemaRDD sourceRDD = sqlCtx.parquetFile(jsonparquetlocation);
			return sourceRDD;
		}else{
			return null;
		}
	}

}
