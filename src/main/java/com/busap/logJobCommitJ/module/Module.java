package com.busap.logJobCommitJ.module;

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

public abstract class Module<T> {
	
	protected  String year;
	
	protected  String month;
	
	protected  String day;
	
	protected  String hour;
	
	protected JavaSparkContext ctx;
	
	protected JavaSQLContext sqlCtx ;
	
	protected TableDependencies tDependencies;
	
	protected Table tab;
	
	TableLocation tableLocation = null;
	
	protected String module;
	
	protected Class<T> entityClass;
	
	
	
	public Module(){
		tDependencies = new TableDependencies();
		ctx = SparkUtils.getContext();
	    sqlCtx = new JavaSQLContext(ctx);
	    entityClass =(Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	    module = entityClass.getSimpleName();
	    	tab = new Table(entityClass);
		Date dt = DateRangeUtils.generateDate(year, month, day, hour);
		String begin = DateRangeUtils.getDate(dt);
		tab.loadHourPartition(module, begin);
		tableLocation = tab.getHourPartition(module).get(0);
	}
	
	
	public void setUp(){
		
	}
	
	protected void init(String[] args){
		parseHourArgs(args);
	}
	
	
	protected void parseHourArgs(String[] args){
		 year = ParameterUtils.getYear(args);
		 month = ParameterUtils.getMonth(args);
		 day = ParameterUtils.getDay(args);
		 hour = ParameterUtils.getHour(args);
	}
	
	
	
	public void addHourTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	public void addDayTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	
	public void addMonthTableDependencies(String key,Table table,String begin,String end){
		tDependencies.addHourDependencies(key, table, begin, end);
	}
	
	/**
	 * 
	 * @param key
	 * @param table
	 */
	public void addTableDependency(String key,Table table){
		Date beginDate = DateRangeUtils.generateDate(year, month, day, hour);
		String begin = DateRangeUtils.getDate(beginDate);
		tDependencies.addHourDependencies(key, table, begin);
	}
	
	
	public JavaRDD<String> getTableDependencyTxt(String key){
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
	
	public JavaSchemaRDD getTableDependencyParquet(String key){
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
	
	
	public JavaSchemaRDD getTableDependencyJSON(String key){
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
