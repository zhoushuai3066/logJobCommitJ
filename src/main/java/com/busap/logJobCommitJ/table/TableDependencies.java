package com.busap.logJobCommitJ.table;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;



public class TableDependencies {
	
	
	private Map<String,Table> dependencies;
	
	
	public void addHourDependencies(String key,Table table,String begin,String end){
		table.loadHourPartition(key, begin, end);
		dependencies.put(key, table);
	} 
	
	public void addDayDependencies(String key,Table table,String begin,String end){
		table.loadDayPartition(key, begin, end);
		dependencies.put(key, table);
	} 
	
	public void addMonthDependencies(String key,Table table,String begin,String end){
		table.loadMonthPartition(key, begin, end);
		dependencies.put(key, table);
	} 
	
	
	
	public void addHourDependencies(String key,Table table,String begin){
		table.loadHourPartition(key, begin);
		dependencies.put(key, table);
	} 
	
	public void addDayDependencies(String key,Table table,String begin){
		table.loadDayPartition(key, begin);
		dependencies.put(key, table);
	} 
	
	public void addMonthDependencies(String key,Table table,String begin){
		table.loadMonthPartition(key, begin);
		dependencies.put(key, table);
	} 
	
	
	public List<TableLocation> getHour(String key){
		Table table = dependencies.get(key);
		return table.getHourPartition(key);
		
	}
	
	public List<TableLocation> getDay(String key){
		Table table = dependencies.get(key);
		return table.getDayPartition(key);
		
	}
	
	public List<TableLocation> getMonth(String key){
		Table table = dependencies.get(key);
		return table.getMonthPartition(key);
		
	}
	
	public JavaSchemaRDD getParquet(String key, JavaSQLContext sqlCtx){
		Table table = dependencies.get(key);
		List<TableLocation> ls = table.getHourPartition(key);
		if(ls.size()>0){
			TableLocation tl = ls.get(0);
			String parquetlocation = tl.getParquet();
			JavaSchemaRDD sourceRDD = sqlCtx.parquetFile(parquetlocation);
			sourceRDD.registerAsTable(table.getName());
			return sourceRDD;
		}else{
			return null;
		}
	}

}
