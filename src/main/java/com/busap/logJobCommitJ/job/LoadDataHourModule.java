package com.busap.logJobCommitJ.job;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;


public abstract class LoadDataHourModule extends Job{
	
	
	
	
	protected static JavaRDD<String> sourceRDD;
	
	protected static JavaRDD targetRDD;
	
	
	
	private static void loadData(){
		String source = tableLocation.getTxt();
//		String source = FileLocationUtils.getModuleHourLocation(module,year, month, day, hour);
		sourceRDD =  ctx.textFile(source);
	}
	
	public static void saveData(){
//		String target = FileLocationUtils.getModuleDBHourLocation(module, year, month, day, hour);
		String target = tableLocation.getParquet();
		JavaSchemaRDD schemaTomcatlog = sqlCtx.applySchema(targetRDD,entityClass);
		schemaTomcatlog.saveAsParquetFile(target);
	}
	
	
//	protected void runJob(String[] args){
//		setUp();
//		init(args);
//		loadData();
//		doJob();
//		saveData();
//	}
	

}
