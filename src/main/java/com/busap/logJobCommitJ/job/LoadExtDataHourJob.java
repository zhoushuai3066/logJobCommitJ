package com.busap.logJobCommitJ.job;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;


public abstract class LoadExtDataHourJob extends Job{
	
	
	
	
	protected static JavaRDD<String> sourceRDD;
	
	protected static JavaRDD parquetRDD;
	protected static JavaRDD<String> extRDD;
	
	
	protected static void loadData(){
		String source = tableLocation.getTxt();
//		String source = FileLocationUtils.getModuleHourLocation(module,year, month, day, hour);
		sourceRDD =  ctx.textFile(source);
	}
	
	protected static void saveData(){
//		String target = FileLocationUtils.getModuleDBHourLocation(module, year, month, day, hour);
		String target = tableLocation.getParquet();
		JavaSchemaRDD schemaTomcatlog = sqlCtx.applySchema(parquetRDD,entityClass);
		schemaTomcatlog.saveAsParquetFile(target);
		
		String jsonpath = tableLocation.getJson();
		JavaSchemaRDD schemaTomcatlogext = sqlCtx.jsonRDD(extRDD);
		schemaTomcatlogext.saveAsParquetFile(jsonpath);
	}
	
	
	

}
