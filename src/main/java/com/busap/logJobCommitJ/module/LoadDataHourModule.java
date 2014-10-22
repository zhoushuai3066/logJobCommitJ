package com.busap.logJobCommitJ.module;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;


public abstract class LoadDataHourModule<T> extends Module<T>{
	
	
	
	
	protected JavaRDD<String> sourceRDD;
	
	protected JavaRDD<T> targetRDD;
	
	/**
	 * 需要通过给sourceRDD给targetRDD 赋值
	 */
	public abstract void doJob();
	
	
	private void loadData(){
		String source = tableLocation.getTxt();
//		String source = FileLocationUtils.getModuleHourLocation(module,year, month, day, hour);
		sourceRDD =  ctx.textFile(source);
	}
	
	public void saveData(){
//		String target = FileLocationUtils.getModuleDBHourLocation(module, year, month, day, hour);
		String target = tableLocation.getParquet();
		JavaSchemaRDD schemaTomcatlog = sqlCtx.applySchema(targetRDD,entityClass);
		schemaTomcatlog.saveAsParquetFile(target);
	}
	
	
	protected void runJob(String[] args){
		setUp();
		init(args);
		loadData();
		doJob();
		saveData();
	}
	

}
