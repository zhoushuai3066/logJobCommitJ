package com.busap.logJobCommitJ.module;

import java.text.SimpleDateFormat;

import org.apache.spark.sql.api.java.JavaSchemaRDD;


public abstract class HourModule<T> extends Module<T>{
	
	
	protected JavaSchemaRDD targetRDD;
	
	public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss E");
	
	
	public abstract void doJob();
	
	
	private void saveData(){
		String target = tableLocation.getTxt();
//		JavaSchemaRDD schemaTomcatlog = sqlCtx.applySchema(targetRDD,entityClass);
//		schemaTomcatlog.saveAsTextFile(target);
		targetRDD.saveAsTextFile(target);
	}
	

	
	protected void runJob(String[] args){
		setUp();
		init(args);
		doJob();
		saveData();
	}
	

}
