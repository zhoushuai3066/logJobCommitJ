package com.busap.logJobCommitJ.job;


import org.apache.spark.sql.api.java.JavaSchemaRDD;


public abstract class HourJob extends Job{
	
	
	protected static JavaSchemaRDD targetRDD;
	
	protected static void saveData(){
		String target = tableLocation.getTxt();
//		JavaSchemaRDD schemaTomcatlog = sqlCtx.applySchema(targetRDD,entityClass);
//		schemaTomcatlog.saveAsTextFile(target);
		targetRDD.saveAsTextFile(target);
	}

}
