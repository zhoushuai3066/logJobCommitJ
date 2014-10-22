package com.busap.logJobCommitJ.job.ext;



import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.busap.logJobCommitJ.job.HourJob;
import com.busap.logJobCommitJ.model.NewUser;
import com.busap.logJobCommitJ.model.Tomcatlog;
import com.busap.logJobCommitJ.module.HourModule;
import com.busap.logJobCommitJ.table.Table;

public class NewUserHourJob extends HourJob{
	
	private static final String OPERATIONMAIN = "USER";
	private static final String OPERATIONSLAVE = "REGISTER";
	private static final String OPERATIONSLAVEONEKEYREGISTER = "ONEKEYREGISTER";
	
	private static void runJob(String[] args){
		setUp(NewUser.class);
		init(args);
		doJob();
		saveData();
	}
	
	
	public static void doJob() {
		JavaSchemaRDD tomcatlogSchemaRDD = getTableDependencyParquet("tomcatlog");
		targetRDD = sqlCtx.sql("SELECT Tomcatlog.userId,max(Tomcatlog.date) date from Tomcatlog where Tomcatlog.operationMain='"+OPERATIONMAIN+"' AND (Tomcatlog.operationSlave='"+OPERATIONSLAVEONEKEYREGISTER+"' OR Tomcatlog.operationSlave='"+OPERATIONSLAVE+"') group by Tomcatlog.userId");
	}
	
	public static void main(String[] args) {
		Table tl = new Table(NewUser.class);
		addTableDependency("tomcatlog", tl);
		runJob(args);
	}



}
