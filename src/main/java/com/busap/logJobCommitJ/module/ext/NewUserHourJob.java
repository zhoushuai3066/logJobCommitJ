package com.busap.logJobCommitJ.module.ext;



import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.busap.logJobCommitJ.model.NewUser;
import com.busap.logJobCommitJ.model.Tomcatlog;
import com.busap.logJobCommitJ.module.HourModule;
import com.busap.logJobCommitJ.table.Table;

public class NewUserHourJob extends HourModule<NewUser>{
	
	
	public static void main(String[] args) {
		NewUserHourJob newuserhourjob = new NewUserHourJob();
		newuserhourjob.runJob(args);
		
	}

	
	@Override
	public void setUp() {
		super.setUp();
		Table tl = new Table(Tomcatlog.class);
		addTableDependency("tomcatlog", tl);
	}


	@Override
	public void doJob() {
		JavaSchemaRDD tomcatlogSchemaRDD = getTableDependencyParquet("tomcatlog");
		targetRDD = sqlCtx.sql("select userId,date from tomcatlog");
	}


}
