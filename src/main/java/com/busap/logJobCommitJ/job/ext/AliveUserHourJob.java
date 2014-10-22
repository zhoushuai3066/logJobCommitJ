package com.busap.logJobCommitJ.job.ext;



import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.busap.logJobCommitJ.job.HourJob;
import com.busap.logJobCommitJ.model.NewUser;
import com.busap.logJobCommitJ.model.Tomcatlog;
import com.busap.logJobCommitJ.module.HourModule;
import com.busap.logJobCommitJ.table.Table;

public class AliveUserHourJob extends HourJob{
	

	
	
	private static void runJob(String[] args){
		setUp(NewUser.class);
		init(args);
		doJob();
		saveData();
	}
	
	
	public static void doJob() {
		JavaSchemaRDD tomcatlogSchemaRDD = getTableDependencyParquet("tomcatlog");
		targetRDD = sqlCtx.sql("select userId,date,province,city,platform,version,channel from Tomcatlog right outer join (SELECT Tomcatlog.userId userId2 from Tomcatlog  group by Tomcatlog.userId) t2 on t2.userId2 = Tomcatlog.userId");
	}
	
	public static void main(String[] args) {
		Table tl = new Table(NewUser.class);
		addTableDependency("tomcatlog", tl);
		runJob(args);
	}



}
