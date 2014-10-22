package com.busap.logJobCommitJ.job.ext;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;

import com.busap.logJobCommitJ.job.LoadExtDataHourJob;
import com.busap.logJobCommitJ.model.Tomcatlog;

public class LoadTomcatDataHourJob extends LoadExtDataHourJob{
	
	

	protected static void runJob(String[] args,Class clz){
		init(args);
		setUp(clz);
		loadData();
		dojob();
		saveData();
	}
	
	private static void dojob(){
		if(year!=null&&month!=null&&day!=null&&hour!=null){
			parquetRDD = sourceRDD.map(new Function<String, Tomcatlog>() {
					private static final long serialVersionUID = 1L;
					public Tomcatlog call(String line) throws Exception {
			          String[] parts = line.split("\\|");
			          Tomcatlog tomcatlog = new Tomcatlog();
			          String date= parts[0];
			          String userId= parts[1];
			          String province= parts[2];
			          String city= parts[3];
			          String platform= parts[4];
			          String version= parts[5];
			          String channel= parts[6];
			          String operationMain= parts[7];
			          String operationSlave= parts[8];
			          String extend= parts[9];
			          tomcatlog.setDate(date);
			          tomcatlog.setUserId(StringUtils.isNotEmpty(userId)?Integer.parseInt(userId.trim()):0);
			          tomcatlog.setProvince(province);
			          tomcatlog.setCity(city);
			          tomcatlog.setPlatform(platform);
			          tomcatlog.setVersion(version);
			          tomcatlog.setChannel(channel);
			          tomcatlog.setOperationMain(operationMain);
			          tomcatlog.setOperationSlave(operationSlave);
			          tomcatlog.setExtend(extend);
			          return tomcatlog;
			        }
				});
			extRDD = parquetRDD.map(new Function<Tomcatlog,String>(){
				private static final long serialVersionUID = 1L;
				public String call(Tomcatlog tomcat) throws Exception {
					String ext = tomcat.getExtend();
					return ext;
				}
			});
		}
	}

	public static void main(String[] args) {
		runJob(args,Tomcatlog.class);
	}

	


}
