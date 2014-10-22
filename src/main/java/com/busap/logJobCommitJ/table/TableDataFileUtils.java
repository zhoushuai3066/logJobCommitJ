package com.busap.logJobCommitJ.table;

import java.io.File;

import com.busap.logJobCommitJ.constants.TableConstants;

public class TableDataFileUtils {
	
	public static String getHDFSHourTxt(String tableName,String year,String month,String day,String hour){
		return TableConstants.TXTPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+hour+File.separator+"f.log";
	}
	
	public static String getHDFSDayTxt(String tableName,String year,String month,String day){
		return TableConstants.TXTPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+"f.log";
	}
	
	public static String getHDFSMonthTxt(String tableName,String year,String month){
		return TableConstants.TXTPATH+tableName+File.separator+year+File.separator+month+File.separator+"f.log";
	}
	
	
	public static String getHDFSHourParquet(String tableName,String year,String month,String day,String hour){
		return TableConstants.PARQUETPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+hour+File.separator+"f.parquet";
	}
	
	public static String getHDFSDayParquet(String tableName,String year,String month,String day){
		return TableConstants.PARQUETPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+"f.parquet";
	}
	
	public static String getHDFSMonthParquet(String tableName,String year,String month){
		return TableConstants.PARQUETPATH+tableName+File.separator+year+File.separator+month+File.separator+"f.parquet";
	}
	
	
	public static String getHDFSHourJSON(String tableName,String year,String month,String day,String hour){
		return TableConstants.PARQUETEXTPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+hour+File.separator+"f.json";
	}
	
	public static String getHDFSDayJSON(String tableName,String year,String month,String day){
		return TableConstants.PARQUETEXTPATH+tableName+File.separator+year+File.separator+month+File.separator+day+File.separator+"f.json";
	}
	
	public static String getHDFSMonthJSON(String tableName,String year,String month){
		return TableConstants.PARQUETEXTPATH+tableName+File.separator+year+File.separator+month+File.separator+"f.json";
	}
	
}
