package com.busap.logJobCommitJ.utils;

public class ParameterUtils {
	
	
	public static String getYear(String[] args){
		if(checkLength(args,1)){
			return args[0];
		}else{
			return null;
		}
	}
	
	public static String getMonth(String[] args){
		if(checkLength(args,2)){
			return args[1];
		}else{
			return null;
		}
		
	}

	public static String getDay(String[] args){
		if(checkLength(args,3)){
			return args[2];
		}else{
			return null;
		}
		
	}
	
	public static String getHour(String[] args){
		if(checkLength(args,4)){
			return args[3];
		}else{
			return null;
		}
	}
	
	public static boolean checkLength(String[] args,int size){
		
		if(args.length>=size){
			return true;
		}else{
			return false;
		}
		
	}
}
