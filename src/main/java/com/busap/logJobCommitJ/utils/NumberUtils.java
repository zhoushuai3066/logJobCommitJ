package com.busap.logJobCommitJ.utils;

public class NumberUtils {
	
	public static String getLessTen(int number){
		String result = String.valueOf(number);
		if(number<10){
			result =  "0"+number;
		}
		return result;
	}
	
	public static void main(String[] args){
		System.out.println(getLessTen(10));
	}

}
