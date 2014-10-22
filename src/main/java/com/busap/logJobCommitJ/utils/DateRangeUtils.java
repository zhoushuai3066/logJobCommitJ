package com.busap.logJobCommitJ.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;



public class DateRangeUtils {
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public static Date getDate(String date){
		Date result = null;
	 try {
		 result =  sdf.parse(date);
	} catch (ParseException e) {
		e.printStackTrace();
	}
	 return result;
	}
	
	public static String getDate(Date date){
		String result = null;
		result =  sdf.format(date);
		return result;
	}
	
	
	
	public static Date generateDate(String year,String month,String day,String hour){
		Date result = null;
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.YEAR, Integer.parseInt(year));
		cl.set(Calendar.MONTH, Integer.parseInt(month));
		cl.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
		cl.set(Calendar.HOUR_OF_DAY, Integer.parseInt(hour));
		result = cl.getTime();
		return result;
	}
	
	public static Date generateDate(String year,String month){
		Date result = null;
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.YEAR, Integer.parseInt(year));
		cl.set(Calendar.MONTH, Integer.parseInt(month));
		result = cl.getTime();
		return result;
	}
	
	
	public static Date generateDate(String year,String month,String day){
		Date result = null;
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.YEAR, Integer.parseInt(year));
		cl.set(Calendar.MONTH, Integer.parseInt(month));
		cl.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
		result = cl.getTime();
		return result;
	}
	
	public static Date generateDate(String year){
		Date result = null;
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.YEAR, Integer.parseInt(year));
		result = cl.getTime();
		return result;
	}
	
	public static Integer monthRange(String begin,String end){
		Date start = null;
		Date stop = null;
		try {
			start = sdf.parse(begin);
		    stop = sdf.parse(end);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		int rangeyear = stop.getYear() - start.getYear();
		int rangemonth = stop.getMonth() - start.getMonth();
		
		Integer result = null;
		if(rangeyear>0){
			result = rangeyear*12+rangemonth;
		}else{
			result = rangemonth;
		}
		return result;
	}
	
	public static Integer dayRange(String begin,String end){
		Date start = null;
		Date stop = null;
		try {
			start = sdf.parse(begin);
		    stop = sdf.parse(end);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		long range = (stop.getTime() - start.getTime())/24/60/60/1000;
		return Integer.parseInt(String.valueOf(range));
	}
	
	
	public static Integer hourRange(String begin,String end){
		Date start = null;
		Date stop = null;
		try {
			start = sdf.parse(begin);
		    stop = sdf.parse(end);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		long range = (stop.getTime() - start.getTime())/1000/60/60;
		return Integer.parseInt(String.valueOf(range));
	}
	
	
	public static Iterator<Date> iteratorHour(String begin,int end){
		List<Date> ls = new ArrayList<Date>();
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			long beginDateTime = beginDate.getTime();
			ls.add(new Date(beginDateTime));
		    for(int i = 0;i<end;i++){
		    	beginDateTime =beginDateTime+60*60*1000;
		    	ls.add(new Date(beginDateTime));
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	public static Iterator<Date> iteratorDay(String begin,int end){
		List<Date> ls = new ArrayList<Date>();
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			long beginDateTime = beginDate.getTime();
			ls.add(new Date(beginDateTime));
		    for(int i = 0;i<end;i++){
		    	beginDateTime =beginDateTime+24*60*60*1000;
		    	ls.add(new Date(beginDateTime));
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	public static Iterator<Date> iteratorMonth(String begin,int end){
		List<Date> ls = new ArrayList<Date>();
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			Calendar cl = Calendar.getInstance();
			cl.setTime(beginDate);
			ls.add(beginDate);
		    for(int i = 0;i<end;i++){
		    	cl.add(Calendar.MONTH, 1);
		    	ls.add(cl.getTime());
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	
	
	public static Iterator<Date> iteratorHour(String begin,String end){
		List<Date> ls = new ArrayList<Date>();
		
		int endtime = hourRange(begin,end);
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			long beginDateTime = beginDate.getTime();
			ls.add(new Date(beginDateTime));
		    for(int i = 0;i<endtime;i++){
		    	beginDateTime =beginDateTime+60*60*1000;
		    	ls.add(new Date(beginDateTime));
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	public static Iterator<Date> iteratorDay(String begin,String end){
		List<Date> ls = new ArrayList<Date>();
		int endtime = dayRange(begin,end);
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			long beginDateTime = beginDate.getTime();
			ls.add(new Date(beginDateTime));
		    for(int i = 0;i<endtime;i++){
		    	beginDateTime =beginDateTime+24*60*60*1000;
		    	ls.add(new Date(beginDateTime));
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	public static Iterator<Date> iteratorMonth(String begin,String end){
		List<Date> ls = new ArrayList<Date>();
		int endtime = monthRange(begin,end);
		try {
			Date beginDate = sdf.parse(begin);
//			System.out.println(beginDate);
			Calendar cl = Calendar.getInstance();
			cl.setTime(beginDate);
			ls.add(beginDate);
		    for(int i = 0;i<endtime;i++){
		    	cl.add(Calendar.MONTH, 1);
		    	ls.add(cl.getTime());
		    }
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return ls.iterator();
	}
	
	
	
	
	public static void main(String[] args) throws ParseException{
//		System.out.println(monthRange("2014-09-30 00:00:00","2015-10-01 23:00:00"));
//		System.out.println(hourRange("2014-09-30 00:00:00","2014-09-30 20:00:00"));
//		Iterator<Date> it = iteratorHour("2014-09-30 00:00:00", hourRange("2014-09-30 00:00:00","2014-09-30 20:00:00"));
//		while(it.hasNext()){
//			Date cu = it.next();
//		 System.out.println(sdf.format(cu));
//		}
		
		
		
//		System.out.println(dayRange("2014-09-30 00:00:00","2014-10-10 20:00:00"));
//		Iterator<Date> it = iteratorDay("2014-09-30 00:00:00", dayRange("2014-09-30 00:00:00","2014-10-10 20:00:00"));
//		while(it.hasNext()){
//			Date cu = it.next();
//		 System.out.println(sdf.format(cu));
//		}
//		
		
//		System.out.println(monthRange("2014-09-30 00:00:00","2014-10-10 20:00:00"));
//		Iterator<Date> it = iteratorMonth("2014-09-30 00:00:00", monthRange("2014-09-30 00:00:00","2015-10-10 20:00:00"));
//		while(it.hasNext()){
//			Date cu = it.next();
//		 System.out.println(sdf.format(cu));
//		}
		
		Calendar cl = Calendar.getInstance();
		cl.setTime(new Date());
		System.out.println(sdf.format(new Date()));
		System.out.println(cl.get(Calendar.YEAR));
		System.out.println(cl.get(Calendar.MONTH)+1);
		System.out.println(cl.get(Calendar.DAY_OF_MONTH));
		System.out.println(cl.get(Calendar.HOUR_OF_DAY));
		
//		sdf.parse("201232");
//		DateUtils du = new DateUtils();
//		Date d1 = new Date();
//		Calendar dd = Calendar.getInstance();
//		dd.setTime(d1);
//		Iterator<Calendar> it = DateUtils.iterator(dd,DateUtils.RANGE_WEEK_MONDAY);
//		
//		while(it.hasNext()){
//			Calendar cu = it.next();
//			System.out.println(cu.get(Calendar.YEAR)+"-"+cu.get(Calendar.MONTH)+"-"+cu.get(Calendar.DATE)+":"+cu.get(Calendar.HOUR));
//		}
	}
	
	

}
