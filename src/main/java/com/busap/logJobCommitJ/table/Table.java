package com.busap.logJobCommitJ.table;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.busap.logJobCommitJ.utils.DateRangeUtils;
import com.busap.logJobCommitJ.utils.NumberUtils;
import com.busap.logJobCommitJ.table.TableLocation;

public class Table {
	
	private String name;
	protected Class entityClass;
	
	
	
	
	private Map<String,List<TableLocation>> hourpartitions = new HashMap<String,List<TableLocation>>();
	private Map<String,List<TableLocation>> daypartitions = new HashMap<String,List<TableLocation>>();
	private Map<String,List<TableLocation>> monthpartitions = new HashMap<String,List<TableLocation>>();
	
	public Table(Class clz){
		entityClass =clz;
		this.name = entityClass.getSimpleName();
	}
	
	
	
	
	public String getName() {
		return name;
	}




	public void setName(String name) {
		this.name = name;
	}




	public Class getEntityClass() {
		return entityClass;
	}




	public void setEntityClass(Class entityClass) {
		this.entityClass = entityClass;
	}




	/**
	 * 
	 * @param name 分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadHourPartition(String key,String begin,String end){
		Iterator<Date> it = DateRangeUtils.iteratorHour(begin,end);
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		Integer day = null;
		Integer hour = null;
		List<TableLocation> paths = hourpartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			day = cl.get(Calendar.DAY_OF_MONTH);
			hour = cl.get(Calendar.HOUR_OF_DAY);
			String hdfspath = TableDataFileUtils.getHDFSHourTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			String hdfsTpath = TableDataFileUtils.getHDFSHourParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			String jsonPath = TableDataFileUtils.getHDFSHourJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		hourpartitions.put(key, paths);
	}
	
	
	/**
	 * 
	 * @param name 分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadDayPartition(String key,String begin,String end){
		Iterator<Date> it = DateRangeUtils.iteratorDay(begin,end);
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		Integer day = null;
		List<TableLocation> paths = daypartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			day = cl.get(Calendar.DAY_OF_MONTH);
			String hdfspath = TableDataFileUtils.getHDFSDayTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			String hdfsTpath = TableDataFileUtils.getHDFSDayParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			String jsonPath = TableDataFileUtils.getHDFSDayJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		daypartitions.put(key, paths);
	}
	
	
	/**
	 * 
	 * @param name 分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadMonthPartition(String key,String begin,String end){
		Iterator<Date> it = DateRangeUtils.iteratorDay(begin,end);
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		List<TableLocation> paths = monthpartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			String hdfspath = TableDataFileUtils.getHDFSMonthTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			String hdfsTpath = TableDataFileUtils.getHDFSMonthParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			String jsonPath = TableDataFileUtils.getHDFSMonthJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		monthpartitions.put(key, paths);
	}
	
	
	
	/**
	 * 
	 * @param name 表分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadHourPartition(String key,String begin){
		List<Date> ls = new ArrayList<Date>();
		ls.add(DateRangeUtils.getDate(begin));
		Iterator<Date> it = ls.iterator();
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		Integer day = null;
		Integer hour = null;
		List<TableLocation> paths = hourpartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			day = cl.get(Calendar.DAY_OF_MONTH);
			hour = cl.get(Calendar.HOUR_OF_DAY);
			String hdfspath = TableDataFileUtils.getHDFSHourTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			String hdfsTpath = TableDataFileUtils.getHDFSHourParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			String jsonPath = TableDataFileUtils.getHDFSHourJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)), String.valueOf(NumberUtils.getLessTen(hour)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		hourpartitions.put(key, paths);
	}
	
	
	/**
	 * 
	 * @param name 表分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadDayPartition(String key,String begin){
		List<Date> ls = new ArrayList<Date>();
		ls.add(DateRangeUtils.getDate(begin));
		Iterator<Date> it = ls.iterator();
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		Integer day = null;
		List<TableLocation> paths = daypartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			day = cl.get(Calendar.DAY_OF_MONTH);
			String hdfspath = TableDataFileUtils.getHDFSDayTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			String hdfsTpath = TableDataFileUtils.getHDFSDayParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			String jsonPath = TableDataFileUtils.getHDFSDayJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)), String.valueOf(NumberUtils.getLessTen(day)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		daypartitions.put(key, paths);
	}
	
	
	/**
	 * 
	 * @param name 分区名字
	 * @param begin 2014-01-01 01:00:00
	 * @param end 2014-01-01 02:00:00
	 */
	public void loadMonthPartition(String key,String begin){
		List<Date> ls = new ArrayList<Date>();
		ls.add(DateRangeUtils.getDate(begin));
		Iterator<Date> it = ls.iterator();
		Calendar cl = Calendar.getInstance();
		Integer year =null;
		Integer month = null;
		List<TableLocation> paths = monthpartitions.get(key);
		if(paths==null){
			 paths = new ArrayList<TableLocation>();
		}
		while(it.hasNext()){
			Date cu = it.next();
			cl.setTime(cu);
			year = cl.get(Calendar.YEAR);
			month = cl.get(Calendar.MONTH);
			String hdfspath = TableDataFileUtils.getHDFSMonthTxt(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			String hdfsTpath = TableDataFileUtils.getHDFSMonthParquet(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			String jsonPath = TableDataFileUtils.getHDFSMonthJSON(name, String.valueOf(year), String.valueOf(NumberUtils.getLessTen(month)));
			TableLocation tl = new TableLocation();
			tl.setTxt(hdfspath);
			tl.setParquet(hdfsTpath);
			tl.setJson(jsonPath);
			paths.add(tl);
		}
		monthpartitions.put(key, paths);
	}
	
	
	public List<TableLocation> getHourPartition(String key){
		return hourpartitions.get(key);
	}
	
	public List<TableLocation> getDayPartition(String key){
		return daypartitions.get(key);
	}
	
	public List<TableLocation> getMonthPartition(String key){
		return monthpartitions.get(key);
	}
	
	
	

	
	
}
