package com.ryxc.iuba.constant;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * 数据库相关的常量
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String SPARK_LOCAL = "spark.local";

	/**
	 * spark 作业相关常量
	 */
	String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
	String FILED_SESSION_ID = "sessionid";
	String FILED_SEARCH_KEYWORDS = "searchKeywords";
	String FILED_CLICK_CATEGORY_IDS = "clickCategoryIds";
	String FILED_AGE = "age";
	String FILED_PROFESSIONAL = "professional";
	String FILED_CITY = "city";
	String FILED_SEX = "sex";

	/**
	 * 任务相关的常量
	 */
	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_SEACH_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryids";

	
}
