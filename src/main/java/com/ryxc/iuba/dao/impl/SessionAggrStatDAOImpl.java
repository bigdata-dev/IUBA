package com.ryxc.iuba.dao.impl;

import com.ryxc.iuba.dao.ISessionAggrStatDAO;
import com.ryxc.iuba.domain.SessionAggrStat;
import com.ryxc.iuba.jdbc.JDBCHelper;

import java.util.Date;

/**
 * session聚合统计DAO实现类
 * @author Administrator
 *
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {
	
	/**
	 * 插入session聚合统计结果
	 * @param sessionAggrStat 
	 */
	public void insert(SessionAggrStat sessionAggrStat) {
		String sql = "insert into session_aggr_stat(task_id,create_time,session_count,1s_3s,4s_6s,7s_9s,10s_30s,30s_60s," +
				"1m_3m,3m_10m,10m_30m,30m,1_3,4_6,7_9,10_30,30_60,`60`) "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		Object[] params = new Object[]{
				sessionAggrStat.getTaskid(),
				sessionAggrStat.getCreateTime(),
				sessionAggrStat.getSession_count(),
				sessionAggrStat.getVisit_length_1s_3s_ratio(),
				sessionAggrStat.getVisit_length_4s_6s_ratio(),
				sessionAggrStat.getVisit_length_7s_9s_ratio(),
				sessionAggrStat.getVisit_length_10s_30s_ratio(),
				sessionAggrStat.getVisit_length_30s_60s_ratio(),
				sessionAggrStat.getVisit_length_1m_3m_ratio(),
				sessionAggrStat.getVisit_length_3m_10m_ratio(),
				sessionAggrStat.getVisit_length_10m_30m_ratio(),
				sessionAggrStat.getVisit_length_30m_ratio(),
				sessionAggrStat.getStep_length_1_3_ratio(),
				sessionAggrStat.getStep_length_4_6_ratio(),
				sessionAggrStat.getStep_length_7_9_ratio(),
				sessionAggrStat.getStep_length_10_30_ratio(),
				sessionAggrStat.getStep_length_30_60_ratio(),
				sessionAggrStat.getStep_length_60_ratio()};

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
