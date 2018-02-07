package com.ryxc.iuba.dao.factory;

import com.ryxc.iuba.dao.ITaskDAO;
import com.ryxc.iuba.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {

	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}


}
