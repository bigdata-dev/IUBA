package com.ryxc.iuba.dao;

import com.ryxc.iuba.conf.ConfigurationManager;
import com.ryxc.iuba.dao.factory.DAOFactory;
import com.ryxc.iuba.domain.Task;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tonye0115 on 2018/2/7.
 */
public class ITaskDAOTest {
    @Test
    public void getPropertyTest() {
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(1);
        System.out.println(task.getTaskName());
        Assert.assertTrue(true);
    }
}
