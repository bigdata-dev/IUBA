package com.ryxc.iuba.conf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tonye0115 on 2018/2/7.
 */
public class ConfigurationMangerTest {
    @Test
    public void getPropertyTest() {
        String key1 = ConfigurationManager.getProperty("key1");
        System.out.println(key1);
        Assert.assertTrue(true);
    }
}
