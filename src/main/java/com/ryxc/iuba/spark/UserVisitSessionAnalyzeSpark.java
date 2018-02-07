package com.ryxc.iuba.spark;

import com.ryxc.iuba.conf.ConfigurationManager;
import com.ryxc.iuba.constant.Constants;
import com.ryxc.iuba.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by tonye0115 on 2018/2/7.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成测试数据
        mockData(sc, sqlContext);

        sc.close();

    }

    private static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc);
        }else{
            return new HiveContext(sc);
        }

    }

    /**
     * 模拟生成模拟数据
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc, sqlContext);
        }
    }
}
