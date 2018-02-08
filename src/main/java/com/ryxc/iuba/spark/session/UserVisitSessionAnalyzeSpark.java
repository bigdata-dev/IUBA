package com.ryxc.iuba.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ryxc.iuba.conf.ConfigurationManager;
import com.ryxc.iuba.constant.Constants;
import com.ryxc.iuba.dao.ITaskDAO;
import com.ryxc.iuba.dao.factory.DAOFactory;
import com.ryxc.iuba.domain.Task;
import com.ryxc.iuba.test.MockData;
import com.ryxc.iuba.utils.ParamUtils;
import com.ryxc.iuba.utils.StringUtils;
import com.ryxc.iuba.utils.ValidUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.math.util.OpenIntToDoubleHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by tonye0115 on 2018/2/7.
 * 用户访问session分析spark作业
 * 接收用户创建的分析任务，用户指定条件：
 * 1. 时间范围：起始~结束
 * 2. 性别
 * 3. 年龄范围
 * 4. 职业
 * 5. 城市
 * 6. 搜索词
 * 7. 点击品类
 *
 * 我们的spark作业如何接受用户创建任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是spark本身提供的特性
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {

        args = new String[]{"1"};

        //构建spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //生成测试数据
        mockData(sc, sqlContext);

        //查询指定任务
        Long teaskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(teaskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        System.out.println("###################taskParam:"+taskParam);

        //如果要进行session粒度的数据聚合
        //首先要从user_visit_action表中，查询指定日期范围
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        System.out.println("###################:"+actionRDD.count());
        //首先将行为数据sesion_id进行groupByKey分组, 可以将session粒度的数据和用户数据进行join
        JavaPairRDD<String, String> session2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);

//        System.out.println(session2AggrInfoRDD.count());
//        for(Tuple2<String, String> tuple2:session2AggrInfoRDD.take(10)){
//            System.out.println(tuple2._2);
//        }

        //针对session粒度的聚合数据， 使用指定的参数进行数据过滤
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD = filterSession(session2AggrInfoRDD, taskParam);
//        System.out.println(filteredSessionid2AggrInfoRDD.count());
//        for(Tuple2<String, String> tuple2:filteredSessionid2AggrInfoRDD.take(10)){
//            System.out.println(tuple2._2);
//        }





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

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                + " from user_visit_action t"
                + " where t.date >=  '" + startDate + "'"
                +" and  t.date <'" + endDate + "'";
        System.out.println(sql);

        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD){
        //将Row映射成<sessionid, Row>格式
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                //第一个参数数输入，第二和第三个参数是tuple第一个值和第二个值
                new PairFunction<Row, String, Row>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public Tuple2<String, Row> call(Row row) throws Exception {
                            return new Tuple2<String, Row>(row.getString(2), row);
                        }
        });

        //对行为数据按照session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
        //对每一个session分组进行聚合， 将session中所有的搜索词和点击品类都聚合起来
        //返回类 <userid, partAggrInfo(sessionid, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long,String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                        String sessionid = tuple2._1;
                        Iterator<Row> iterator = tuple2._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer categoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        //遍历session所有的访问行为
                        while(iterator.hasNext()){
                            //提取每个访问为搜索词字段或者点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.get(6)==null?null:row.getLong(6);

                            //可能为null
                            if(StringUtils.isNotEmpty(searchKeyword)){
                                if(!searchKeywordsBuffer.toString().contains(searchKeyword)){
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            if(clickCategoryId!=null){
                                if(!categoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                                    categoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(categoryIdsBuffer.toString());

                        //还需要将每一行数据和用户信息聚合
                        //如何和用户信息聚合 key应该是userid
                        //直接返回数据个是<userid, partAggInfo>
                        String partAggInfo = Constants.FILED_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FILED_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FILED_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
                        return new Tuple2<Long, String>(userid, partAggInfo);
                    }
        });

        //查询所有用户信息
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //将session粒度聚合数据和用信息join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行转换，并且返回<sessionid, fullAggrInfo>格式的数据
        JavaPairRDD<String, String>  sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple2) throws Exception {
                        String partAggrInfo = tuple2._2._1;
                        Row userInfoRow = tuple2._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FILED_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(5);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FILED_AGE  + "=" + age + "|"
                                + Constants.FILED_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FILED_CITY + "=" + city + "|"
                                + Constants.FILED_SEX + "=" + sex;



                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
        });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam){

        //为了使用ValieUtils, 将筛选条件拼接成一个串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge  = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals  = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities  = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex  = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords  = ParamUtils.getParam(taskParam, Constants.PARAM_SEACH_KEYWORDS);
        String categoryids  = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_SEACH_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryids != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryids + "|" : "");

        if (_parameter.endsWith("\\|")){
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSession2idAggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                        //从tuple2中获取聚合数据
                        String aggrInfo = tuple2._2;

                        //按年龄范围进行过滤
                        if(!ValidUtils.between(aggrInfo, Constants.FILED_AGE, parameter,
                                Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                            return false;
                        }

                        //按职业范围过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FILED_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)){
                            return false;
                        }

                        //按照城市范围进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FILED_CITY, parameter,
                                Constants.PARAM_CITIES)){
                            return false;
                        }

                        //按照性别进行过滤
                        if(!ValidUtils.equal(aggrInfo, Constants.FILED_SEX, parameter,
                                Constants.PARAM_SEX)){
                            return false;
                        }

                        //按照搜索词进行过滤
                        //session搜索了 火锅 蛋糕 烧烤
                        //筛选条件 火锅 串串香
                        //in的校验方法 只要有任何一个搜索词相匹配就算通过
                        if(!ValidUtils.in(aggrInfo, Constants.FILED_SEARCH_KEYWORDS, parameter,
                                Constants.PARAM_SEACH_KEYWORDS)){
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FILED_CLICK_CATEGORY_IDS, parameter,
                                Constants.PARAM_CATEGORY_IDS)){
                            return false;
                        }

                        return true;
                    }
        });

        return filteredSession2idAggrInfoRDD;

    }
}
