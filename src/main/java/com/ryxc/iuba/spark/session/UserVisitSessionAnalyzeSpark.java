package com.ryxc.iuba.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ryxc.iuba.conf.ConfigurationManager;
import com.ryxc.iuba.constant.Constants;
import com.ryxc.iuba.dao.ISessionAggrStatDAO;
import com.ryxc.iuba.dao.ITaskDAO;
import com.ryxc.iuba.dao.factory.DAOFactory;
import com.ryxc.iuba.domain.SessionAggrStat;
import com.ryxc.iuba.domain.Task;
import com.ryxc.iuba.test.MockData;
import com.ryxc.iuba.utils.*;
import org.apache.spark.Accumulator;
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
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by tonye0115 on 2018/2/7.
 * 用户访问session分析spark作业
 * 接收用户创建的分析任务，用户指定条件：
 * 1. 时间范围：起始~结束
 * 2. 性别f
 * 3. 年龄范围
 * 4. 职业
 * 5. 城市
 * 6. 搜索词
 * 7. 点击品类
 *
 * 我们的spark作业如何接受用户创建任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中f
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
        System.out.println("###################getActionRDDByDateRange:"+actionRDD.count());
        //首先将行为数据sesion_id进行groupByKey分组, 可以将session粒度的数据和用户数据进行join
        JavaPairRDD<String, String> session2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);

        System.out.println("##################aggregateBySession:"+ session2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple2:session2AggrInfoRDD.take(10)){
            System.out.println(tuple2._2);
        }

        //重构同时进行过滤和统计
        //针对session粒度的聚合数据， 使用指定的参数进行数据过滤
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                session2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        System.out.println("##################filterSessionAndAggrStat:"+ filteredSessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple2:filteredSessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple2._2);
        }
//
//        /**
//         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
//         *
//         * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
//         * 再进行。。。
//         *
//         * 如果没有action的话，那么整个程序根本不会运行。。。
//         *
//         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
//         * 不对！！！
//         *
//         * 必须把能够触发job执行的操作比如rdd.count()，放在最终写入MySQL方法之前
//         *
//         * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
//         */
//        //计算出各个范围的session占比，并写入mysql
//        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());
//
//
//        /**
//         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
//         *
//         * 如果不进行重构，直接来实现，思路：
//         * 1、actionRDD，映射成<sessionid,Row>的格式
//         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
//         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
//         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
//         * 5、将最后计算出来的结果，写入MySQL对应的表中
//         *
//         * 普通实现思路的问题：
//         * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
//         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
//         * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
//         *
//         * 重构实现思路：
//         * 1、不要去生成任何新的RDD（处理上亿的数据）
//         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
//         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
//         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
//         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
//         * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
//         * 		半个小时，或者数个小时
//         *
//         * 开发Spark大型复杂项目的一些经验准则：
//         * 1、尽量少生成RDD
//         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
//         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
//         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
//         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
//         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
//         * 4、无论做什么功能，性能第一
//         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
//         * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
//         *
//         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
//         * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
//         * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
//         * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
//         * 		此时，对于用户体验，简直就是一场灾难
//         *
//         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
//         *
//         * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
//         * 		如果采用第二种方案，那么其实就是性能优先
//         *
//         * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
//         * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
//         * 		积累了，处理各种问题的经验
//         *
//         */

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
                //第一个参数数输入，第二和第三个参数是函数的输出  （tuple第一个值和第二个值）
                new PairFunction<Row, String, Row>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public Tuple2<String, Row> call(Row row) throws Exception {
                            // sessionId 序号是2
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

                        //session的开始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        //session的访问步长
                        int stepLength = 0;

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

                            //计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if(startTime == null){
                                startTime = actionTime;
                            }
                            if(endTime == null){
                                endTime = actionTime;
                            }
                            if(actionTime.before(startTime)){
                                startTime = actionTime;
                            }
                            if(actionTime.after(endTime)){
                                endTime = actionTime;
                            }

                            //计算session访问步长
                            stepLength++;
                        }

                        //计算session访问时长(秒)
                        long visitLenth = (endTime.getTime() - startTime.getTime()) / 1000;

                        //截取两边逗号
                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(categoryIdsBuffer.toString());

                        //还需要将每一行数据和用户信息聚合
                        //如何和用户信息聚合 key应该是userid
                        //直接返回数据个是<userid, partAggInfo>
                        String partAggInfo = Constants.FILED_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FILED_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FILED_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLenth + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
                        return new Tuple2<Long, String>(userid, partAggInfo);
                    }
        });

        //查询所有用户信息
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        //将userInfo ROW 映射
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
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator){

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
                        //筛选条件 火锅 串串香 ipone手机
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

                        //如果经过了之前的多个过滤条件后，就是需要保留的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                        //那么对session的访问时间和访问步长进行统计，相应的累加计数
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
                                Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
                                Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }
                    //计算访问时长
                    private void calculateVisitLength(long visitLength){
                        if(visitLength >= 1&& visitLength <= 3){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        }else if(visitLength >= 4&& visitLength <= 6){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        }else if(visitLength >= 7&& visitLength <= 9){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        }else if(visitLength >= 10&& visitLength <= 30){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        }else if(visitLength > 30&& visitLength <= 60){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        }else if(visitLength > 60&& visitLength <= 180){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        }else if(visitLength > 180&& visitLength <= 600){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        }else if(visitLength > 600&& visitLength <= 1800){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        }else if(visitLength > 1800){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }
                    //计算访问步长
                    private void calculateStepLength(long stepLength){
                        if(stepLength >= 1 && stepLength <=3){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        }else if(stepLength >= 4 && stepLength <=6){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        }else if(stepLength >= 7 && stepLength <=9){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        }else if(stepLength >= 10 && stepLength <=30){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        }else if(stepLength > 30 && stepLength <=60){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        }else if(stepLength > 60){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
        });

        return filteredSession2idAggrInfoRDD;

    }

    /**
     * 计算各session范围占比
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid){
        if(StringUtils.isEmpty(value)){
            return;
        }
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);


        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setCreateTime(new Date());
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }
}
