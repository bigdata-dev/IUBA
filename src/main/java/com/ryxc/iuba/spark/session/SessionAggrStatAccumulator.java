package com.ryxc.iuba.spark.session;

import com.ryxc.iuba.utils.StringUtils;
import org.apache.spark.AccumulatorParam;
import com.ryxc.iuba.constant.Constants;
/**
 * Created by tonye0115 on 2018/2/8.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

    /**
     * 数据的初始化
     * @param initialValue
     * @return 初始化中所有范围区间的数量 0
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 在v1中，找到对应的value，累加1，然后再更新会连接串里面去
     * @param v1    初始化连接串
     * @param v2    在遍历session的时候，判断出某个session对应的区间，传入相应变量
     * @return
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }


    /**
     * session统计计算逻辑
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2){
        //校验: v1为空，直接返回v2
        if(StringUtils.isEmpty(v1)){
            return v2;
        }

        //从v1中提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if(oldValue != null){
            int newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }

        return v1;

    }



}
