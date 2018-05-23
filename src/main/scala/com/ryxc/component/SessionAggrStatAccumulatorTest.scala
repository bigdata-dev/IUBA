package com.ryxc.component

import com.ryxc.iuba.constant.Constants
import com.ryxc.iuba.utils.StringUtils
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

object SessionAggrStatAccumulatorTest {

  def main(args: Array[String]): Unit = {

    object SessionAggrStatAccumulator extends AccumulatorParam[String] {
      override def addInPlace(v1: String, v2: String): String = {
        if (v1 == "") {
          v2
        } else {
          val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
          val newValue = Integer.valueOf(oldValue) + 1
          StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
        }
      }

      override def zero(initialValue: String): String = Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
    }

    // 创建spark上下文环境
    val sparkConf = new SparkConf()
      .setAppName("SessionAggrStatAccumulatorTest")
      .setMaster("local")

    val sc = new SparkContext(sparkConf)

    val sessionAggrStatAccumulator = sc.accumulator("")(SessionAggrStatAccumulator)

    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)
    print(rdd.foreach(d=>println(d)))
    rdd.foreach(sessionAggrStatAccumulator.add(_))
    println(sessionAggrStatAccumulator.value)
  }
}


