package com.bigdata.userbehavior

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppCoursesStudyAnalysis {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    if("".equals(day) || day.length() != 8){
      println("Usage:Please input date,eg:20190424")
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = spark.sql(
      s"""
         |insert overwrite table tmp.app_course_study_analysis_${day}
         |select sum(watch_video_count),sum(complete_video_count),dt from (
         |select count(distinct uid) as watch_video_count,0 as complete_video_count,dt from dwd.user_behavior where dt = ${day} and event_key = 'startVideo' group by dt
         |union all
         |select 0 as watch_video_count,count(distinct uid) as complete_video_count,dt from dwd.user_behavior where dt = ${day} and event_key = 'endVideo'
         |and (end_video_time - start_video_time) >= video_length group by dt) tmp group by dt
          """.stripMargin)


    spark.stop()
  }
}
