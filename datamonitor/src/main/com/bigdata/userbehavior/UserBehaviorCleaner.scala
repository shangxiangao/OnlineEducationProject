package com.bigdata.userbehavior
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserBehaviorCleaner {

  def checkEventValid(event: String) = {
    val fields = event.split("\t")
    fields.length == 17
  }

  def maskPhone(event: String) = {
    var maskPhone = new StringBuilder
    val fields: Array[String] = event.split("\t")
    val phone = fields(9)
    if(phone != null && !"".equals(phone)){
      maskPhone = maskPhone.append(phone.substring(0,3)).append("xxx").append(phone.substring(7,11))
      fields(9) = maskPhone.toString()
    }
    fields.mkString("\t")
  }

  def repairUsername(event: String) = {
    val fields = event.split("\t")
    val username = fields(1)
    if(username != "" && !"Null".equals(username)){
      fields(1) = username.replace("\n","")
    }
    fields.mkString("\t")
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("Usage:please input inputPath and outputPath")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val eventRDD: RDD[String] = sc.textFile(inputPath)
    eventRDD.filter(checkEventValid)
      .map(maskPhone)
      .map(repairUsername)
      .coalesce(3)
      .saveAsTextFile(outputPath)

      sc.stop()
  }
}
