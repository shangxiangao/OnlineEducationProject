package com.bigdata.streaming

import java.sql.{Connection, Date, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

object VipIncrementAnalysis {
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("VipIncrementAnalysis.properties"))

  val ipdb = new City(this.getClass().getClassLoader().getResource("ipipfree.ipdb").getPath())

  val driver = prop.getProperty("jdbcDriver")
  val jdbcUrl =  prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword = prop.getProperty("jdbcPassword")

  Class.forName(driver)
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)


  def filterCompleteOrderData(msg: (String, String)): Boolean = {
    val fields = msg._2.split("\t")
    if(fields.length == 17){
      val eventType = msg._2.split("\t")(15)
      "completeOrder".equals(eventType)
    }else{
      false
    }
  }

  def getCountryAndDate(msg : (String,String)): ((String,String),Int) ={
    val fields = msg._2.split("\t")
    val ip = fields(8)
    val eventTime = fields(16).toLong

    val date = new Date(eventTime * 1000)
    val eventDay = sdf.format(date)

    var regionName = "未知"
    val info = ipdb.findInfo(ip,"CN")
    if(info != null){
      regionName = info.getRegionName()
    }

    ((eventDay,regionName),1)
  }

  def filter2DaysBeforeState(state : ((String,String),Int)): Boolean ={
    val day = state._1._1
    val eventTime = sdf.parse(day).getTime
    val currentTime = System.currentTimeMillis()
    if(currentTime - eventTime >= 172800000){
      false
    }else{
      true
    }
  }
  def getVipIncrementByCountry(checkPoint: String): StreamingContext = {
    val updateFunc = (values: Seq[Int],state: Option[Int])=>{
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val processingInterval = prop.getProperty("processingInterval").toLong
    val brokers = prop.getProperty("brokers")
    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "enable.auto.commit" ->"false")
    val fromOffsets = DB.readOnly { implicit session => sql"select topic, part_id, offset from topic_offset".
      map { r =>
        TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list.apply().toMap
    }


    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic + "-" + mmd.partition, mmd.message())
    var offsetRanges : Array[OffsetRange] = Array.empty[OffsetRange]

    // Get Dstream
    val  messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    messages.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter{ msg =>
      filterCompleteOrderData(msg)
    }.map{ msg =>
      getCountryAndDate(msg)
    }.updateStateByKey[Int]{
      updateFunc
    }.filter{ state =>
      filter2DaysBeforeState(state)
    }.foreachRDD(rdd=> {
      val resultTuple: Array[((String, String), Int)] = rdd.collect()

      // 开启事务
      DB.localTx { implicit session =>
        resultTuple.foreach(msg => {
          val dt = msg._1._1
          val province = msg._1._2
          val cnt = msg._2.toLong

          sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate().apply()
          println(msg)
        })

        for (o <- offsetRanges) {
          println(o.topic,o.partition,o.fromOffset,o.untilOffset)
          sql"""update topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update.apply()
        }
      }
    })
    ssc.checkpoint(checkPoint)
    messages.checkpoint(Seconds(processingInterval * 10))
    ssc
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("Usage:Please input checkpointPath")
      System.exit(1)
    }
    val checkPoint = args(0)

    val ssc = StreamingContext.getOrCreate(checkPoint,
      () => {getVipIncrementByCountry(checkPoint)}
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
