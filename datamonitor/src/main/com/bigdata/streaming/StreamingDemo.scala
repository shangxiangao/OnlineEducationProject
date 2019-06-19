package com.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB}

object StreamingDemo {
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("VipIncrementAnalysis.properties"))
  val ipdb = new City(this.getClass.getClassLoader.getResourceAsStream("ipipfree.ipdb"))

  val driver = prop.getProperty("jdbcDriver")
  val url = prop.getProperty("jdbcUrl")
  val user = prop.getProperty("jdbcUser")
  val password = prop.getProperty("jdbcPassword")
  val duration = prop.getProperty("processingInterval")
  val brokers = prop.getProperty("brokers")
  val topic = prop.getProperty("topic")

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  Class.forName(driver)
  ConnectionPool.singleton(url,user,password)


  def filterCompleteOrderData(msg: (String, String)): Boolean = {

    val a: Array[String] = msg._2.split("\t")
    if (a.length == 17) {
      val evenType = a(15)
      "completeOrder".equals(evenType)
    } else {
      false
    }
  }

  def getCityAndDate(msg: (String, String)): ((String, String), Int) = {
    val a: Array[String] = msg._2.split("\t")
    val ip: String = a(8)
    val evenTime: Long = a(16).toLong

    /**
      * 获取日期
      */
    val date = new Date(evenTime * 1000)
    val evenDay: String = sdf.format(date)

    /**
      * 获取城市
      */
    var regionName = "未知"
    val info: CityInfo = ipdb.findInfo(ip, "zh")
    if (info != null) {
      regionName = info.getRegionName
    }


    ((evenDay, regionName), 1)
  }


  def getVipIncrementEveryDay(checkpointDir: String): StreamingContext = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(duration.toLong))

    val kafkaParam = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers)

    val fromOffset: Map[TopicAndPartition, Long] = DB.readOnly(session => session.list("select topic, part_id, offset from topic_offset") {
      rs => TopicAndPartition(rs.string("topic"), rs.int("part_id")) -> rs.long("offset")
    }.toMap)

    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
      kafkaParam,
      fromOffset,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message()))

    var offsets = Array.empty[OffsetRange]

    dstream.transform(rdd => {
      offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(filterCompleteOrderData).map(getCityAndDate)


    ssc
  }
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("lack of arguments")
      System.exit(1)
    }
    val checkpointDir = args(0)

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDir, () => getVipIncrementEveryDay(checkpointDir))
    ssc.start()
    ssc.awaitTermination()
  }

}
