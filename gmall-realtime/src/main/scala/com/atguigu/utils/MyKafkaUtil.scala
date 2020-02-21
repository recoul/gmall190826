package com.atguigu.utils


import java.util.Properties




object MyKafkaUtil {

  def getKafkaStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {

    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkaPara = Map(
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "group.id" -> "bigdata0408"
    )

    //基于Direct方式消费Kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, topics)

    //返回
    kafkaDStream
  }

}

