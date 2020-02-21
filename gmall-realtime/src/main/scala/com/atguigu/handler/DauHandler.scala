package com.atguigu.handler

import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  // 同批次去重

  def filterDataByBatch(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {

    //a.将数据转换为((date,mid),log)
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedis.map(log =>
      ((log.logDate, log.mid), log)
    )

    //b.按照key分组
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //c.组内按照日期排序，取第一个元素

    val value: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap { case ((_, _), logIter) =>
      val logs: List[StartUpLog] = logIter.toList.sortWith(_.ts < _.ts).take(1)
      logs
    }

    //d.返回数据
    value
  }


  //跨批次去重(根据Redis中的数据进行去重)



  def filterDataByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    startLogDStream.transform(rdd => {



      rdd.mapPartitions(iter => {
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //过滤
        val logs: Iterator[StartUpLog] = iter.filter(log => {
          val redisKey = s"dau:${log.logDate}"
          !jedisClient.sismember(redisKey, log.mid)
        })
        //释放连接
        jedisClient.close()
        logs
      })
    })

  }


   //将2次过滤后的数据集中的mid保存至Redis


  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {

    startLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //将数据写入Redis
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //释放连接
        jedisClient.close()

      })

    })

  }

}