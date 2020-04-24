package com.atguigu.handler

import java.lang
import java.text.SimpleDateFormat

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  def fiterDateByBatch(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    /*过滤思路：以下这种思路有问题，仍然无法解决跨天数据，同一批次数据中有相同的mid,但是不同的日期，
      比如：同一个mid用户，在23点登陆一次，在第二日凌晨登陆一次，但是两条数据在同一个批次中，如果按照
      下边的逻辑无法解决这种问题，在处理跨天数据时会出错*/
//    filterByRedisDStream.map(log =>{
//      (log.mid ,log)
//    }).groupByKey()
//        .flatMap(_._2.take(1))
    //((date,mid),log)
    val resultDStream: DStream[StartUpLog] = filterByRedisDStream.map(log => {
      ((log.logDate, log.mid), log)
    }).groupByKey()
      .flatMap {
        case ((_, _), logIter) => {
          logIter.toList.sortWith(_.logDate < _.logDate).take(1)
        }
      }
    resultDStream

  }

  def fiterDateByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //redis去重的思路：

    startLogDStream.transform(rdd =>{

      //可以考虑使用批量过滤的方式，但是需要考虑跨日的问题
      rdd.mapPartitions(iter =>{

        //获取链接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //过滤相关数据
        val logs: Iterator[StartUpLog] = iter.filter(log => {

          val redisKey = s"dau:${log.logDate}"
          val flag: lang.Boolean = jedisClient.sismember(redisKey,log.mid)
          !flag
        })

        //关闭链接
        jedisClient.close()

        logs
      })



    })

//    startLogDStream
  }


  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
    *
    * @param startLogDStream  经过两次过滤的ds
    */
  def addDateToRedis(startLogDStream: DStream[StartUpLog]) = {

    startLogDStream.foreachRDD(rdd =>{

      rdd.foreachPartition(iter =>{

        val jedisClient: Jedis = RedisUtil.getJedisClient

        iter.foreach( log =>{
          val  redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey,log.mid)
        })



        jedisClient.close()
      })
    })
  }

}
