package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis


import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {



    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")

    val ssc = new StreamingContext(conf, Seconds(3))


    //获取user数据流  ,Set(GmallConstants.GMALL_USER_INFO_TOPIC)
    val userDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_USER_INFO_TOPIC))

//    userDStream.print()

    //将user信息写入redis缓存
    userDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter =>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach{case (_,value)=>{

          val userInfo: UserInfo = JSON.parseObject(value,classOf[UserInfo])
          jedisClient.set(s"user:${userInfo.id}",value)
        }}
        jedisClient.close()
      })
    })


//
//    //获取orderinfo数据流
    val orderInfoToMapDStream: DStream[(String, OrderInfo)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))
      .map { case (_, v) => {
        val orderInfo: OrderInfo = JSON.parseObject(v, classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1)
        orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "......."
        (orderInfo.id, orderInfo)
      }
      }

    //数据已联通
//    orderInfoToMapDStream.print(100)
//
//
//    //获取orderdetail数据流
    val orderDetailToMapDStream: DStream[(String, OrderDetail)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
      .map { case (_, v) => {
        JSON.parseObject(v, classOf[OrderDetail])
      }
      }.map(detail => {
      (detail.order_id, detail)
    })
    //数据已联通
//    orderDetailToMapDStream.print(100)


    //join操作的前提，两个RDD或者DStream必须是kv类型数据

    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoToMapDStream.fullOuterJoin(orderDetailToMapDStream)


    joinDStream.print(10)


    //主要分为以下几种情况
    //每次处理一个分区的数据
    val orderInfoAndDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions {
      iter => {
      //同一分区获取一个链接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      var list = new ListBuffer[SaleDetail]()
      for ((orderId, (orderInfoOpt, orderDetailOpt)) <- iter) {
        //orderInfo不为空
        if (!orderInfoOpt.isEmpty) {
          //1.orderDetail不为空
          if (!orderDetailOpt.isEmpty) {
            //直接组装对象
            val detail = new SaleDetail(orderInfoOpt.get, orderDetailOpt.get)
            list += detail
          }

          //2.orderInfo写入redis缓存
          val orderInfoRedisKey = s"order:info:${orderId}"
          implicit val formats=org.json4s.DefaultFormats
          val orderInfoJson: String = Serialization.write(orderInfoOpt.get)
          jedisClient.set(orderInfoRedisKey, orderInfoJson)
          jedisClient.expire(orderInfoRedisKey, 300)

          //3.判断缓存中是否命中orderDetail，命中则写出
          val detailRedisKey = s"order:detail:${orderId}"
          val detailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)

          //命中缓存，说明详情数据在上一个批次已经进来
          //不命中缓存则数据丢弃
          if (!detailSet.isEmpty) {
            import collection.JavaConversions._
            detailSet.foreach(detailCache =>{
              val detailJsonStr: OrderDetail = JSON.parseObject(detailCache, classOf[OrderDetail])
              val detail = new SaleDetail(orderInfoOpt.get, detailJsonStr)
              list += detail
            })
          }
        } else {
          //orderInfo为空，则orderDetail一定不为空（如果为空，这样的数据不存在）
          //从缓存中读取orderInfo数据，判断是否可以命中
          val orderInfoRedisKey = s"order:info:${orderId}"
          if (jedisClient.exists(orderInfoRedisKey)) {
            val orderInfoRedis: String = jedisClient.get(orderInfoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoRedis,classOf[OrderInfo])
            val detail = new SaleDetail(orderInfo, orderDetailOpt.get)
            list += detail
          } else {
            //写入缓存
            val detailRedisKey = s"order:detail:${orderId}"
            implicit val formats=org.json4s.DefaultFormats
            val detailJson: String = Serialization.write(orderDetailOpt.get)
            jedisClient.sadd(detailRedisKey, detailJson)
            jedisClient.expire(detailRedisKey, 300)
          }
        }
      }

      jedisClient.close()
      list.toIterator
    }
    }

//    orderInfoAndDetailDStream.print(10)

//    查询redis中user信息，
    val saleDetailDStream: DStream[SaleDetail] = orderInfoAndDetailDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val result: Iterator[SaleDetail] = iter.map(saleDetail => {

        val userJSON: String = jedisClient.get(s"user:${saleDetail.user_id}")

        val userInfo: UserInfo = JSON.parseObject(userJSON, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })

      jedisClient.close()
      result
    })

//    saleDetailDStream.print()

    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter =>{
        //保存数据到ES



      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
