package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object MyOrderApp {


  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))

    //将每一行转换为样例类对象

    val orderDStream: DStream[OrderInfo] = kafkaDStream.map { case (_, value) => {
      //转换成样例类
      val order: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      //获取特定时间格式

      val createTime: Array[String] = order.create_time.split(" ")

      //处理数据，给字段重新赋值
      order.create_date = createTime(0)
      order.create_hour = createTime(1).split(":")(0)

      order.consignee_tel = order.consignee_tel.splitAt(3)._1 + "%%%%%%%%"
      order
    }
    }

    orderDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL2019_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new  Configuration() , Some("hadoop102,hadoop103,hadoop104:2181"))
    })


    ssc.start()
    ssc.awaitTermination()



  }

}
