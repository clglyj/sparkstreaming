package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object MyAlterApp {



  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")

    val ssc = new StreamingContext(conf,Seconds(3))


    //读取kafka数据

    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_EVENT_TOPIC))


    //将kafka的json数据转换为样例类
    val logDStream: DStream[EventInfo] = kafkaDStream.map {
      case (_, value) => {

        val info: EventInfo = JSON.parseObject(value, classOf[EventInfo])
        //获取日志记录时间
        val ts: Long = info.ts
        val timestamp: String = sdf.format(new Date(ts))

        //转换时间格式
        val logDate: String = timestamp.split(" ")(0)
        val logHour: String = timestamp.split(" ")(1)
        //log对象赋值
        info.logDate = logDate
        info.logHour = logHour
        info
      }
    }
//    logDStream.print()


    //根据需求进行预警数据处理

    //开窗

    val winDStream: DStream[EventInfo] = logDStream.window(Seconds(30))

    //对一行数据来说，同一个mid所有的时间日志列表
    val groupDStream: DStream[(String, Iterable[EventInfo])] = winDStream.map(log => {
      (log.mid, log)
    }).groupByKey()

    //拼接预警日志参数
    /**注意map中只能处理数据，并不能删除数据*/
    val booleanDStream: DStream[(Boolean, CouponAlertInfo)] = groupDStream.map {

      case (mid, iter) => {
        val uids = new util.HashSet[String]()
        val items = new util.HashSet[String]()
        val events = new util.ArrayList[String]()

        var noClickProd: Boolean = true


                breakable{
                  iter.foreach(log => {
                    events.add(log.itemid)
                    if ("clickItem".eq(log.evid)) {
                      noClickProd = false
                      //其实只要是点击过，该条数据在该批次中不需要，可以直接剔除
                      break()
                    } else if ("coupon".eq(log.evid)) {
                      //设置预警参数
                      uids.add(log.uid)
                      items.add(log.itemid)
                    }
                  })

                }


        val couponInfo = CouponAlertInfo(mid, uids, items, events, System.currentTimeMillis())
        (uids.size() >= 3 && noClickProd, couponInfo)
      }
    }

    booleanDStream.print()







    ssc.start()
    ssc.awaitTermination()
  }
}
