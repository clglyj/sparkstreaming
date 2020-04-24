package com.atguigu.etl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

object MemberETL {


  def main(args: Array[String]): Unit = {

    new SparkConf().setAppName("etl").setMaster("local[*]")
    new SparkContext()
    new SparkSession()



  }

  def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/ods/baswewebsite.log").mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }
}
