package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.AdsLog
import com.atguigu.spark.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/12/23 15:30
 */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("RealtimeApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 从kafka消费数据
        val adsLogStream = MyKafkaUtil
            .getKafkaStream(ssc, "RealtimeApp", "project0821")
            .map(line => {
                val split: Array[String] = line.split(" ")
                AdsLog(split(0).toLong, split(1), split(2), split(3), split(4))
            })
        
        // 需求1: 做黑名单   先计算每个用户每个广告的点击量, 然后把点击量写到mysql, 然后再判断数据是否到了阈值,决定是否写入到黑名单
        BlackListHandler.writeBlackList(adsLogStream)
        
        
        ssc.start()
        ssc.awaitTermination()
        
        
    }
}
