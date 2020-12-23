package com.atguigu.spark.project.app

import java.time.LocalDate

import com.atguigu.spark.project.bean.AdsLog
import com.atguigu.spark.project.util.MyJdbcUtil
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/23 15:41
 */
object BlackListHandler {
    val url = "jdbc:mysql://hadoop162:3306/spark0821?user=root&password=aaaaaa"
    val inserSql = "insert into user_ad_count values(?,?,?,?) on duplicate key update COUNT=count+?"
    val querySql = "select userid, count from user_ad_count where dt=? and count>=?"
    
    def writeBlackList(adsLogStream: DStream[AdsLog]) = {
        // 先计算每个用户每个广告的点击量, 然后把点击量写到mysql, 然后再判断数据是否到了阈值,决定是否写入到黑名单
        adsLogStream
            .map(log => (log.logDate, log.userId, log.adsId) -> 1L)
            .reduceByKey(_ + _)
            .foreachRDD(rdd => {
                rdd.foreachPartition((it: Iterator[((String, String, String), Long)]) => {
                    // 1. 点击量数据写入到Mysql
                    val data = it.map {
                        case ((date, userId, adsId), count) =>
                            Array(date, userId, adsId, count, count)
                    }
                    MyJdbcUtil.writeToJdbc(url, inserSql, data)
                    
                    // 2. 查看每个用户对每个广告的点击量是否到了阈值, 如果到了, 写入到黑名单
                    //
                    val userIds = MyJdbcUtil
                        .readFormJdbc(url, querySql, Array(LocalDate.now().toString, 30))
                        .map(map => Array[Any](map("userid")))
                        .toIterator
                    
                    // 3. 把用户地写入到黑名单
                    MyJdbcUtil.writeToJdbc(url, "replace INTO black_list values(?)", userIds)
                })
            })
    }
    
}
