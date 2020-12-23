package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream

/**
 * Author atguigu
 * Date 2020/12/23 15:41
 */
object BlackListHandler {
    def writeBlackList(adsLogStream: DStream[AdsLog]) = {
        // 先计算每个用户每个广告的点击量, 然后把点击量写到mysql, 然后再判断数据是否到了阈值,决定是否写入到黑名单
        adsLogStream
            .map(log => (log.logDate, log.userId, log.adsId) -> 1L)
            .reduceByKey(_ + _)
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    // ...
                    //...
                    // ...
                })
                
            })
    }
    
}
