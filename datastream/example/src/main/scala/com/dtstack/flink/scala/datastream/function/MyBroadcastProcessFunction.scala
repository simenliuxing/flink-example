package com.dtstack.flink.scala.datastream.function

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

/**
 * @author xiuyuan
 */
class MyBroadcastProcessFunction(mapStateDescriptor: MapStateDescriptor[Int, (Int, String)]) extends BroadcastProcessFunction[(Int, Int, Int), (Int, String), (Int, String, Int)] {

  override def open(parameters: Configuration): Unit = super.open(parameters)

  //处理数据流表中的值
  override def processElement(value: (Int, Int, Int), ctx: BroadcastProcessFunction[(Int, Int, Int), (Int, String), (Int, String, Int)]#ReadOnlyContext, out: Collector[(Int, String, Int)]): Unit = {
    //1.读取广播状态中的值
    val robs: ReadOnlyBroadcastState[Int, (Int, String)] = ctx.getBroadcastState(mapStateDescriptor)
    //2.根据高吞吐量传来的数据去状态中查找数据
    if (robs.contains(value._2)) {
      //根据存在的key 获取对应的值
      val tuple: (Int, String) = robs.get(value._2)
      //向下游流放数据（bookID，bookName,brose）
      out.collect((tuple._1, tuple._2, value._3))
    }
  }

  //处理广播状态中的数据元素
  override def processBroadcastElement(value: (Int, String), ctx: BroadcastProcessFunction[(Int, Int, Int), (Int, String), (Int, String, Int)]#Context, out: Collector[(Int, String, Int)]): Unit = {
    //1、根据上线文获取广播流中的值
    val bs: BroadcastState[Int, (Int, String)] = ctx.getBroadcastState(mapStateDescriptor)
    //2、将传入的值存放到广播状态中
    bs.put(value._1, value)
  }
}
