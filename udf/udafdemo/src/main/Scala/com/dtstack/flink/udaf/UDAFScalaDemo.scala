package com.dtstack.flink.udaf

import org.apache.flink.table.functions.AggregateFunction

//自定义累加器ACC
case class WeightedAvgAccum(var totalWeighted: Int = 0, var totalValue: Long = 0)

class UDAFScalaDemo extends AggregateFunction[java.lang.Long, WeightedAvgAccum] {

  //获取累加器中的值
  override def getValue(accumulator: WeightedAvgAccum): java.lang.Long = {
    if (accumulator.totalWeighted != 0) {
      accumulator.totalValue / accumulator.totalWeighted
    } else {
      null
    }
  }

  //创建一个累加器
  override def createAccumulator(): WeightedAvgAccum = WeightedAvgAccum()

  //定义一个累加器累加的函数
  def accumulate(accumulator: WeightedAvgAccum, inputValue: java.lang.Long, inputWeight: java.lang.Integer): Unit = {
    try {
      accumulator.totalWeighted += inputWeight
      accumulator.totalValue += inputValue * inputWeight
    } catch {
      //  case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  //回退操作
  def retract(accumulator: WeightedAvgAccum, inputValue: java.lang.Long, inputWeight: java.lang.Integer): Unit = {
    try {
      accumulator.totalWeighted -= inputWeight
      accumulator.totalValue -= inputValue * inputWeight
    } catch {
      //case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  def merge(acc: WeightedAvgAccum, it: java.lang.Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.totalWeighted += a.totalWeighted
      acc.totalValue += a.totalValue
    }
  }

//  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
//    acc.totalWeighted = 0
//    acc.totalValue = 0L
//  }

}