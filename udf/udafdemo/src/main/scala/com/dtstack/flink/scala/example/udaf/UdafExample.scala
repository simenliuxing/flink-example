package com.dtstack.flink.scala.example.udaf


import org.apache.flink.table.functions._
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author xiuyuan
 */
class UdafExample extends AggregateFunction[java.lang.Double, WeightedAvgAccum] {
  //private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  // todo 可能是产品bug,导致该部分功能不可用
  //获取累加器中的值
  override def getValue(accumulator: WeightedAvgAccum): java.lang.Double = {
    if (accumulator.totalWeighted != 0) {
      accumulator.totalValue / accumulator.totalWeighted
    } else {
      null
    }
  }

  //创建一个累加器
  override def createAccumulator(): WeightedAvgAccum = WeightedAvgAccum()

  //定义一个累加器累加的函数
  def accumulate(accumulator: WeightedAvgAccum, inputValue: java.lang.Double, inputWeight: java.lang.Long): Unit = {
    try {
      accumulator.totalWeighted += inputWeight
      accumulator.totalValue += inputValue * inputWeight
    } catch {
      //  case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  //若出现脏数据则回退操作
  def retract(accumulator: WeightedAvgAccum, inputValue: java.lang.Double, inputWeight: java.lang.Long): Unit = {
    try {
      accumulator.totalWeighted -= inputWeight
      accumulator.totalValue -= inputValue * inputWeight
    } catch {
      //case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }


  //约束函数只能用在OVER_WINDOWS_ONLY
  //override def getRequirements: util.Set[FunctionRequirement] = util.Set[FunctionRequirement]
}

/**
 * 自定义累加器ACC
 * totalWeighted 累计的权重
 * totalValue  累计的加权值
 */
case class WeightedAvgAccum(
                             var totalWeighted: Long = 0,
                             var totalValue: Double = 0
                           )
