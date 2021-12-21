package com.dtstack.flink.scala.datastream.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @author xiuyuan
 */
class MyAggregateFunction extends AggregateFunction[(Int, String, Int), AggregateAcc, (Int, String, Int)] {
  override def createAccumulator(): AggregateAcc = AggregateAcc()

  override def add(value: (Int, String, Int), accumulator: AggregateAcc): AggregateAcc = {
    println("value =" + value + "\n accumulator =" + accumulator)

    AggregateAcc(value._1, value._2, value._3 + accumulator.count)
  }

  override def getResult(acc: AggregateAcc): (Int, String, Int) = (acc.bookId, acc.bookName, acc.count)

  override def merge(a: AggregateAcc, b: AggregateAcc): AggregateAcc = {

    AggregateAcc(a.bookId, a.bookName, a.count + b.count)
  }
}

case class AggregateAcc(
                         bookId: Int = 0,
                         bookName: String = null,
                         count: Int = 0
                       )