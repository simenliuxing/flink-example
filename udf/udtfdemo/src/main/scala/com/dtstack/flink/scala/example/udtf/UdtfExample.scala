package com.dtstack.flink.scala.example.udtf

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author xiuyuan
 */
class UdtfExample extends TableFunction[Row] {
  // todo 可能是产品bug,导致该部分功能不可用
  //private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   *
   * @param value     传入的数据 包含分割符
   * @param separator 用于切分数据
   */
  def eval(value: String, separator: String): Unit = {
    try {
      val str: Array[String] = value.split(separator)

      if (!str.length.equals(3)) new RuntimeException("The data length does not match, please check the data format")

      // todo 此处使用 Row.of(str(0).toInt,str(1),str(2).toDouble) 编译无法通过--类型转换异常
      val row = new Row(str.length)
      row.setField(0, str(0).toInt)
      row.setField(1, str(1))
      row.setField(2, str(2).toDouble)

      //将传入的所有row都给收集起来，并向下放
      collect(row)

    } catch {
      // case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  override def getResultType: TypeInformation[Row] = Types.ROW(Types.INT, Types.STRING, Types.DOUBLE)
}
