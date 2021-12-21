package com.dtstack.flink.scala.example.udf.util

import org.slf4j.{Logger, LoggerFactory}

import java.sql._

/**
 * @author xiuyuan
 */
object JdbcUtil {
  // todo 可能是产品bug,导致该部分功能不可用
  //private val LOG:Logger = LoggerFactory.getLogger(JdbcUtil.getClass)

  def getConnect(driverName: String, url: String, user: String, password: String): Connection = {
    try {
      Class.forName(driverName)
      DriverManager.getConnection(url, user, password)
    } catch {
      //case e:Exception => LOG.error(e.toString)
      case _ => null
    }
  }

  def close(con: Connection): Unit = {
    if (con != null && !con.isClosed) {
      con.close()
    }
  }


}
