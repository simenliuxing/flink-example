package com.dtstack.flink.scala.example.udf

import com.dtstack.flink.scala.example.udf.util.JdbcUtil
import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, PreparedStatement}
import scala.annotation.varargs

/**
 * @author xiuyuan
 * 将oracle中的logminer日志中的操作记录，存到kafka中，并基于原始的数据在UDF中将操作同步到数据库中
 */
class UdfExample extends ScalarFunction {
  // todo 可能是产品bug,导致该部分功能不可用
  //private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  private var con: Connection = _


  override def open(context: FunctionContext): Unit = {
    try {
      val user = "****"
      val password = "****"
      val url = "****"
      val driverName = "oracle.jdbc.driver.OracleDriver"
      con = JdbcUtil.getConnect(driverName, url, user, password)
    } catch {
      //case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  override def close(): Unit = {
    try {
      JdbcUtil.close(con)
    } catch {
      //case e: Exception => logger.error(e.toString)
      case _ =>
    }
  }

  /**
   * 注解@varargs 的作用是标识函数中有传入的可变长参数
   * 注解@DataTypeHint(inputGroup = InputGroup.ANY) 表示Scala中输入的参数是any类型的
   *
   * @param tbName      表名
   * @param columnsName 所有列名的拼接
   * @param objects     此时得到的是包装后的数组，其数据结构与Row高度 享受
   * @return
   */
  @varargs
  def eval(tbName: String, columnsName: String, @DataTypeHint(inputGroup = InputGroup.ANY) objects: AnyRef*): Boolean = {

    try {

      val strings: Array[String] = columnsName.split(",")
      val sql: StringBuilder = new StringBuilder

      //进行模式匹配，用来比较操作类型
      objects(0).toString match {
        case "INSERT" =>
          //构建可执行的SQL语句
          val insert: String = "insert into " + tbName + " (" + columnsName + ") values("
          sql.append(insert)
          for (_ <- 0 until strings.length - 1) {
            sql.append("? ,")
          }
          sql.append("? )")
          //调用方法，返回结果值
          sendExecution(con, sql.toString(), objects)

        case "DELETE" =>
          val delete: String = "delete from " + tbName + " where "
          sql.append(delete).append(strings(0)).append(" = ?")
          sendExecution(con, sql.toString(), objects)

        case "UPDATE" =>
          val update: String = "update " + tbName + " set "
          sql.append(update)
          for (i <- strings.indices) {
            sql.append(strings(i)).append(" = nvl(?,").append(strings(i)).append("),\n")
          }
          sql.deleteCharAt(sql.length - 2)
          sql.append("where ").append(strings(0)).append(" = ?")
          sendExecution(con, sql.toString(), objects)

        case _ => new RuntimeException("Illegal operation, please try again"); false

      }
    } catch {
      //case e: Exception => logger.error(e.toString)
      case _ =>
        false
    }
  }

  //执行具体SQL的函数

  /**
   *
   * @param con 与数据库之间的连接
   * @param sql 将要执行的SQL
   * @param row 传过来的字段值
   * @return 操作的成功与否
   */
  def sendExecution(con: Connection, sql: String, row: Seq[AnyRef]): Boolean = {
    //println(sql)
    var ps: PreparedStatement = null

    try {
      //获取发送SQL的对象
      ps = con.prepareStatement(sql)
      if (row.head.equals("DELETE")) {
        ps.setObject(1, row(1))
      } else {
        //针对Insert| update操作  给结果表中所有字段赋值，为SQL中的问号占位符赋值
        for (a <- 2 until row.length) {
          ps.setObject(a - 1, row(a))
        }

        //若是操作为update 需要给 where条件赋值
        if (row.head.equals("UPDATE")) {
          ps.setObject(row.length - 1, row(1))
        }

      }
      //执行发送过去的SQL语句
      ps.executeUpdate()
      true
    } catch {
      //case e: SQLException => logger.error(sql);
      // logger.error(e.toString)
      case _ =>
        false
    } finally {
      try {
        if (ps != null && !ps.isClosed) {
          ps.close()
        }
      } catch {
        //case e: SQLException => logger.error(e.toString)
        case _ =>
      }

    }
  }

}
