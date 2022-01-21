package com.dtstack.flink.udf

import org.apache.flink.table.functions.ScalarFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class UDFScalaDemo extends ScalarFunction {

  def eval(name: String) = {
    var finalName = ""
    try if (!("" == name) && name != null) finalName = name + "abcd"
    else throw new Exception("The field you entered does not meet requirements")
    catch {
      case _ =>
    }
    finalName
  }
 //    截取字符串
 //    def eval(s: String, begin: Integer, end: Integer): String = {
 //    s.substring(begin, end)

}


