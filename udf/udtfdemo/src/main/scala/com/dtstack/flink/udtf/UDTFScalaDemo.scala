package com.dtstack.flink.udtf

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


class UDTFScalaDemo extends TableFunction[Row] {

  def eval(str: String): Unit = {
    // use collect(...) to emit a row
    //print("===================" + str +"=================")
    var strings: Array[String] = new Array[String](4)
    if (StringUtils.isNotEmpty(str) && str.contains(",")) {
      strings = str.split(",");
      collect(Row.of(strings(0),strings(1)))
    }
    else {
      collect(Row.of(str,str))
    }

  }

  override def getResultType: TypeInformation[Row] = Types.ROW(Types.STRING, Types.STRING)

}
