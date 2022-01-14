package com.dtstack.flink.udtf;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** @Author xiaoyu @Create 2022/1/12 11:27 @Description */
public class UdtfDemo extends TableFunction<Row> {

  public void eval(String str) {
    collect(Row.of(str.split(":")[0], str.split(":")[1]));
  }

  //规定返回值的类型
  @Override
  public TypeInformation<Row> getResultType() {

    return new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
  }
}
