package com.dtstack.flink.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** @Author xiaoyu @Create 2022/1/12 11:27 @Description */
@FunctionHint(output = @DataTypeHint("ROW<name STRING, departmentName STRING"))
public class UdtfDemo extends TableFunction<Row> {

  public void eval(String str) {

    collect(Row.of(str.split(":")[0], str.split(":")[1]));
  }
}
