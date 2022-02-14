package com.dtstack.flink.streamudtf.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/** @author 行星 on 2021/11/03. */
public class TzBankFiledSplitUdaf extends TableFunction<Tuple2<String, Double>> {

  public static final String CHAR_1 = ",";

  public void eval(String str) {
    if (str == null || "".equals(str)) {
      collect(Tuple2.of("", 0.0));
    } else {
      for (String s : str.split(CHAR_1)) {
        String[] ss = s.split("\\|");
        Tuple2<String, Double> t2 = new Tuple2<>(ss[0], Double.valueOf(ss[1]));
        collect(t2);
      }
    }
  }
}
