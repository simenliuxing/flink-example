package com.dtstack.test;

import com.dtstack.flink.fk.func.helper.KuduClientSinkHelper;

import java.util.HashMap;

public class KuduSinkTest {
  public static void main(String[] args) throws Exception {

    KuduClientSinkHelper<Object> ks = new KuduClientSinkHelper<>("", true, null, true);

    ks.open();
    final HashMap<String, Object> map = new HashMap<>();
    map.put("ACCT_NO", "test");
    map.put("REC_NO", "tesast");
    map.put("TRAN_TYPE", "tessst");
    map.put("STAT", "tessst");
    map.put("TRN_DATE", "tdest");
    map.put("POST_DATE", "tsest");
    map.put("SYSTEM_DATE", "test");
    map.put("SYSTEM_TIME", "test");
    map.put("TELL_AND_BR", "test");
    map.put("JRNL_NO", "test");
    map.put("TRN_CODE", "test");
    map.put("BRTERM", "test");
    map.put("CHANNEL", "test");
    map.put("DELI", "test");
    map.put("VAR_AREA", "test");

    ks.insert(map);
    ks.testQuery(() -> null);
    map.put("ACCT_NO", "tes1t");
    map.put("STAT", "asda");
    ks.insert(map);
    ks.testQuery(() -> null);

    map.clear();
    map.put("ACCT_NO", "test");
    ks.delete(map);
    map.put("ACCT_NO", "tes1t");
    ks.delete(map);

    System.out.println("-----");
    ks.testQuery(() -> null);
    System.out.println("-----");

    map.put("ACCT_NO", "test11");
    map.put("REC_NO", "tes1ast");
    map.put("TRAN_TYPE", "te1ssst");
    map.put("STAT", "tes1sst");
    map.put("TRN_DATE", "t1dest");
    map.put("POST_DATE", "tse1st");
    map.put("SYSTEM_DATE", "t1est");
    map.put("SYSTEM_TIME", "t1est");
    map.put("TELL_AND_BR", "test");
    map.put("JRNL_NO", "test");
    map.put("TRN_CODE", "test");
    map.put("BRTERM", "test");
    map.put("CHANNEL", "test");
    map.put("DELI", "test");
    map.put("VAR_AREA", "test");
    ks.insert(map);

    ks.testQuery(() -> null);

    Thread.sleep(1000);

    // ks.delete(map);

    //   Thread.sleep(1000);

    //  ks.testQuery( () -> null);

  }
}
