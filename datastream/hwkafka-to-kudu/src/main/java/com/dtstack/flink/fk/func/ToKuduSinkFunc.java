package com.dtstack.flink.fk.func;

import com.dtstack.flink.fk.func.helper.Helper;
import com.dtstack.flink.fk.pojo.ColumnInfo;
import com.dtstack.flink.fk.pojo.KafkaRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dtstack.flink.fk.util.GlobalConfig.kuduTable;

/**
 * flink sink func的实现
 *
 * @author beifeng
 */
public class ToKuduSinkFunc extends RichSinkFunction<KafkaRecord> {
  public static final String OP_INSERT = "I";
  public static final String OP_UPDATE = "U";
  public static final String OP_DELETE = "D";
  private static final String SINK_TABLE = kuduTable();
  private static final Logger LOG = LoggerFactory.getLogger(ToKuduSinkFunc.class);
  private Helper<ColumnInfo> helper;
  private int errorCount = 0;

  public ToKuduSinkFunc(Helper<ColumnInfo> helper) {
    this.helper = helper;
  }

  @Override
  public void invoke(KafkaRecord value, Context context) throws Exception {
    try {
      helper.invoke(value.getColumnInfo(), value.getOP());
    } catch (Exception e) {
      isKill0(e);
    }
  }

  private void isKill0(Exception e) throws Exception {
    // 让我们杀掉他好吗
    final int var1 = 3;
    if (++errorCount > var1) {
      throw e;
    }
    // 要不要杀死任务?
    LOG.error("execution failed .  ", e);
  }

  @Override
  public void close() throws Exception {
    helper.close();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    helper.open();
  }
}
