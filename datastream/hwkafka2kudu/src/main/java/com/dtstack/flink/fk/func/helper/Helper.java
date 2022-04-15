package com.dtstack.flink.fk.func.helper;

import com.dtstack.flink.fk.util.GlobalConfig;

import java.io.Serializable;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flink.fk.util.GlobalConfig.batchSize;
import static com.dtstack.flink.fk.util.GlobalConfig.filedNameEqual;

/** @author beifeng */
public abstract class Helper<T> implements AutoCloseable, Serializable {
  protected String tableName;
  protected boolean isBatch;
  protected List<String> pk;
  protected int batchSize;
  protected int count = 0;
  protected boolean isUpper;
  protected boolean filedNameEqual;
  protected long upTime;
  private ScheduledExecutorService executorService;

  public Helper(String tableName, boolean isBatch, List<String> pk, boolean isUpper) {
    if (pk.size() < 1) {
      throw new RuntimeException(
          "Pk field must be specified, corresponding to configuration {kudu.pk}");
    }
    this.tableName = tableName;
    this.isBatch = isBatch;
    this.pk = pk;
    this.isUpper = isUpper;
    if (isBatch) {
      batchSize = batchSize();
    }
    filedNameEqual = filedNameEqual();
  }

  public void init() {
    upTime = System.currentTimeMillis();
    if (isBatch) {
      executorService = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "kudu-flush-thread"));
      // 或许没什么用,如果启动batch,保证数据停止发送后也可以将未提交的数据进行提交
      executorService.scheduleWithFixedDelay(
          new TimerTask() {
            final int var0 = 1000 * 60 * 2;

            @Override
            public void run() {
              if (System.currentTimeMillis() - upTime > var0) {
                if (count >= 0) {
                  try {
                    flush();
                  } catch (Exception ignored) {
                  }
                }
              }
            }
          },
          120,
          150,
          TimeUnit.SECONDS);
    }
  }

  public abstract void flush() throws Exception;

  public abstract void invoke(T value, String op) throws Exception;

  public abstract void open() throws Exception;

  @Override
  public void close() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}
