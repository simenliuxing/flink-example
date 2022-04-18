package com.dtstack.flink.fk.func.helper;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.kudu.client.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.dtstack.flink.fk.func.ToKuduSinkFunc.*;
import static com.dtstack.flink.fk.util.GlobalConfig.kuduAddress;

/**
 * 基于kudu api的sink代码实现
 *
 * @author beifeng
 */
public class KuduClientSinkHelper<IN> extends Helper<IN> {

  private KuduTable table;
  private KuduClient client;
  private KuduSession session;

  public KuduClientSinkHelper(String tableName, boolean isBatch, List<String> pk, boolean isUpper) {
    super(tableName, isBatch, pk, isUpper);
  }

  @Override
  public void invoke(IN value, String op) throws Exception {
    switch (op) {
      case OP_INSERT:
        if (isBatch) {
          batchInsert(buildParam(value));
        } else {
          insert(buildParam(value));
        }
        break;
      case OP_UPDATE:
        update(buildParam(value));
        break;
      case OP_DELETE:
        delete(buildParam(value));
        break;
      default:
        throw new IllegalArgumentException(String.format("Illegal type %s", op));
    }
  }

  @Override
  public void flush() throws Exception {
    session.flush();
  }

  public HashMap<String, Object> buildParam(IN in) throws IllegalAccessException {
    final Field[] fields = in.getClass().getDeclaredFields();
    final HashMap<String, Object> f = new HashMap<>(fields.length);
    for (Field field : fields) {
      field.setAccessible(true);
      final Object o = field.get(in);
      if (o != null) {
        if (isUpper) {
          f.put(field.getName().toUpperCase(), o);
        } else {
          f.put(field.getName().toLowerCase(), o);
        }
      }
    }
    return f;
  }

  @Override
  public void open() throws Exception {
    client =
        new KuduClient.KuduClientBuilder(kuduAddress()).defaultOperationTimeoutMs(100000).build();
    session = client.newSession();

    if (!client.tableExists(tableName)) {
      throw new Exception(String.format("Table:{%s} not exists !", table));
    }
    table = client.openTable(tableName);
    if (isBatch) {
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    }
    init();
  }

  public void insert(Map<String, Object> kv) throws Exception {
    addAndFlush(kv, table.newInsert());
  }

  public void batchInsert(Map<String, Object> kv) throws Exception {
    addBatchAndFlush(kv, table.newInsert());
    if (count >= batchSize) {
      flush();
    }
  }

  public void update(Map<String, Object> kv) throws Exception {
    addAndFlush(kv, table.newUpdate());
  }

  public void delete(Map<String, Object> kv) throws Exception {
    addAndFlush(kv, table.newDelete());
  }

  public void upsert(Map<String, Object> kv) throws Exception {
    throw new Exception("Upsert is not supported for the time being !");
  }

  private void addAndFlush(Map<String, Object> kv, Operation action) throws Exception {

    PartialRow row = action.getRow();
    for (Map.Entry<String, Object> entry : kv.entrySet()) {
      row.addObject(entry.getKey(), entry.getValue());
    }
    session.apply(action);
    session.flush();
  }

  private void addBatchAndFlush(Map<String, Object> kv, Operation action) throws Exception {
    PartialRow row = action.getRow();
    for (Map.Entry<String, Object> entry : kv.entrySet()) {
      row.addObject(entry.getKey(), entry.getValue());
    }
    session.apply(action);
    count++;
    upTime = System.currentTimeMillis();
  }

  /** 用于test包测试 */
  @VisibleForTesting
  public void testQuery(Supplier<? extends KuduPredicate> filterSupplier) throws Exception {

    KuduScanner.KuduScannerBuilder kuduScannerBuilder = client.newScannerBuilder(table);

    final KuduPredicate f = filterSupplier.get();
    KuduScanner scanner;
    if (f != null) {
      scanner = kuduScannerBuilder.addPredicate(f).build();
    } else {
      scanner = kuduScannerBuilder.build();
    }

    while (scanner.hasMoreRows()) {
      RowResultIterator rows = scanner.nextRows();
      while (rows.hasNext()) {
        RowResult row = rows.next();
        String id = row.getString("ACCT_NO");
        String name = row.getString("TRAN_TYPE");
        String age = row.getString("STAT");
        System.out.println(id + "---" + name + "---" + age);
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (count > 0) {
      session.flush();
    }
    if (session != null && !session.isClosed()) {
      session.close();
    }
    if (client != null) {
      client.close();
    }
  }
}
