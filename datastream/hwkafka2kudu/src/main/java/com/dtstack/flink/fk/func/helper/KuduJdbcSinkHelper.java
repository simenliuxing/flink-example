package com.dtstack.flink.fk.func.helper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flink.fk.func.ToKuduSinkFunc.*;
import static com.dtstack.flink.fk.util.GlobalConfig.*;

/** @author beifeng */
public class KuduJdbcSinkHelper<IN> extends Helper<IN> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduJdbcSinkHelper.class);
  private boolean mark = false;
  private Connection connection;
  /** TODO 是保留sql形式还是对象有待思考,如果失败,需要重新执行sql,或者通过对象重新构建sql */
  private Map<String, String> batchSql;

  private Statement statement;
  private String fn;

  public KuduJdbcSinkHelper(String tableName, boolean isBatch, List<String> pk, boolean isUpper) {
    super(tableName, isBatch, pk, isUpper);
  }

  @Override
  public void open() throws Exception {
    Class.forName("com.cloudera.impala.jdbc41.Driver");
    final String url = impalaJdbcUrl();
    connection = DriverManager.getConnection(url, username(), password());

    batchSql = new HashMap<>();
    statement = connection.createStatement();
    init();
  }

  @Override
  public void invoke(IN in, String op) throws Exception {
    Class<?> clazz = in.getClass();
    StringBuilder sb = new StringBuilder();

    for (String k : pk) {
      Field f = clazz.getDeclaredField(k);
      f.setAccessible(true);
      sb.append((String) f.get(in));
    }
    String key = sb.toString();

    // 如果key的值没有获取到是否选择过滤呢 ?
    if (StringUtils.isEmpty(key)) {
      return;
    }

    switch (op) {
      case OP_INSERT:
        if (batchSql.containsKey(key)) {
          // 如果同一个主键被insert两次会是什么效果,让我们想一想
          // 如果在拥有主键的情况的下如果插入两次会有问题,你觉得?
          // 除非是upset
        } else {
          String sqlPart = insertSqlBuilder(in, clazz);
          String sql = "insert into " + sqlPart;
          System.out.println("获得insert sql = " + sql);
          batchSql.put(key, sql);
        }
        break;
      case OP_UPDATE:
        // 强制更新
        String sql = updateSqlBuilder(in, clazz);
        System.out.println("update sql = " + sql);
        batchSql.put(key, sql);
        break;
      case OP_DELETE:
        // remove会返回key之前关联的value,如果等于null,则说明没有我们添加到
        // map中,如果!=null,我们已经remove了,所以不需要重复操作
        // if (batchSql.remove(key) == null) {
        sql = deleteSqlBuilder(in, clazz);
        System.out.println("delete sql = " + sql);
        batchSql.put(key, sql);
        //   }
        break;
      default: // kill the ninja
        throw new IllegalArgumentException(String.format("Illegal type %s", op));
    }

    upTime = System.currentTimeMillis();
    if (count++ >= batchSize) {
      // TODO 没有做容错
      flush();
    }
  }

  @Override
  public void flush() throws SQLException {
    for (String sql : batchSql.values()) {
      statement.addBatch(sql);
    }
    final int[] batch = statement.executeBatch();
    LOG.info("Execute batch, the number of processing  is {} !", batch.length);
  }

  private String deleteSqlBuilder(IN in, Class<?> clazz)
      throws NoSuchFieldException, IllegalAccessException {
    StringBuilder sb = new StringBuilder("delete from " + tableName + " where ");

    mark = false;
    for (String k : pk) {
      final Field f = clazz.getDeclaredField(k);
      f.setAccessible(true);

      fn = filedName(f);
      if (mark) {
        sb.append("' and ");
      }
      sb.append(fn).append("='").append(f.get(in));
      mark = true;
    }

    return sb.append("'").toString();
  }

  private String filedName(Field f) {
    String filedName;
    if (filedNameEqual) {
      filedName = f.getName();
    } else if (isUpper) {
      filedName = f.getName().toUpperCase();
    } else {
      filedName = f.getName().toLowerCase();
    }
    return filedName;
  }

  private String updateSqlBuilder(IN in, Class<?> clazz)
      throws IllegalAccessException, NoSuchFieldException {
    StringBuilder sb;
    final Field[] fields = clazz.getDeclaredFields();
    sb = new StringBuilder("update " + tableName + " set ");

    ArrayList<String> tmp = new ArrayList<>(pk);

    boolean cont = true;
    mark = false;
    for (Field field : fields) {
      field.setAccessible(true);

      fn = filedName(field);

      // --------------去除主键部分----------------------
      for (String k : tmp) {
        if (k.equalsIgnoreCase(field.getName())) {
          break;
        }
      }
      if (cont) {
        if (tmp.size() > 0) {
          tmp.remove(fn);
        } else {
          cont = false;
        }
      }
      // ------------------------------------

      if (mark) {
        sb.append("',");
      }
      if (isUpper) {
        sb.append(fn);
      } else {
        sb.append(fn);
      }
      sb.append("='").append(field.get(in));
      mark = true;
    }
    if (!mark) {
      sb.append("' where ");
    } else {
      sb.append(" where ");
    }

    mark = false;
    for (String k : pk) {
      Field f = clazz.getDeclaredField(k);
      f.setAccessible(true);
      fn = filedName(f);

      if (mark) {
        sb.append("' and ");
      }
      sb.append(fn).append("='");
      sb.append((String) f.get(in));
      mark = true;
    }

    return sb.toString();
  }

  private String insertSqlBuilder(IN in, Class<?> clazz) throws IllegalAccessException {
    final StringBuilder values = new StringBuilder("values(");
    final StringBuilder vb = new StringBuilder(tableName + "(");
    mark = false;
    for (Field field : clazz.getDeclaredFields()) {
      field.setAccessible(true);
      final Object o = field.get(in);
      if (o != null) {

        // before  val = values('v1 --- vb = name(col1
        if (mark) {
          values.append("',");
          vb.append(",");
        }
        // after val = values('v1', --- vb = name(col1,

        // 拼接 table_name(x,x,x...)

        fn = filedName(field);
        vb.append(fn);
        // 拼接 values(x,x,x...)
        values.append("'").append(o);
        mark = true;
      }
    }
    // 收到期望是这样的  val = values('v1 --- vb = name(col1
    // 完整的拼接的样子  val = values('v1') --- vb = name(col1)
    values.append("') ");
    vb.append(") ").append(values);
    return vb.toString();
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (!connection.isClosed()) {
      connection.close();
    }
  }
}
