package com.dtstack.flink.fk.func.helper;

import com.dtstack.flink.fk.pojo.ColumnInfo;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.dtstack.flink.fk.func.ToKuduSinkFunc.*;

/** @author beifeng */
public class TmpSink extends Helper<ColumnInfo> {
  int i = 0;
  private Connection connection;
  /** TODO 是保留sql形式还是对象有待思考,如果失败,需要重新执行sql,或者通过对象重新构建sql */
  private Map<String, ColumnInfo> batchData;

  public TmpSink(String tableName, boolean isBatch, List<String> pk, boolean isUpper) {
    super(tableName, isBatch, pk, isUpper);
  }

  @Override
  public void open() throws Exception {
    Class.forName("com.cloudera.impala.jdbc41.Driver");

    connection =
        DriverManager.getConnection(
            "jdbc:impala://172.16.101.218:8191/default;AuthMech=3", "hzhxb", "admin123");
    //    connection = DriverManager.getConnection(impalaJdbcUrl(), username(), password());
  }

  private void fillStatement(PreparedStatement stat, String op, ColumnInfo columnInfo)
      throws SQLException {

    if (op.equals(OP_DELETE)) {
      stat.setString(1, columnInfo.getAcct_no());
      stat.setString(2, columnInfo.getRec_no());
      stat.setString(3, columnInfo.getPost_date());
    } else if (op.equals(OP_INSERT)) {
      stat.setString(1, columnInfo.getAcct_no());
      stat.setString(2, columnInfo.getRec_no());
      stat.setString(3, columnInfo.getTran_type());
      stat.setString(4, columnInfo.getStat());
      stat.setString(5, columnInfo.getPost_date());
      stat.setString(6, columnInfo.getTrn_date());
      stat.setString(7, columnInfo.getSystem_date());
      stat.setString(8, columnInfo.getSystem_time());
      stat.setString(9, columnInfo.getTell_and_br());
      stat.setString(10, columnInfo.getJrnl_no());
      stat.setString(11, columnInfo.getTrn_code());
      stat.setString(12, columnInfo.getBrterm());
      stat.setString(13, columnInfo.getChannel());
      stat.setString(14, columnInfo.getDeli());
      stat.setString(15, columnInfo.getVar_area());
    } else {
      stat.setString(1, columnInfo.getStat());
      stat.setString(2, columnInfo.getPost_date());
      stat.setString(3, columnInfo.getTrn_date());
      stat.setString(4, columnInfo.getSystem_date());
      stat.setString(5, columnInfo.getSystem_time());
      stat.setString(6, columnInfo.getTell_and_br());
      stat.setString(7, columnInfo.getJrnl_no());
      stat.setString(8, columnInfo.getTrn_code());
      stat.setString(9, columnInfo.getBrterm());
      stat.setString(10, columnInfo.getChannel());
      stat.setString(11, columnInfo.getDeli());
      stat.setString(12, columnInfo.getVar_area());
      stat.setString(13, columnInfo.getAcct_no());
      stat.setString(14, columnInfo.getRec_no());
      stat.setString(15, columnInfo.getPost_date());
    }
  }

  private String buildSql(String op) {
    String sql;
    if (OP_INSERT.equals(op)) {
      sql =
          "insert into "
              + tableName
              + "(ACCT_NO, REC_NO, TRAN_TYPE, STAT, POST_DATE, TRN_DATE, SYSTEM_DATE, "
              + "`SYSTEM_TIME`, TELL_AND_BR, JRNL_NO, TRN_CODE, BRTERM, CHANNEL, DELI, VAR_AREA) "
              + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

    } else if (OP_UPDATE.equals(op)) {
      sql =
          "update "
              + tableName
              + " set  STAT=?, POST_DATE=?, TRN_DATE=?, `SYSTEM_DATE`=?,`SYSTEM_TIME`=?, "
              + "TELL_AND_BR=?, JRNL_NO=?, TRN_CODE=?, BRTERM=?, CHANNEL=?, DELI=?,"
              + " VAR_AREA=? where ACCT_NO = ? and REC_NO=?  and POST_DATE=? ";

    } else if (OP_DELETE.equals(op)) {
      sql = "delete from " + tableName + " where ACCT_NO = ? and REC_NO= ?  and POST_DATE= ? ";

    } else {
      throw new IllegalArgumentException(String.format("Illegal type %s", op));
    }
    return sql;
  }

  @Override
  public void flush() throws Exception {}

  @Override
  public void invoke(ColumnInfo columnInfo, String op) throws Exception {

    Class<?> clazz = columnInfo.getClass();
    StringBuilder sb = new StringBuilder();
    for (String k : pk) {
      Field f = clazz.getDeclaredField(k);
      sb.append((String) f.get(columnInfo));
    }
    String key = sb.toString();

    // 如果key的值没有获取到是否选择过滤呢 ?
    if (StringUtils.isEmpty(key)) {
      return;
    }

    switch (op) {
      case OP_INSERT:
        if (batchData.containsKey(key)) {
          // 如果同一个主键被insert两次会是什么效果,让我们想一想
        } else {
          batchData.put(key, columnInfo);
        }
        break;
      case OP_UPDATE:
        // 强制更新
        batchData.put(key, columnInfo);
        break;
      case OP_DELETE:
        // remove会返回key之前关联的value,如果等于null,则说明没有我们添加到
        // map中,如果!=null,我们已经remove了,所以不需要重复操作
        if (batchData.remove(key) == null) {
          batchData.put(key, columnInfo);
        }
        break;
      default: // kill the ninja
        throw new IllegalArgumentException(String.format("Illegal type %s", op));
    }

    // 执行单个语句
    single(columnInfo, op);
  }

  private void single(ColumnInfo columnInfo, String op) throws SQLException {
    String sql = buildSql(op);
    PreparedStatement stat = connection.prepareStatement(sql);
    fillStatement(stat, op, columnInfo);
    stat.execute();
  }

  @Override
  public void close() throws Exception {
    if (!connection.isClosed()) {
      connection.close();
    }
  }
}
