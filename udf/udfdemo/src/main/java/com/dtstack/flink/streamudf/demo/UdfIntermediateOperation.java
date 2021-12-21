package com.dtstack.flink.streamudf.demo;

import com.dtstack.flink.streamudf.demo.utils.ConnectionTool;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author xiuyuan
 * @date 2021-12-21 10:27
 * 通过该UDF函数访问数据库，解决日志信息转换成表中数据的增删改操作
 */
public class UdfIntermediateOperation extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(UdfIntermediateOperation.class);

    private Connection connect = null;

    @Override
    public void open(FunctionContext context) {
        try {
            final String user = "****";
            final String password = "****";
            final String url = "****";
            final String driver = "oracle.jdbc.driver.OracleDriver";

            connect = ConnectionTool.getConnect(driver, url, user, password);
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    @Override
    public void close() {
        try {
            ConnectionTool.close(connect);
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
    }


    /**
     * @param tbName      结果表表名
     * @param columnsName 结果表中字段名的拼接字符串
     * @param objects     包含日志中存储操作类型，操作前的主键值，操作后结果表中的对应columnsName字段值
     * @return 标识一个操作执行是否成功
     */
    public boolean eval(String tbName, String columnsName, Object... objects) {
        try {
            Row row = Row.of(objects);
            String[] split = columnsName.split(",");
            StringBuilder sql = new StringBuilder();
            switch ((String) row.getField(0)) {
                case "INSERT":
                    String insert = "insert into " + tbName + "(" + columnsName + ") values(";

                    sql.append(insert);
                    for (int i = 0; i < split.length - 1; i++) {
                        sql.append("?,");
                    }
                    sql.append("?)");

                    return sendExecution(connect, sql.toString(), row);
                case "UPDATE":
                    String update = "update " + tbName + " set \n";

                    sql.append(update);
                    for (String s : split) {
                        sql.append(s).append(" = nvl(?,").append(s).append("),\n");
                    }
                    sql.delete(sql.length() - 2, sql.length() - 1);
                    //多条件更新 or 单条件更新
                    sql.append(" \nwhere ").append(split[0]).append(" = ?");

                    return sendExecution(connect, sql.toString(), row);

                case "DELETE":
                    String delete = " delete from ";
                    sql.append(delete).append(tbName).append(" where ").append(split[0]).append(" = ?");
                    return sendExecution(connect, sql.toString(), row);

                default:
                    throw new RuntimeException("exec failed");
            }
        } catch (Exception e) {
            LOG.error("cause : ", e);
            return false;
        }
    }

    /**
     * @param con JDBC 中的数据库连接
     * @param sql 待执行的SQL语句
     * @param row 表中一行字段值的一个对象
     * @return 返回是否成功执行当前SQL语句
     */
    private boolean sendExecution(Connection con, String sql, Row row) {

        PreparedStatement st = null;
        try {
            st = con.prepareStatement(sql);

            if ("DELETE".equals(row.getField(0))) {
                //给UPDATE操作的where但条件赋值
                st.setObject(1, row.getField(1));
            } else {
                //给INSERT | UPDATE操作的全部字段赋值
                for (int i = 2; i < row.getArity(); i++) {
                    st.setObject(i - 1, row.getField(i));
                }
                //给UPDATE操作的where但条件赋值
                if ("UPDATE".equals(row.getField(0))) {

                    st.setObject(row.getArity() - 1, row.getField(1));
                }
            }

            st.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOG.error(sql);
            LOG.error(e.toString());
            return false;
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException e) {
                LOG.error(e.toString());
            }
        }
    }

}
