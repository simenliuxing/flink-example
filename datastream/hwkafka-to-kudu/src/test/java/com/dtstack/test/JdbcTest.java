package com.dtstack.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcTest {

  public static void main(String[] args) throws Exception {
    Class.forName("com.cloudera.impala.jdbc41.Driver");
    Connection connection =
        DriverManager.getConnection( // admin123
            "jdbc:impala://172.16.101.218:8191/default;AuthMech=3", "hzhxb", "admin123");

    final Statement statement = connection.createStatement();
    final ResultSet rs = statement.executeQuery("select * from test_xzw");
    while (rs.next()) {
      System.out.println(rs.getString("ACCT_NO"));
      System.out.println(rs.getString("ACCT_NO"));
      System.out.println(rs.getString("TRAN_TYPE"));
      System.out.println(rs.getString("STAT"));
      System.out.println(rs.getString("POST_DATE"));
      System.out.println(rs.getString("TRN_DATE"));
      System.out.println(rs.getString("CHANNEL"));

      System.out.println("------------------------------");
    }

    // impala.batch.username=hzhxb
    // impala.batch.password=admin123

  }
}
