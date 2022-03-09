package com.dtstack.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 通过sql api使用flink cdc 原生功能
 *
 * @author xuzhiwen
 */
public class MysqlCdcApp {

  public static void main(String[] args) throws Exception {
    /*
     TODO 建表语句,source和sink结构一样,表名改一下即可测试

       CREATE TABLE `mysql_cdc_test2` (
         `runoob_id` bigint NOT NULL,
         `runoob_title` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `runoob_author` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `submission_date` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `runoob_id1` bigint DEFAULT NULL,
         `runoob_title1` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `runoob_author1` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `submission_date1` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `runoob_id2` bigint DEFAULT NULL,
         `runoob_title2` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `runoob_author2` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         `submission_date2` varchar(10000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
         PRIMARY KEY (`runoob_id`)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3
    */

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    env.setParallelism(1);

    final String host = "192.168.112.237";
    final int port = 3306;
    final String user = "root";
    final String pwd = "root123456";
    final String db = "test";
    final String sourceTable = "mysql_cdc_test";
    final String sinkTable = "mysql_cdc_test2";

    String sourceDDL =
        String.format(
            "CREATE TABLE source ("
                + "runoob_id   bigint ,\n"
                + "runoob_title   STRING,\n"
                + "runoob_author   STRING,\n"
                + "submission_date   STRING,\n"
                + "runoob_id1   bigint  ,\n"
                + "runoob_title1   STRING,\n"
                + "runoob_author1   STRING,\n"
                + "submission_date1   STRING,\n"
                + "runoob_id2   bigint,  \n"
                + "runoob_title2   STRING, \n"
                + "runoob_author2   STRING,\n"
                + "submission_date2   STRING "
                + ") WITH ("
                + " 'connector' = 'mysql-cdc',"
                + " 'hostname' = '%s',"
                + " 'port' = '%s',"
                + " 'username' = '%s',"
                + " 'password' = '%s',"
                + " 'database-name' = '%s',"
                + " 'table-name' = '%s'"
                + ")",
            host, port, user, pwd, db, sourceTable);

    String url =
        String.format(
            "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8&useSSL=false",
            host, port, db);

    String sinkDDL =
        String.format(
            "CREATE TABLE sink (\n"
                + "runoob_id   bigint ,\n"
                + "runoob_title   STRING,\n"
                + "runoob_author   STRING,\n"
                + "submission_date   STRING,\n"
                + "runoob_id1   bigint  ,\n"
                + "runoob_title1   STRING,\n"
                + "runoob_author1   STRING,\n"
                + "submission_date1   STRING,\n"
                + "runoob_id2   bigint  ,\n"
                + "runoob_title2   STRING,\n"
                + "runoob_author2   STRING,\n"
                + "submission_date2   STRING"
                + "  ,PRIMARY KEY (runoob_id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "   'connector' = 'jdbc',\n"
                + "   'url' = '%s',\n"
                + "   'table-name' = '%s', "
                + " 'username' = '%s', "
                + " 'password' = '%s' "
                + ")",
            url, sinkTable, user, pwd);

    // 创建 source sink table
    tableEnv.executeSql(sourceDDL);
    tableEnv.executeSql(sinkDDL);

    // 添加insert语句插入数据
    String insertSql = "insert into sink select * from source";
    tableEnv.executeSql(insertSql);
    env.execute("flink mysql cdc example");
  }
}
