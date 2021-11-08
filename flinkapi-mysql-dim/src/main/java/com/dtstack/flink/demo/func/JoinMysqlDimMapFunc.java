package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.*;

/**
 * 简单的维表关联
 *
 * @author beifeng
 */
public class JoinMysqlDimMapFunc extends RichMapFunction<User, User> {
    Connection connection;

    public static void main(String[] args) throws SQLException {
        Connection connection1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection1);
        Connection connection2 = DriverManager.getConnection("jdbc:mysql://192.168.107.237:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection2);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://192.168.107.237:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println("mysql 连接成功");

    }

    @Override
    public void close() throws Exception {
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public User map(User user) throws Exception {
       // System.out.println(user);

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "select * from mysqldim where id =" + user.getCityId() + " ;"
        );
        if (rs.next()) {
            user.setCityName(rs.getString("city_name"));
        }
        rs.close();
        statement.close();

        return user;
    }

}
