package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 简单的维表关联
 *
 * @author beifeng
 */
public class MapFunc extends RichMapFunction<User, User> {
    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println("!");
    }

    @Override
    public void close() throws Exception {
        if(!connection.isClosed()){
           connection.close();
        }
    }

    @Override
    public User map(User user) throws Exception {
        System.out.println(user);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "select * from mysqldim where uid =" + user.getUserid() + " ;"
        );

        rs.next();
        String orderId = rs.getString("id");
        String orderTime = rs.getString("order_time");

        user.setOrderId(orderId);
        user.setOrderTime(orderTime);


        return user;
    }

}
