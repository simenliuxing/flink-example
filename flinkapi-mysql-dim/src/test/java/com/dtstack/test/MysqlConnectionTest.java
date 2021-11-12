package com.dtstack.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlConnectionTest {

    public void test1() throws SQLException, UnknownHostException {
        Connection connection1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection1);


        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        System.out.println(hostAddress);
        Connection connection2 = DriverManager.getConnection("jdbc:mysql://"+hostAddress+":3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection2);
    }
}
