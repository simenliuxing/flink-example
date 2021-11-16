package com.dtstack.test;

import com.dtstack.flink.demo.util.HbaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseConnectionTest {

    public void test1() throws IOException {
        Connection connection = HbaseUtil.getConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("xzw_test");
        boolean b = admin.tableExists(tableName);

        byte[] infos = Bytes.toBytes("info");
//        admin.createTable(
//                TableDescriptorBuilder
//                        .newBuilder(tableName)
//                        .setColumnFamily(
//                                ColumnFamilyDescriptorBuilder
//                                        .newBuilder(infos)
//                                        .build()
//                        )
//                        .build()
//        );

        Table table = connection.getTable(tableName);

        byte[] row = Bytes.toBytes("test");
        Put put = new Put(row);
        byte[] qualifier = Bytes.toBytes("a");
        put.addColumn(infos, qualifier, Bytes.toBytes("aaa"));
        table.put(put);

        table.close();
        System.out.println("3");
        table = connection.getTable(tableName);
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] value = result.getValue(infos, qualifier);
        System.out.println(new String(value));


        // 关闭资源

    }
}
