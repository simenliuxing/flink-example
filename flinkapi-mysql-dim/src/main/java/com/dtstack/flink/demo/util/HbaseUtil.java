package com.dtstack.flink.demo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 获取hbase连接的工具类
 *
 * @author beifeng
 */
public class HbaseUtil {
    private static Connection connection;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();

            // configuration.set("hbase.zookeeper.property.dataDir","/hbase2");
            // configuration.set("zookeeper.znode.parent","/hbase2");
            // configuration.set("hbase.unsafe.stream.capability.enforce","false");

            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("hbase 连接失败");
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void main(String[] args) throws IOException {


        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("xzw_test");
        boolean b = admin.tableExists(tableName);

        byte[] infos = Bytes.toBytes("info");
        admin.createTable(
                TableDescriptorBuilder
                        .newBuilder(tableName)
                        .setColumnFamily(
                                ColumnFamilyDescriptorBuilder
                                        .newBuilder(infos)
                                        .build()
                        )
                        .build()
        );

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
