package com.dtstack.flink.fk.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author xuzhiwen
 */
public class HdfsUtils {

    public static final String HDFS_KEY_TABLE_PATH = "/data/kafka/kafka.keytab";
    public static final String HDFS_KRB5_PATH = "/data/kafka/krb5.conf";
    private static final String HDFS_URL = "hdfs://172.16.101.77:9000";
    public static final String DIR = System.getProperty("user.dir") + "/datastream/hwkafka-to-kudu/src/main/resources/kerberos/";

    public static String LOCAL_KEYTAB = DIR + "hwkafka.keytab";
    public static String LOCAL_KRB = DIR + "hwkrb5.conf";

    public static void downloadFile() throws InterruptedException, IOException {
        FileSystem fs = FileSystem.get(URI.create(HDFS_URL), new Configuration());

        download(fs, HDFS_KEY_TABLE_PATH, LOCAL_KEYTAB);
        download(fs, HDFS_KRB5_PATH, LOCAL_KRB);

        fs.close();
    }

    private static void download(FileSystem fs, String srcP, String ou) throws IOException {
        final Path src = new Path(srcP);
        if (!fs.exists(src)) {
            throw new IOException(srcP + "不存在");
        }
        fs.copyToLocalFile(false, src, new Path(ou), true);
    }
}
