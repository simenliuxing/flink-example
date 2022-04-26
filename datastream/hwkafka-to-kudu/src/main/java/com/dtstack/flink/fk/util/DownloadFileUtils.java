/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.fk.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author chuixue
 * @create 2022-04-26 16:30
 * @description
 **/
public class DownloadFileUtils {

    public static final String HDFS_KEY_TABLE_PATH = "/Users/chuixue/Desktop/kafka.keytab";
    public static final String HDFS_KRB5_PATH = "/Users/chuixue/Desktop/krb5.conf";

    public static final String DIR = System.getProperty("user.dir") + "/datastream/hwkafka-to-kudu/src/main/resources/kerberos/";

    public static String LOCAL_KEYTAB = DIR + "hwkafka.keytab";
    public static String LOCAL_KRB = DIR + "hwkrb5.conf";


    public static void downloadFile() throws IOException {
        copyFileUsingStream(new File(HDFS_KEY_TABLE_PATH), new File(LOCAL_KEYTAB));
        copyFileUsingStream(new File(HDFS_KRB5_PATH), new File(LOCAL_KRB));
    }

    public static void copyFileUsingStream(File source, File dest)
            throws IOException {
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (os != null) {
                os.close();
            }
        }
    }
}
