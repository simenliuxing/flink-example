package com.dtstack.flink.util;


import com.dtstack.flink.pojo.Response;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * 远程接口调用
 *
 * @author beifeng
 */
public class RemoteInvoke {
    private static final ObjectMapper OBJECT_MAPPER;
    private static final Logger LOG = LoggerFactory.getLogger(RemoteInvoke.class);

    static {
        OBJECT_MAPPER = new ObjectMapper();
    }

    public static Response remoteGetRequest(String url) {
        URL connect;
        StringBuilder data = new StringBuilder();
        try {
            connect = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) connect.openConnection();

            setProperty(connection);

            InputStream inputStream = connection.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line;
            while ((line = reader.readLine()) != null) {
                data.append(line);
            }

            reader.close();

            // 如果断开连接则表示不复用
            // inputStream.close();
            // connection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            return OBJECT_MAPPER.readValue(data.toString(), Response.class);
        } catch (Exception e) {
            LOG.error("parse json failed ,json string :{} , cause : {}", data, e.getMessage());
            return null;
        }
    }

    private static void setProperty(HttpURLConnection connection) throws ProtocolException {
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        connection.setReadTimeout(10000);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Connection", "keep-alive");
        connection.setRequestProperty("Host", "localhost:8080");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36");
    }

}
