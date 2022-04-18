package com.dtstack.flink.fk.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
/**
 * 全局配置工具
 *
 * @author beifeng
 */
public class GlobalConfig {

  private static final Config CONFIG = ConfigFactory.load("application.conf");

  /** kafka相关配置 */
  public static String bootstrapServers() {
    return CONFIG.getString("bootstrap.servers");
  }

  public static String groupId() {
    return CONFIG.getString("group.id");
  }

  public static String kks() {
    return CONFIG.getString("key.serializer");
  }

  public static String kkd() {
    return CONFIG.getString("key.deserializer");
  }

  public static String sourceTopic() {
    return CONFIG.getString("kafka.source.topic");
  }

  public static String jaas() {
    return CONFIG.getString("kafka.jaas");
  }

  public static String krb5() {
    return CONFIG.getString("kafka.krb5");
  }

  /** kudu相关 */
  public static String kuduTable() {
    return CONFIG.getString("kudu.sink.table");
  }

  public static String impalaIp() {
    return CONFIG.getString("impala.ip");
  }

  public static String impalaPort() {
    return CONFIG.getString("impala.port");
  }

  public static String impalaDb() {
    return CONFIG.getString("impala.db");
  }

  public static String username() {
    return CONFIG.getString("impala.username");
  }

  private static String impalaUrl() {
    return CONFIG.getString("impala.url");
  }

  public static String password() {
    return CONFIG.getString("impala.password");
  }

  public static String uid() {
    return CONFIG.getString("impala.password");
  }

  public static String pwd() {
    return CONFIG.getString("impala.password");
  }

  public static boolean isLdap() {
    return CONFIG.getBoolean("impala.ldap");
  }

  public static String impalaJdbcUrl() {
    // 如果传入url则直接返回
    String url = impalaUrl();
    if (StringUtils.isNoneBlank(url)) {
      return url;
    }
    // 根据是否开启ldap 来拼接url
    url = "jdbc:impala://" + impalaIp() + ":" + impalaPort() + "/" + impalaDb() + ";AuthMech=3;";
    if (isLdap()) {
      url += "UID=" + uid() + ";PWD=" + pwd();
    }

    return url;
  }

  public static List<String> kuduAddress() {
    return Arrays.stream(CONFIG.getString("kudu.master.address").split(","))
        .collect(Collectors.toList());
  }

  // --------------------------------------------------------------

  public static List<String> pk() {
    return Arrays.stream(CONFIG.getString("kudu.pk").split(",")).collect(Collectors.toList());
  }

  public static boolean isBatch() {
    return CONFIG.getBoolean("sink.batch.mode");
  }


  public static int batchSize() {
    return CONFIG.getInt("sink.batch.size");
  }

  public static boolean isUpper() {
    return CONFIG.getBoolean("filed.upper");
  }
  public static boolean filedNameEqual() {
    return CONFIG.getBoolean("filed.equal");
  }
}
