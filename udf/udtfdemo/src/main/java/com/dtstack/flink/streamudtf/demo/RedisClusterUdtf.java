package com.dtstack.flink.streamudtf.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author xiuyuan
 * 在udtf中访问redis cluster 数据库
 */
public class RedisClusterUdtf extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterUdtf.class);

    private JedisCluster cluster = null;

    @Override
    public void open(FunctionContext context) {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            // 最大连接数
            poolConfig.setMaxTotal(2);
            // 最大空闲数
            poolConfig.setMaxIdle(1);
            // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
            // Could not get a resource from the pool
            poolConfig.setMaxWaitMillis(1000);

            Set<HostAndPort> nodes = new LinkedHashSet<>();
            nodes.add(new HostAndPort("ip1", 3164));
            nodes.add(new HostAndPort("ipn", 3123));
            //JedisCluster cluster = new JedisCluster(nodes, 5000, 30, 3, "redis@123", poolConfig);
            JedisCluster cluster = new JedisCluster(nodes, poolConfig);
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException("Connect error!!!");
        }
    }

    @Override
    public void close() {
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public void eval(String secCode) {
        try {
            // 模糊查询表中包含的 传入的股票代码的全部key
            final Set<String> keys = cluster.keys("*" + secCode + "*");
            //遍历所有满足条件的key,取对应的hash值
            for (String key : keys) {
                Row row = Row.of(cluster.hget(key, "s_info_windcode"), cluster.hget(key, "s_info_stockwindcode"));
                collect(row);
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.STRING);
    }

}
