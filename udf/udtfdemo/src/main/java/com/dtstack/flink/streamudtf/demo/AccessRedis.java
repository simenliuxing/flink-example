package com.dtstack.flink.streamudtf.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @author xiuyuan
 */
public class AccessRedis extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(AccessRedis.class);

    private Jedis jedis = null;

    @Override
    public void open(FunctionContext context) {
        try {
            //创建redis的连接
            jedis = new Jedis("ip", 6379);
            // todo 配置Redis的密码
            //jedis.auth();
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException("Connect error!!!");
        }
    }

    @Override
    public void close() {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    /**
     * @param secCode 传入需要进行模糊匹配的值
     */
    public void eval(String secCode) {
        try {
            // 模糊查询表中包含的 传入的股票代码的全部key
            final Set<String> keys = jedis.keys("*" + secCode + "*");
            //遍历所有满足条件的key,取对应的hash值
            for (String key : keys) {
                Row row = Row.of(jedis.hget(key, "s_info_windcode"), jedis.hget(key, "s_info_stockwindcode"));
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
