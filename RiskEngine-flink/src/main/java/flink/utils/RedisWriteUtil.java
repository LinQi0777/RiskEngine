package flink.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

/* **********************
 *
 * 知识点
 * 一：
 *
 * Flink 写入Redis:
 *
 * 1. 继承RichSinkFunction (Flink-Stream)
 * 2. 使用第3方的包 (Apache-Bachir-Flink)
 *
 * 二：
 *
 * Apache-Bachir-Flink 的 Redis-Connector的缺点：
 * 1. 使用Jedis, 没有使用Lettuce
 * 2. 没有对 Flink Table/SQL Api 的支持
 *
 * 三：
 *
 * Flink 读取Redis:
 * 1. 继承RichSourceFunction (实现自定义Source)
 * 2. 继承RichParallelSourceFunction (实现自定义Source)
 * 3. 实现SourceFunction接口 (实现自定义Source)
 *
 *
 * *********************/


public class RedisWriteUtil {

    /* **********************
     *
     * FlinkJedisClusterConfig: 集群模式
     * FlinkJedisPoolConfig：单机模式
     * FlinkJedisSentinelConfig：哨兵模式
     *
     * *********************/

    //Jedis配置
    private static FlinkJedisClusterConfig JEDIS_CONF = null;

    static {
            ParameterTool parameterTool = ParameterUtil.getParameters();
            String host = parameterTool.get("redis.host");
            String port = parameterTool.get("redis.port");

            /* **********************
             *
             * InetSocketAddress 是Java的套接字
             *
             * *********************/
            InetSocketAddress inetSocketAddress = new InetSocketAddress(host,Integer.parseInt(port));

            Set<InetSocketAddress> set = new HashSet<>();
            set.add(inetSocketAddress);
            JEDIS_CONF = new FlinkJedisClusterConfig
                    .Builder()
                    .setNodes(set)
                    .build();
    }


/**
* @Author: 123
* @Description: writeByBahirWithString
* @DateTime: 2024
*/
    public static void writeByBahirWithString(DataStream<Tuple2<String,String>> input) {
        input.addSink(new RedisSink<>(JEDIS_CONF,new RedisSinkByBahirWithString()));
    }

}
