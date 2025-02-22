package flink.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class RedisReadUtil {

/**
* @Author: 123
* @Description: read
* @DateTime: 2024
*/
    public static DataStream<RedisPO> read(
            StreamExecutionEnvironment env,
            JRedisCommand jRedisCommand,
            String key
            ) {

       return env.addSource(new JRedisSource(jRedisCommand,key));
    }
}
