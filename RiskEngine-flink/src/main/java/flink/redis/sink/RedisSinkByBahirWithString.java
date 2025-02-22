package flink.redis.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
* @Author: 123
* @Description:
* @DateTime: 2025
*/

/* **********************
 *
 * redis的数据类型：
 * 1. String
 * 2. Hash
 * 3. List
 * 4. Set
 * 5. z-Set
 *
 * *********************/
public class RedisSinkByBahirWithString implements RedisMapper<Tuple2<String,String>> {

 /**
 * @Author: 123
 * @Description: getCommandDescription
 * @DateTime: 2025
 */
    @Override
    public RedisCommandDescription getCommandDescription() {
        /* **********************
         *
         * 如果Redis的数据类型是 hash 或 z-Set
         * RedisCommandDescription 的构造方法必须传入 additionalKey
         * additionalKey就是Redis的键
         *
         * *********************/
        return new RedisCommandDescription(RedisCommand.SET);
    }

/**
* @Author: 123
* @Description: getKeyFromData
* @DateTime: 2025
*/
    @Override
    public String getKeyFromData(Tuple2<String,String> input) {
        return input.f0;
    }

/**
* @Author: 123
* @Description: getValueFromData
* @DateTime: 2025
*/
    @Override
    public String getValueFromData(Tuple2<String,String> input) {
        return input.f1;
    }
}
