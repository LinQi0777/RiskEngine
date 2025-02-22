package flink.redis.conf;

import lombok.Getter;

/**
* @Author: 123
* @Description: JRedisCommand
* @DateTime: 2025/2/22 15:23
*/

@Getter
public enum JRedisCommand {

    GET(JRedisDataType.STRING);

    private JRedisDataType jRedisDataType;

    JRedisCommand(JRedisDataType jRedisDataType) {
        this.jRedisDataType = jRedisDataType;
    }
}
