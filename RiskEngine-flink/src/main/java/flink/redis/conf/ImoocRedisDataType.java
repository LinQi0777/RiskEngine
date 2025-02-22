package flink.redis.conf;

import lombok.Getter;

/**
* @Author: 123
* @Description: JRedisDataType
* @DateTime: 2025/2/22
*/

@Getter
public enum JRedisDataType {

    STRING,
    HASH,
    LIST,
    SET,
    SORTED_SET,
    ;

    JRedisDataType() {
    }
}
