package flink.redis.conf;

import redis.clients.jedis.JedisCluster;

/**
* @Author: 123
* @Description: Builder
* @DateTime: 2025/2/22
*/

public class JBuilder {

    private JedisCluster jedis = null;

    public JBuilder(JedisCluster jedisCluster) {
        this.jedis = jedisCluster;
    }

    public void close() {
        if (this.jedis != null) {
            this.jedis.close();
        }
    }

    /**
     * author: 123
     * description: Redis的Get方法
     * @param key:  redis key
     * @return java.lang.String
     */
    public String get(String key) {
        return jedis.get(key);
    }
}
