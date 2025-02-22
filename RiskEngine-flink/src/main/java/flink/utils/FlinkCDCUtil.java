package flink.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * author: 123
 * description: Flink CDC 工具类
 * date: 2023
 */

/* **********************
 *
 *
 * 知识点：
 *
 * 1.
 * Flink-CDC 应用场景：
 * a. 实时捕获 Mysql 数据变更
 * b. Mysql 数据同步
 *
 * 2.
 * CDC：捕获数据变更
 * Debezium: 将数据库转化为事件流的分布式服务, 可以捕获数据库中表的每一行的数据更改
 *
 * 3.
 * Flink-CDC 底层原理
 * a. Flink-CDC 底层封装了Debezium
 * b. Debezium 可以将数据库转化为事件流
 * c. Flink 消费 Debezium转化的事件流, 就可以捕获数据库中表的每一行的数据更改
 * d. Debezium 是读取 Mysql binlog 日志 , 再转化为事件流
 * e. Debezium 在读取 Mysql binlog 日志时, 是有全局锁
 *
 * 4.
 * Flink-CDC 和 Canal 同步方案的对比：
 * a. Canal 只针对Mysql,
 * b. Canal 只支持增量同步,
 * c. Canal 全量同步要借助于 DataX 或 Sqoop
 * d. Canal 不支持分布式
 *
 * 5.
 * Flink-CDC 读取 Mysql binlog 日志,
 * Mysql binlog 日志格式要设置为 Row,
 * 因为 Flink-CDC 是伪装为slave去拉取binlog,
 * 但并不具备sql执行引擎,
 * 所以要将binlog 日志格式要设置为 Row
 *
 * binlog Row 模式是只记录了哪一条数据被修改了, 修改之后的样子,
 * 但不会记录数据修改的sql执行的上下文信息
 *
 *
 * *********************/

public class FlinkCDCUtil {


    /**
     * 获取MySQL CDC数据源
     *
     * @param parameterTool 参数工具
     * @param p Properties 属性
     * @param tableList 需要监听的表
     * @return MySqlSource<T> Mysql CDC数据源
     */
    public static <T> MySqlSource<T> getMysqlCDCSource(
            ParameterTool parameterTool,
            Properties p,
            DebeziumDeserializationSchema<T> deserializer,
            String serverId,
            String... tableList
            ) {

        /* **********************
         *
         * 注意：
         *
         * 若 Mysql 开启了 SSL 加密，
         * 则需要配置
         * jdbc.properties.useSSL=true
         * debezium.database.ssl.mode=required
         *
         * 如
         * Properties p = new Properties()
         * p.setProperty("jdbc.properties.useSSL","true");
         * p.setProperty("debezium.database.ssl.mode","required");
         *
         * *********************/

        return MySqlSource.<T>builder()
                .serverTimeZone("Asia/Shanghai")
                .hostname(parameterTool.get(ParameterConstantsUtil.Mysql_HOST))
                .port(parameterTool.getInt(ParameterConstantsUtil.Mysql_PORT))
                .username(parameterTool.get(ParameterConstantsUtil.Mysql_USERNAME))
                .password(parameterTool.get(ParameterConstantsUtil.Mysql_PASSWD))
                //binlog读取起始位置
                /* **********************
                 *
                 * 知识点：
                 *
                 * 6.
                 * StartupOptions 值：
                 * a. initial ：
                 *    Flink程序首次启动, 全量读取,
                 *    然后增量读取
                 * b. earliest： 从第一行读取
                 * c. latest： 读取最新的数据, 这里的最新是指从Flink程序启动后
                 * d. timestamp ： 指定时间
                 * e. specificOffset： 指定offset
                 *
                 * 注意：
                 * Mysql-CDC-Connector 2.2 startupOptions只支持 initial, latest
                 *
                 * *********************/
                .startupOptions(StartupOptions.initial())
                //是否扫描新增表 (Flink-CDC 2.2+ 才支持)
                .scanNewlyAddedTableEnabled(true)
                //监听的表所在的库
                /* **********************
                 *
                 * 注意：
                 *
                 * databaseList() 是不定参数
                 *
                 * *********************/
                .databaseList(parameterTool.get(ParameterConstantsUtil.FLINK_CDC_MYSQL_DATABASE))
                //需要监听的表,多个表用逗号隔开
                /* **********************
                 *
                 * 注意：
                 *
                 * 1. tableList() 是不定参数
                 * 2. 表名必须加上库名  "j.mysql_test","j2.mysql_tst2"
                 *
                 * *********************/
                .tableList(tableList)
                //jdbc配置
                .jdbcProperties(p)
                //debezium配置
                .debeziumProperties(p)
                //序列化为Json String
                /* **********************
                 *
                 * 若自定义反序列化,
                 * 实现接口DebeziumDeserializationSchema,
                 * 重写deserialize
                 *
                 * *********************/
                .deserializer(deserializer)
                //标识 MySQL 数据库的唯一标识
                /* **********************
                 *
                 * 当多个 Flink CDC 实例连接到同一个 MySQL 数据库时，
                 * 每个实例都需要使用不同的 server-id 来避免冲突
                 *
                 * *********************/
                .serverId(serverId)
                .build();

    }


    /**
     * 建立Flink-CDC mysql虚拟表的ddl语句
     * @param parameterTool 参数工具
     * @param tableName 需要同步的表名
     * @param databaseName 数据库名
     * @param ddlString 表的定义语句
     * @param jdbcPropertiesString JDBC连接参数
     */
    public static String buildMysqlCdcDdl(
            ParameterTool parameterTool,
            String tableName,
            String databaseName,
            String ddlString,
            String jdbcPropertiesString
    ) {

        // 从参数工具中获取mysql连接的用户名
        String username = parameterTool.get(ParameterConstantsUtil.Mysql_USERNAME);
        // 从参数工具中获取mysql连接的密码
        String passwd = parameterTool.get(ParameterConstantsUtil.Mysql_PASSWD);
        String hostname = parameterTool.get(ParameterConstantsUtil.Mysql_HOST);
        String port = parameterTool.get(ParameterConstantsUtil.Mysql_PORT);


        /* **********************
         *
         *
         * 注意：
         *
         * Flink-CDC 的 mysql DDL 格式
         *
         *
         * *********************/

        //编写DDL ( 数据定义语言 )
        return  "" +
                "CREATE TABLE IF NOT EXISTS " +
                tableName +
                " (\n" +
                ddlString +
                ")" +
                " WITH (\n" +
                jdbcPropertiesString +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = '" + hostname + "',\n" +
                "'port' = '" + port + "',\n" +
                "'username' = '" + username + "',\n" +
                "'password' = '" + passwd + "',\n" +
                "'database-name' = '" + databaseName + "',\n" +
                "'table-name' = '" + tableName + "'\n" +
                ")";

    }


    public static Map<String, String>  deserialize(
            Struct value
    ) {

        // sourceRecord携带的数据，是Struct类型
        /* **********************
         *
         * 注意：
         * Struct对象是
         * com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data 包
         *
         * 因为
         * Flink CDC 的 shaded 使用了 Kafka Connect 类库
         *
         * *********************/

        // 获取变更后的数据，名称after
        Struct after = value.getStruct("after");

        Map<String, String> afterMap = new HashMap<>();
        //处理after数据
        if (null != after) {

            // 获取Struct中包含所有字段名，遍历即可
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                String k = field.name();
                String v = after.get(field).toString();
                afterMap.put(k,v);
            }
        }

        //获取 ts_ms 时间戳
        Long ts_ms = (Long)value.get("ts_ms");
        afterMap.put("ts_ms",ts_ms.toString());


        return afterMap;
    }

}
