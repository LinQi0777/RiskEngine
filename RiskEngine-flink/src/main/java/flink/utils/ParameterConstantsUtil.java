package flink.utils;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class ParameterConstantsUtil {


    /* **********************
     *
     * Flink 配置参数名称
     *
     * *********************/

    //触发checkpoint时间间隔
    public static final String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    //checkpoint超时
    public static final String FLINK_CHECKPOINT_TIMEOUT = "flink.checkpoint.timeout";
    //checkpoint最小时间间隔
    public static final String FLINK_CHECKPOINT_MINPAUSE = "flink.checkpoint.minPause";
    //checkpoint允许失败次数
    public static final String FLINK_CHECKPOINT_FAILURENUMBER = "flink.checkpoint.failureNumber";
    //同一时间checkpoint数量
    public static final String FLINK_CHECKPOINT_MAXCONCURRENT = "flink.checkpoint.maxConcurrent";
    //并行度
    public static final String FLINK_PARALLELISM = "flink.parallelism";
    //数据延迟的最大时间
    public static final String FLINK_MAXOUTOFORDERNESS = "flink.maxOutOfOrderness";


    /* **********************
     *
     * Kafka 配置参数名称
     *
     * *********************/

    //brokers
    public static final String KAFKA_BROKERS = "kafka.brokers";
    //topic
    public static final String KAFKA_TOPIC = "kafka.topic";
    //group
    public static final String KAFKA_GROUP = "kafka.group";


    /* **********************
     *
     * Mysql 配置参数名称
     *
     * *********************/

    //url
    public static final String Mysql_URL = "mysql.url";
    //username
    public static final String Mysql_USERNAME = "mysql.username";
    //passwd
    public static final String Mysql_PASSWD = "mysql.passwd";
    //port
    public static final String Mysql_PORT = "mysql.port";
    //host
    public static final String Mysql_HOST = "mysql.host";
    //driver
    public static final String Mysql_DRIVER = "mysql.driver";

    //Flink CDC监听的表1
    public static final String FLINK_CDC_MYSQL_TABLE_LIST_1 = "flink.cdc.mysql.table.list.1";
    // Flink CDC监听的表所在的库
    public static final String FLINK_CDC_MYSQL_DATABASE = "flink.cdc.mysql.database";
    // Flink CDC监听的原子规则表的源名称
    public static final String FLINK_CDC_SINGLE_RULE_SOURCE_NAME = "flink.cdc.singleRule.sourceName";
    // Flink CDC监听的规则表的源名称
    public static final String FLINK_CDC_RULES_SOURCE_NAME = "flink.cdc.rules.sourceName";



    /* **********************
     *
     * Flink环境 配置参数名称
     *
     * *********************/

    //当前环境
    public static final String FLINK_ENV_ACTIVE = "flink.env.active";
    /* **********************
     *
     * Flink 配置文件
     *
     * *********************/
    //根配置文件
    public static final String FLINK_ROOT_FILE = "flink.properties";
    //不同环境配置文件
    public static final String FLINK_ENV_FILE = "flink-%s.properties";

    /* **********************
     *
     * 包完整路径
     *
     * *********************/
    //累加器计算方法模型包路径
    public static final String PACKAGE_AGGREGATE_ACC = "com.j.RiskCtrlSys.flink.job.aggregate.acc";


}
