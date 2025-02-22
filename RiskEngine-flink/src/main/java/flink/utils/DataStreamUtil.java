package flink.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Properties;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class DataStreamUtil {

    //流式计算上下文环境
    //设为 final 以及 public
    public static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();


    //参数工具
    private static ParameterTool parameterTool =
            ParameterUtil.getParameters();

    /**
     * 初始化环境的StreamExecutionEnvironment。(重载 initEnv 方法)
     *
     * @return 初始化后的StreamExecutionEnvironment实例。
     */
    public static StreamExecutionEnvironment initEnv() {

        //ParameterTool 注册为 global
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 配置上下文环境
        ParameterUtil.envWithConfig(env,parameterTool);

        return env;
    }

    /**
     * 初始化StreamExecutionEnvironment并返回该环境 (重载 initEnv 方法)
     * @param args 命令行参数
     * @return StreamExecutionEnvironment实例
     */
    public static StreamExecutionEnvironment initEnv(String[] args) {

        //ParameterTool 注册为 global
        parameterTool = ParameterUtil.getParameters(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 配置上下文环境
        ParameterUtil.envWithConfig(env,parameterTool);

        return env;

    }

    /**
     * 获取TableEnv
     * @param env StreamExecutionEnvironment
     * @return StreamTableEnvironment
     */
    public static StreamTableEnvironment getTableEnv(
            StreamExecutionEnvironment env
    ) {

        //创建TableApi运行环境
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance()
                        // Flink 1.14不需要再设置 Planner
                        //.useBlinkPlanner()
                        // 设置流计算模式
                        .inStreamingMode()
                        .build();

        // 创建StreamTableEnvironment实例
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,bsSettings);

        //指定方言 (选择使用SQL语法还是HQL语法)
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        return tableEnv;
    }

    /**
     * 为给定的富源函数创建一个类型为 T 的数据流 (重载 streamBuilder)
     *
     * @param sourceFunc 富源函数对象，用于产生数据流事件
     * @param <T> 数据流元素的类型
     * @return 类型为 T 的数据流
     */
    public static <T> DataStream<T> streamBuilder(
            RichSourceFunction<T> sourceFunc
            ) {

        //初始化流式计算上下文环境
        initEnv();
        //返回类型为 T 的事件流
        return env.addSource(sourceFunc);
    }

    /**
     * 为给定的参数工具和富源函数创建一个类型为 T 的数据流 (重载 streamBuilder)
     *
     * @param args 程序参数数组
     * @param sourceFunc 富源函数对象，用于产生数据流事件
     * @param <T> 数据流元素的类型
     * @return 类型为 T 的数据流
     */
    public static <T> DataStream<T> streamBuilder(
            String[] args,
            RichSourceFunction<T> sourceFunc
    ) {

        //初始化流式计算上下文环境
        initEnv(args);
        //返回类型为 T 的事件流
        return env.addSource(sourceFunc);
    }



 /**
 * @Author: 123
 * @Description: broadcastStreamBuilder
 * @DateTime: 2024
 */
    public static <T,K,V> BroadcastStream<T> broadcastStreamBuilder(
            DataStream<T> dataStream,
            MapStateDescriptor<K,V> mapState
            ){

        /* **********************
         *
         * 知识点：
         *
         * 1.
         * 什么是广播流
         * 广播流是事件流, 是属于低吞吐的事件流
         *
         * 2.
         * 什么时候会使用广播流
         * 将配置,规则 应用到另外一个事件流，
         * 通过广播流，将配置,规则传递到下游Task,
         * 这些下游Task可以将配置,规则保存为 广播状态，
         * 然后将广播状态应用到另外一个事件流
         *
         * 2.
         * 广播状态
         * 什么是广播状态
         * 广播状态是Map结构，K-V结构
         * 广播状态是算子状态( Operator State )的一种
         *
         *
         *
         *
         * *********************/

        return dataStream.broadcast(mapState);
    }


/**
* @Author: 123
* @Description: streamConnect
* @DateTime: 2024
*/
    public static <T,U,KEY> BroadcastConnectedStream<T,U> streamConnect(
            KeyedStream<T,KEY> keyedStream,
            BroadcastStream<U> broadcastStream
    ) {
        return keyedStream.connect(broadcastStream);
    }


/**
* @Author: 123
* @Description: processFuncWithKey
* @DateTime: 2024
*/
    public static <T,U,KEY,OUT> SingleOutputStreamOperator<OUT> processFuncWithKey(
            BroadcastConnectedStream<T,U> broadcastConnectedStream,
            KeyedBroadcastProcessFunction<KEY,T,U,OUT> keyedBroadcastProcessFunction
    ) {
        /* **********************
         *
         * 知识点：
         * 3.
         * Flink 针对不同类型的DataStream (事件流)提供不同的 process(ProcessFunction)
         * a. 普通DataStream调用process(), 入参是 ProcessFunction 实例
         * b. KeyedStream 调用process(), 入参是 KeyedProcessFunction 实例
         *
         * c. BroadcastConnectedStream, 调用process():
         *    c1. 若行为事件流(非广播流), 是按照keyby()分组(KeyedStream),
         *        入参是 KeyedBroadcastProcessFunction 类型
         *
         *    c2. 若行为事件流(非广播流) 没有进行分组,
         *        入参是 BroadcastProcessFunction 类型
         *
         * *********************/

        return broadcastConnectedStream.process(keyedBroadcastProcessFunction);
    }



/**
* @Author: 123
* @Description: buildMysqlCDCStream
* @DateTime: 2024
*/
    public static <T> DataStream<T> buildMysqlCDCStream(
            StreamExecutionEnvironment env,
            ParameterTool parameterTool,
            Properties properties,
            String table_name,
            DebeziumDeserializationSchema<T> deserializer,
            String source_name,
            String serverId,
            SerializableTimestampAssigner<T> serializableTimestampAssigner
    ) {

        //监听的数据库 (这里限定只有一个数据库)
        String cdc_database_name = parameterTool.get(ParameterConstantsUtil.FLINK_CDC_MYSQL_DATABASE);
        //监听的表要要加上库名
        String cdc_table_name = cdc_database_name+"."+table_name;

        // 创建CDC源
        MySqlSource<T> mySqlSource = FlinkCDCUtil.getMysqlCDCSource(
                parameterTool,
                properties,
                deserializer,
                serverId,
                cdc_table_name
        );

        // 从源获取数据生成带有水印的事件流
        return mysqlCDCFromSourceWithWatermark(env,mySqlSource,serializableTimestampAssigner,source_name);

    }

/**
* @Author: 123
* @Description: mysqlCDCFromSourceWithWatermark
* @DateTime: 2024
*/
    public static <T> DataStream<T> mysqlCDCFromSourceWithWatermark(
            StreamExecutionEnvironment env,
            MySqlSource<T> source,
            SerializableTimestampAssigner<T> serializableTimestampAssigner,
            String source_name
    ) {
       return env.fromSource(
               source,
                WatermarkStrategy
                        .<T>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                        .withTimestampAssigner(serializableTimestampAssigner),
                source_name);
    }


/**
* @Author: 123
* @Description: mysqlCDCFromSourceNoWatermark
* @DateTime: 2024
*/
    public static <T> DataStream<T> mysqlCDCFromSourceNoWatermark(
            StreamExecutionEnvironment env,
            MySqlSource<T> source,
            String source_name
    ) {
        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                source_name);
    }


}
