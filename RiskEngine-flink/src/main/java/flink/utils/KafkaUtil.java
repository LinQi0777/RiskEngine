package flink.utils;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class KafkaUtil {

    //流式计算上下文环境
    //设为 final 以及 public
    public static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static KafkaSource<KafkaMessagePO> KAFKA_SOURCE = null;
    private static ParameterTool parameterTool = null;


/**
* @Author: 123
* @Description: initEnv
* @DateTime: 2024
*/
    private static void initEnv(String[] args) {

        //ParameterTool 注册为 global
        parameterTool = ParameterUtil.getParameters(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 配置上下文环境
        ParameterUtil.envWithConfig(env,parameterTool);
    }
/**
* @Author: 123
* @Description: initEnv
* @DateTime: 2024
*/
    private static void initEnv() {

        //ParameterTool 注册为 global
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 配置上下文环境
        ParameterUtil.envWithConfig(env,parameterTool);
    }

/**
* @Author: 123
* @Description: kafkaSourceBuilder
* @DateTime: 2024
*/
    private static void kafkaSourceBuilder() {

        String brokers = parameterTool.get(ParameterConstantsUtil.KAFKA_BROKERS);
        String topic = parameterTool.get(ParameterConstantsUtil.KAFKA_TOPIC);
        String group = parameterTool.get(ParameterConstantsUtil.KAFKA_GROUP);

        /* **********************
         *
         * 知识点：
         *
         * 1：
         *
         * Flink 提供专门的连接器 (Connector) 读写kafka ( Flink-Kafka-Connector )
         * a. Flink-Kafka-Connector 基于 Flink CheckPoint 的容错机制
         * b. Flink-Kafka-Connector 提供了 Flink 到 Kafka 的端到端的精确一次的语义 ( Exactly-Once )
         *
         * Flink 的精确一次的语义 ( Exactly-Once )：
         * 即使在运行的过程了发生了故障，数据都不会丢失，数据也不会被重复处理。
         * Flink 的精确一次的语义只适用于Flink内部的数据流转。
         *
         * 2：
         *
         * Flink 1.14 之前提供 Flink-Kafka-Connector 所使用主要是 FlinkKafkaConsumer 类，
         *  Flink 1.14 版本 不建议使用 FlinkKafkaConsumer 类，
         * Flink 1.15 版本之后就去掉 Flink-Kafka-Connector 的 FlinkKafkaConsumer 类.
         *
         * *********************/

        KAFKA_SOURCE = KafkaSource.<KafkaMessagePO>builder()
                //bootstrap
                .setBootstrapServers(brokers)
                //主题
                .setTopics(topic)
                //groupid
                .setGroupId(group)
                //偏移量
                .setStartingOffsets(OffsetsInitializer.earliest())
                /* **********************
                 *
                 * 知识点：
                 *
                 * 3：
                 *
                 * Kafka存放的数据是二进制，
                 * 读取Kafka的数据需要反序列化 ( Deserializer )
                 *
                 * 4：
                 *
                 * kafkaSourceBuilder对象提供了2个反序列的方法：
                 *
                 * Kafka数据是一个 Kafka ConsumerRecord对象
                 *
                 * a. setDeserializer(): 完整的反序列化ConsumerRecord对象
                 * b. 反序列化ConsumerRecord对象的value值
                 *
                 * 5：
                 *
                 * DeserializationSchema 是接口：
                 *
                 * SimpleStringScheme类实现了DeserializationSchema接口
                 * SimpleStringScheme类只是将Kafka反序列后的数据转化 DataStream<String>
                 *
                 * *********************/
                .setDeserializer(KafkaRecordDeserializationSchema.of(new JDeserializationSchema()))
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
/**
* @Author: 123
* @Description: makeEventStream
* @DateTime: 2024
*/
    private static DataStream<EventPO> makeEventStream() {
        /* **********************
         *
         * 知识点：
         *
         * 6
         * addSource 和 fromSource 的区别：
         *
         * addSource需要传入的参数是SourceFunction对象，对组件的读取的连接逻辑需要自己实现的。
         * fromSource需要传入的参数是Source对象，Flink帮我们封装好了简易的组件读取的连接逻辑。
         *
         * *********************/
        return env
                .fromSource(
                    KAFKA_SOURCE,
                    WatermarkStrategy.noWatermarks(),
                "Kafka Source")
                //将Kafka消息数据转换为用户事件行为POJO对象
                .map(new KafkaETL());
    }

/**
* @Author: 123
* @Description: read
* @DateTime: 2024
*/
    public static DataStream<EventPO> read(
            String[] args) {

        //初始化流式计算上下文环境
        initEnv(args);
        //初始化KafkaSource
        kafkaSourceBuilder();
        //返回类型为 EventPO 的事件流
        return makeEventStream();
    }

    /**
     * author: 123
     * description: 生成行为事件流 (重载 read 方法)
     * @param parameter:  ParameterTool 对象 (不包含启动参数, 项目中主要用于单元测试)
     * @return org.apache.flink.streaming.api.datastream.DataStream<com.RiskCtrlSys.model.EventPO>
     */
    public static DataStream<EventPO> read(
            ParameterTool parameter) {

        //初始化流式计算上下文环境
        parameterTool = parameter;
        initEnv();
        //初始化KafkaSource
        kafkaSourceBuilder();
        //返回类型为 EventPO 的事件流
        return makeEventStream();
    }

}
