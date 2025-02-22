package flink.utils;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
* @Author: 123
* @Description: ClickHouseUtil
* @DateTime: 2024
*/

public class ClickHouseUtil<T> {

    private static String URL = null;

    static {
        ParameterTool parameterTool = ParameterUtil.getParameters();
        URL = parameterTool.get("clickhouse.url");

    }

/**
* @Author: 123
* @Description: read
* @DateTime: 2024
*/
    public static  DataStream<CHTestPO> read(StreamExecutionEnvironment env, String sql) {
        return env.addSource(new ClickHouseSource(URL,sql));
    }


/**
* @Author: 123
* @Description: batchWrite
* @DateTime: 2024
*/
    public static <T> DataStreamSink<T> batchWrite(
            DataStream<T> dataStream,
            String sql,
            int batchSize) {

        //生成 SinkFunction

        ClickHouseJdbcSink<T> clickHouseJdbcSink =
                new ClickHouseJdbcSink<T>(sql,batchSize,URL);

        return dataStream.addSink(clickHouseJdbcSink.getSink());
    }

}
