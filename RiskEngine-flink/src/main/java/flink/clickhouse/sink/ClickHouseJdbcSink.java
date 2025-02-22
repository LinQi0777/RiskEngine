package flink.clickhouse.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
* @Author: 123
* @Description: ClickHouseJdbcSink
* @DateTime: 2025/2/22 15:22
*/

/* **********************
 *
 * 知识点：
 *
 * 一：
 *
 * Flink-jdbc-connector：通过这个连接器，可以以流计算的方式操作数据库（ClickHouse,Mysql）
 *
 * 二：
 *
 * 需要添加的依赖：
 * 1. flink-connector-jdbc依赖
 * 2. 数据库所对应的JDBC驱动
 *
 * *********************/
public class ClickHouseJdbcSink<T> {

    private final SinkFunction<T> sink;
    private final static String NA = "null";

    public ClickHouseJdbcSink(String sql,int batchSize,String url) {

        sink = JdbcSink.sink(
                sql,
                //对sql语句进行预编译
                new ClickHouseJdbcStatementBuilder<T>(),
                //设置批量插入数据
                new JdbcExecutionOptions.Builder().withBatchSize(batchSize).build(),
                //设置ClickHouse连接配置
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .build()
        );

    }

    public SinkFunction<T> getSink() {
        return this.sink;
    }


    /* **********************
     *
     * 知识点：
     *
     * Java 对sql语句处理的两个对象
     *
     * 1. PreparedStatement对象：能够对预编译之后的sql语句进行处理
     * 2. Statement对象：只能对静态的sql语句进行处理
     *
     * *********************/

/**
* @Author: 123
* @Description: setPreparedStatement
* @DateTime: 2025/2/22 15:22
*/
    public static void setPreparedStatement(
            PreparedStatement ps,
            Field[] fields,
            Object object) throws IllegalAccessException, SQLException {

        //遍历 Field[]
        for (int i = 1; i <= fields.length; i++) {
            //取出每个Field实例
            Field field = fields[i - 1];
            //指示反射的对象在使用时应该取消 Java 语言访问检查
            field.setAccessible(true);
            //通过Field实例的get方法返回指定的对象
            Object o = field.get(object);
            if (o == null) {
                ps.setNull(i, 0);
                continue;
            }

            //这里统一设为字符型
            String fieldValue = o.toString();

            //判断字符串是否为null或者空
            /* **********************
             * 知识点：
             * 一。
             * 变量和常量的比较，通常将常量放前，可以避免空指针
             *
             * 二。
             * equals的比较是内容的比较
             * == 是内存地址的比较
             *
             * *********************/
            if (!NA.equals(fieldValue) && !"".equals(fieldValue)) {
                //替换对应位置的占位符
                ps.setObject(i, fieldValue);
            } else {
                ps.setNull(i, 0);
            }
        }
    }

}
