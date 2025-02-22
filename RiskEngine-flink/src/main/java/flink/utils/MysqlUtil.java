package flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class MysqlUtil {


    /**
     * 数据库连接对象
     */
    private static Connection connection = null;
    /**
     * SQL语句对象
     */
    private static PreparedStatement preparedStatement = null;
    /**
     * 结果集对象
     */
    private static ResultSet rs = null;


/**
* @Author: 123
* @Description: readWithTableOrSQLApi
* @DateTime: 2024
*/
    public static <T> DataStream<T> readWithTableOrSQLApi(
            StreamExecutionEnvironment env,
            ParameterTool parameterTool,
            Class<T> clazz,
            String tableName,
            String ddlString,
            String sql

    ) throws Exception {

        /* **********************
         *
         *
         * 知识点：
         *
         * 一。
         *  使用 Table/SQL Api 需要加载的依赖
         *  1. Table/SQL Api 扩展依赖
         *  2. Table/SQL Api 基础依赖
         *  3. Table/SQL Api 和 DataStream Api 交互的依赖
         *  4. Flink Planner 依赖
         *
         *
         * 二。
         *  Table API和SQL Api都是基于Table接口。
         *  使用 Table/SQL Api, 要加载Table Api上下文环境
         *  Table Api上下文环境有3种类型
         *  1. TableEnvironment：只支持Batch作业
         *  2. BatchTableEnvironment：只支持Batch作业
         *  3. StreamTableEnvironment: 支持流计算
         *
         * 三。
         *  Planner（查询处理器）：解析sql、优化sql和执行sql
         *  Flink Plnner的类型：
         *  1. Flink Planner (Old Planner)
         *  2. Blink Planner （Flink 1.14之前需要手动导入依赖）
         *
         *  Blink Planner从Flink 1.11版本开始为Flink-table的默认查询处理器
         *  Blink Planner使得Table Api & SQL 层实现了流批统一
         *
         * 四。
         *  使用 flink-connector-jdbc + mysql-jdbc-driver 操作mysql数据库
         *
         * 五。
         *  使用StreamTableEnvironment注册对应的数据源以及数据表信息
         *
         * 六。
         *  Catalog对象是提供了元数据信息,数据源与数据表的信息则存储在CataLog中
         *
         * //创建Catalog对象
         * new JdbcCatalog(catalog_name, database, username, passwd, url);
         *
         *  Catalog对象是接口
         *  Catalog接口的实现：（Flink 1.14版本之前）
         *  1. PG (PostgresSQL) Catalog
         *  2. HiveCatalog
         *  3. Mysql Catalog (Flink 1.15 才有)
         *
         * 七.
         *  Table 对象转换为 DataStream 对象
         *  通过 toDataStream() 方法，DataStream的数据类型是Row对象
         *
         *
         * *********************/


        // 创建StreamTableEnvironment实例
        StreamTableEnvironment tableEnv = DataStreamUtil.getTableEnv(env);

        //编写DDL ( 数据定义语言 )
        String ddl =buildMysqlDDL(parameterTool,tableName,ddlString);

        //StreamTableEnvironment注册虚拟表
        tableEnv.executeSql(ddl);
        //查询结果是Table对象
        Table table = tableEnv.sqlQuery(sql);
        //将Table对象转换为DataStream对象
        return tableEnv.toDataStream(table, clazz);

    }

    /**
     * 根据参数生成MySQL的DDL语句
     * @param parameterTool 参数工具，用于获取MySQL连接信息
     * @param tableName 要创建的表名
     * @param ddlFieldString 表字段的DDL语句
     * @return 生成的完整的MySQL DDL语句
     */
    public static String buildMysqlDDL(
            ParameterTool parameterTool,
            String tableName,
            String ddlFieldString
    ) {

        // 从参数工具中获取mysql连接的url
        String url = parameterTool.get(ParameterConstantsUtil.Mysql_URL);
        // 从参数工具中获取mysql连接的用户名
        String username = parameterTool.get(ParameterConstantsUtil.Mysql_USERNAME);
        // 从参数工具中获取mysql连接的密码
        String passwd = parameterTool.get(ParameterConstantsUtil.Mysql_PASSWD);
        // 从参数工具中获取MySQL的驱动程序
        String driver = parameterTool.get(ParameterConstantsUtil.Mysql_DRIVER);


        //返回完整的DDL语句
        return  "" +
                "CREATE TABLE IF NOT EXISTS " +
                tableName +
                " (\n" +
                ddlFieldString +
                ")" +
                " WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'driver' = '" + driver + "',\n" +
                "'url' = '" + url + "',\n" +
                "'username' = '" + username + "',\n" +
                "'password' = '" + passwd + "',\n" +
                "'table-name' = '" + tableName + "'\n" +
                ")";
    }


/**
* @Author: 123
* @Description: init
* @DateTime: 2024
*/
    public static Connection init(ParameterTool parameterTool) {

        String _url = parameterTool.get(ParameterConstantsUtil.Mysql_URL);
        String _username = parameterTool.get(ParameterConstantsUtil.Mysql_USERNAME);
        String _passwd = parameterTool.get(ParameterConstantsUtil.Mysql_PASSWD);

        try {
            connection = DriverManager.getConnection(_url, _username, _passwd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

/**
* @Author: 123
* @Description: initPreparedStatement
* @DateTime: 2024
*/
    public static PreparedStatement initPreparedStatement(String sql) {
        try {
            preparedStatement = connection.prepareStatement(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return preparedStatement;
    }


 /**
 * @Author: 123
 * @Description: close
 * @DateTime: 2024
 */
    public static void close() {
        try {
            // 如果preparedStatement不为null，则关闭preparedStatement
            if (preparedStatement != null) {
                preparedStatement.close();
            }

            // 如果connection不为null，则关闭connection
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

/**
* @Author: 123
* @Description: closePreparedStatement
* @DateTime: 2024
*/
    public static void closePreparedStatement() {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

/**
* @Author: 123
* @Description: closeResultSet
* @DateTime: 2024
*/
    public static void closeResultSet() {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

/**
* @Author: 123
* @Description: executeQuery
* @DateTime: 2024
*/
    public static ResultSet executeQuery(PreparedStatement ps) {
        preparedStatement = ps;
        try {
            rs = preparedStatement.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rs;
    }


}
