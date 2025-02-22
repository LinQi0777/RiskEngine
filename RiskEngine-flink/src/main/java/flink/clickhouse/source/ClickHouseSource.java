package flink.clickhouse.source;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
* @Author: 123
* @Description: ClickHouseSource
* @DateTime: 2025/2/22 15:22
*/

/* **********************
 *
 * 注意：
 *
 * flink 读取 ch 数据，使用 Clickhouse JDBC的方式读取，
 * Clickhouse JDBC 有两个：
 * 1. ClickHouse 官方提供Clickhouse JDBC。
 * 2. 第3方提供的Clickhouse JDBC。ru.yandex.clickhouse.ClickHouseDriver.
 *
 * ru.yandex.clickhouse.ClickHouseDriver.现在是没有维护
 * ClickHouse 官方提供Clickhouse JDBC的包名：com.clickhouse.jdbc.*,
 * 有些版本com.clickhouse.jdbc.* 包含了 ru.yandex.clickhouse.ClickHouseDriver.
 * 所以在加载包的时候一定要注意导入的包名
 *
 * *********************/

public class ClickHouseSource implements SourceFunction<CHTestPO> {

    private String URL;
    private String SQL;

    public ClickHouseSource(String URL, String SQL) {
        this.URL = URL;
        this.SQL = SQL;
    }

    @Override
    public void run(SourceContext<CHTestPO> output) throws Exception {

        /* **********************
         *
         * 知识点：
         *
         * Properties是持久化的属性集
         * Properties的key和value都是字符串
         *
         * *********************/
        Properties properties = new Properties();
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(URL,properties);

        /* **********************
         *
         * 知识点：
         * 使用 try-with-resoured 方式关闭JDBC连接。
         * 不需要手动关闭
         *
         * *********************/
        try(ClickHouseConnection conn = clickHouseDataSource.getConnection()) {
            /* **********************
             *
             * clickhouse 通过游标的方式读取数据
             *
             * *********************/


            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next()) {
                String name = rs.getString(1);
                output.collect(new CHTestPO(name));
            }
        }

    }

    @Override
    public void cancel() {

    }
}
