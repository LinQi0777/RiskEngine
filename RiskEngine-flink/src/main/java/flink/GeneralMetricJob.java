package flink;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

/* **********************
 *
 * 为什么需要指标的通用聚合计算框架？
 *
 * 1. 每个指标都需要有对应的聚合计算Job, Job的数量就会很多,
 * 2. Kafka的每条行为数据，会被多个Job重复消费。
 * 3. 指标的聚合计算的流程大体相同，每个Job的重复代码很多, 不易维护
 * 4. 通用聚合计算框架只是参数的不同, 运营人员可以在后台配置这些参数,
 *    例如按照哪些字段分组，按照什么时间字段进行窗口计算等等，
 *    从而可以根据后台配置的参数自动的生成指标计算Job
 *
 *
 * 指标的通用聚合计算框架的设计思路
 * 1. 通过运营后台配置指标的聚合计算的规则,
 * 2. 每个Job只需要调用通用的模块, 这些通用模块去读取运营后台对应指标的配置和计算规则, 然后进行聚合计算
 *
 *
 * Flink在什么地方去读取指标的聚合计算的规则 ?
 * 答案：在 FlatMap算子里的open()进行读取, 把读取到的聚合计算的规则, 写入到数据流 (DataStream<EventPO>)
 *
 *
 * Flink读取指标的聚合计算的规则是一次性读取, 还是多次读取 ?
 * 答案：多次读取。通过一个单独的线程去定时的读取。
 *
 *
 * *********************/


public class GeneralMetricJob {

    public static void main(String[] args) throws Exception {

        /* **********************
         *
         * Flink配置 (获取配置文件以及任务提交参数)
         *
         * *********************/
        //参数工具对象
        ParameterTool tool = ParameterUtil.getParameters(args);


        SingleOutputStreamOperator<Tuple2<EventPO,Double>> dataStream =
        KafkaUtil
                //读取Kafka
                .read(tool)
                /* **********************
                 *
                 * 知识点：
                 *
                 * 1.
                 * Flink 1.12 之前的水印生成策略只有2种：
                 * a. AssignerWithPunctuatedWatermarks
                 * b. AssignerWithPeriodicWatermarks
                 *
                 * 要使用不同的水印生成,就要实现不同的接口,
                 * 这样会造成代码的重复以及变得复杂,
                 *
                 * 所以 Flink 1.12 对水印生成进行了重构
                 *
                 * 2.
                 *
                 * Flink 1.12 后是使用 assignTimestampsAndWatermarks() 生成水印,
                 * assignTimestampsAndWatermarks()的参数是 WatermarkStrategy 对象,
                 *
                 * 3.
                 *
                 * WatermarkStrategy 对象是什么？
                 * WatermarkStrategy 对象提供了很多的静态方法很方便的生成水印,
                 * 也提供了 createWatermarkGenerator(),
                 * 可以自定义水印的生成。
                 *
                 * 4.
                 *
                 * WatermarkStrategy 对象必须包含：
                 * a. 水印生成策略
                 * b. 水印时间的生成 (从事件流提取)
                 *
                 * 4.
                 * 水印生成策略, 课程使用的是 固定延迟生成水印 策略,
                 * 为什么使用这个策略 ?
                 * 这个策略可以对数据的延迟时间有一个大概的预估,
                 * 在这个预估值内,就可以等到所有的延迟数据
                 *
                 * 5.
                 * 通过 WatermarkStrategy 对象的静态方法 forBoundedOutOfOrderness(),
                 * 就可以生成 固定延迟生成水印 策略,
                 * 需要传入 Duration 类型的时间间隔
                 *
                 *
                 *
                 * *********************/

                //注册水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //水印生成器: 实现一个延迟10秒的固定延迟水印
                                .<EventPO>forBoundedOutOfOrderness(Duration.ofMillis(tool.getInt(ParameterConstantsUtil.FLINK_MAXOUTOFORDERNESS) * 1000L))
                                //时间戳生成器：提取事件流的event_time字段
                                .withTimestampAssigner(new MetricTimestampAssigner())
                )
                // 读取Mysql, 获取指标聚合计算的规则写入到事件流
                .flatMap(new MetricConfFlatMap())
                //根据指标聚合计算的规则过滤出计算所需要的行为事件
                .filter(new MetricFilter())
                //根据指标聚合计算的规则分组
                .keyBy(new MetricKeyBy())
                /* **********************
                 *
                 * 注意：
                 *
                 * 因为指标计算的时间是最近的xx时间段,
                 * 所以窗口自定义只限定为滑动窗口,
                 *
                 * *********************/
                //根据指标聚合计算的规则指定计算窗口类型,大小,步长
                //以及将每条行为事件分配到对应的窗口中
                .window(new MetricWindowAssigner())
                //根据指标聚合计算的规则指定计算触发器
                //.trigger(new MetricTrigger())

                /* **********************
                 *
                 * 知识点：
                 *
                 * 1.
                 * 聚合计算算子 为什么用 aggregate(), 不用 process() ?
                 * a.
                 * aggregate() 可以调用4种计算API,
                 * process() 只能调用1种计算API, 但可以调用定时器等的方法, 如统计TopN,会用到
                 * b.
                 * aggregate() 累加计算
                 * process() 全量计算 TopN计算
                 *
                 *
                 * 2.
                 * aggregate() 计算过程：
                 * 每进入窗口一条数据, 就聚合计算一次, 直到窗口结束,
                 * 当窗口触发的时候, 输出结果
                 *
                 *
                 * 3.
                 * 为什么 aggregate() 要2个入参 ?
                 *
                 * 若只有1个入参,即入参是 AggregateFunction 对象
                 * 则 AggregateFunction 只能获取到当前数据,
                 * 若要获取窗口信息, 需要缓存窗口的所有数据
                 *
                 * 若2个入参, 即入参是 AggregateFunction 对象和 WindowFunction 对象
                 * 则 WindowFunction 不但可以获取 AggregateFunction 的计算输出,
                 * 还能获取 keyBy 的 key, 以及窗口信息,如窗口大小
                 *
                 *
                 * *********************/

                /* **********************
                 *
                 * 注意：
                 *
                 * 项目中的风控指标计算都是累加计算 和 平均计算,
                 * 所以只自定义了 累加计算逻辑 和 平均计算逻辑,
                 *
                 *
                 * *********************/

                //根据指标聚合计算的规则进行增量聚合计算
                .aggregate(new MetricAggFunction(), new MetricWindowFunction());

        /* **********************
         *
         * 为什么要写入 Redis ?
         * 因为Flink这里只是计算了最近1小时的登录次数,
         * 但如果指标需要最近2小时的登录次数呢? 最近3小时的登录次数呢?
         * 如何快速获取？难道要Flink再重新计算吗？
         *
         * 思路：
         * 在Redis将3个最近1小时的登录次数相加,
         * 不就可以得到最近3小时的登录次数,
         * 如此类推。
         *
         * 要做到这一步关键的就是 Redis key 的设计,
         * 这个key必须能够快速的定位到 最近1小时的登录次数
         *
         * 所以 Redis key 的格式为：
         * 指标id+uid+指标主维度+计算方式+指标版本号+编号
         *
         * 编号是指
         * 哪一个的 "最近1小时的登录次数" 才是 最近1小时,
         * 因为随着时间的推移,
         * 当前的 "最近1小时",
         * 相对于未来并不是 "最近1小时"
         *
         * 那么如何根据编号,在读取指标值的现在这一时间,
         * 距离这个当前时间的"最近1小时"的指标值是哪一个,
         * 因为Redis上存放了很多"最近1小时"的指标值,
         * 这是一个难点。
         *
         *
         * *********************/

        /* **********************
         *
         * 注意：
         *
         * 项目中的风控指标计算结果限定写入Redis,
         * 不实现写入ClickHouse的逻辑
         *
         *
         * *********************/

        //组装 Redis Key
        DataStream<Tuple2<String,String>> redisKeyStream =
                dataStream.map(new MetricMapForRedisKey());
        //写入Redis
        RedisWriteUtil.writeByBahirWithString(redisKeyStream);

        KafkaUtil.env.execute();


        /* **********************
         *
         * 知识扩展：
         *
         * 一般做法,
         * 1个 Job 对应1个 jar包,
         * 有多少 Job 就有多少的 jar 包。
         * 即使使用聚合计算的通用框架也是这样子,
         * 通用框架也只是把重复的代码封装起来,
         *
         * 这样子,就会有很多 jar 包,但这不是最主要的,
         * 主要是每个 Job 的启动都会耗费资源,
         * 并且这些 Job 都是业务逻辑基本相同的指标计算,
         * 有没有什么方法能合并这些 Job ?
         *
         * 2种方案：
         * 1. 在提交的时候,将多个Job合并
         * 优点：
         * 这些作业是使用同一份代码,
         * 例如聚合计算的通用框架,只是参数不同
         * 将这些作业合并到1个Job执行,
         * 可以节省启动的资源开销
         * 缺点：
         * 需要修改Flink源码,难度比较大
         *
         * 2.
         * 通过工厂设计模式调用对应的Job类
         *
         *
         * *********************/

    }
}
