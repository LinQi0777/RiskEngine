package flink.utils;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class JoinUtil {

/**
* @Author: 123
* @Description: intervalJoinStream
* @DateTime: 2024
*/
    public static <A,B,Out> SingleOutputStreamOperator<Out> intervalJoinStream(
            KeyedStream<A,String> a,
            KeyedStream<B,String> b,
            int lowerBound,
            int upperBound,
            ProcessJoinFunction<A,B,Out> processJoinFunction
    ) {
        /* **********************
         *
         * 知识点：
         *
         * 事件流Join痛点：
         * a. 事件流没有边界,
         * b. 两个事件流的数据不是同步的, 数据会延迟
         *
         * 1.
         * 双流Join方案
         * a. join
         * b. coGroup
         * c. intervalJoin
         *
         * 2.
         * join 和 coGroup
         * 区别：
         * a.
         * join: join().where().equalTo().window().apply(JoinFunction)
         * coGroup: coGroup().where().equalTo().window().apply(CoGroupFunction)
         * b.
         * CoGroupFunction 比 JoinFunction 更加灵活
         * c.
         * join: 如果一个流中的元素在另一个流中没有相对应的元素，则不会输出该元素
         * coGroup 能自定义没有能够join的数据的输出格式
         * 共同点：
         * a.
         * 必须对两个流数据划分相同的窗口，在同一个窗口中，进行数据的连接
         *
         * 3.
         * join 和 coGroup 痛点：
         * 只能实现在同一个窗口的两个数据流进行join,
         * 但是在实际中常常会存在数据乱序或者延时的情况，
         * 导致两个流的数据进度不一致,
         * 那么数据就无法在同一个窗口内join
         *
         * 4.
         * intervalJoin:
         * 是基于KeyedStream,
         * 是连接两个 KeyedStream,
         * 按照相同的key在一个相对数据时间的时间段内进行连接
         *
         * 5.
         * intervalJoin 不需要开窗，但是需要指定偏移区间的上下界,
         * 且只支持事件时间
         *
         * intervalJoin between()中的上界和下界
         * B流 比 A流 晚 1~5秒
         * A.time + 1 <= B.time <= A.time + 5
         * or
         * B.time -1 <= A.time <= B.time - 5
         *
         * A.intervalJoin(B).between(Time.second(1), Time.second(5))
         *
         *
         * 相当于左边的流可以晚到lowerBound（lowerBound为负的话）时间，也可以早到upperBound（upperBound为正的话）时间
         *
         *
         * *********************/



        return
                a.intervalJoin(b)
                        // a流去b流1~5之前去找数据
                        .between(Time.milliseconds(lowerBound), Time.milliseconds(upperBound))
                        .process(processJoinFunction);
    }
}


