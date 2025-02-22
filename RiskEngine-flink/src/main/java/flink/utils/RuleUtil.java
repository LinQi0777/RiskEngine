package flink.utils;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class RuleUtil {

    public static <T> MapStateDescriptor<String, T> getMapState(String name,Class<T> clazz) {
            return new MapStateDescriptor<String, T>(
                    name,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(clazz)
            );
        }



/**
* @Author: 123
* @Description: traversalRoster
* @DateTime: 2024
*/
    public static boolean traversalRoster(
            String rosterType,
            Integer uid) {

        /* **********************
         *
         * 名单判断在 Redis 的 集合(set) 中进行
         *
         * *********************/

        //TODO

        return false;
    }


 /**
 * @Author: 123
 * @Description: traversalRules
 * @DateTime: 2024
 */
    public static boolean traversalRules(
            String event_name,
            RulesPO rules) {

        //先假设规则组内规则全部命中
        boolean judgeTag = true;

        //遍历规则组
        for(SingleRulePO rule:rules.getRules()) {

            //根据 event_name 匹配对应的规则判断
            String[] eventNames = rule.getEvent_name().split(",");
            for(String eventName:eventNames) {
                if (eventName.equals(event_name)) {
                    //规则判断逻辑
                    if (!ruleJudgment(rule)) {
                        judgeTag = false;
                    }
                }
            }
        }

        //若规则组内规则全部命中执行策略
        if(judgeTag) {

            //TODO 执行风控策略
        }

        //返回规则组内规则是否全部命中的判断
        return judgeTag;
    }

/**
* @Author: 123
* @Description: ruleJudgment
* @DateTime: 2024
*/
    public static boolean ruleJudgment(SingleRulePO rule) {

        /* **********************
         *
         * 规则判断业务逻辑就是
         * 规则的指标值和风控阈值的逻辑判断
         *
         * *********************/

        //TODO 规则判断逻辑

        return true;
    }

/**
* @Author: 123
* @Description: doRuleBroadcastStream
* @DateTime: 2024
*/
    public static SingleOutputStreamOperator<EventPO> doRuleBroadcastStream(
            StreamExecutionEnvironment env,
            ParameterTool parameterTool,
            DataStream<RulesPO> rulesStream,
            KeyedStream<EventPO, Integer> keyedStream,
            String set_code
    ) throws Exception{

        //建立MapState解析器
        //key 是规则组的唯一编码
        MapStateDescriptor<String, RulesPO>mapState = getMapState("rulesBroadcastState",RulesPO.class);


        //转化为广播流
        BroadcastStream<RulesPO> broadcastStream = DataStreamUtil.broadcastStreamBuilder(
                        rulesStream,
                        mapState
                );

        //流合并
        BroadcastConnectedStream<EventPO, RulesPO> connectStream  =DataStreamUtil.streamConnect(keyedStream,broadcastStream);

        //将规则组写入行为事件
        return DataStreamUtil.processFuncWithKey(
                                connectStream,
                                new RulesKeyedBroadcastProcessFunc(mapState,set_code)
                                );
    }


    /**
     * author: 123
     * description: 生成cep pattern广播流
     * @param env:
     * @param parameterTool:
     * @param rulesStream:
     * @param keyedStream:
     * @param set_code:
     * @return org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<com.RiskCtrlSys.model.EventPO>
     */
    public static SingleOutputStreamOperator<EventPO> doPatternBroadcastStream(
            StreamExecutionEnvironment env,
            ParameterTool parameterTool,
            DataStream<RedisPO> rulesStream,
            KeyedStream<EventPO, Integer> keyedStream,
            String set_code
    ) throws Exception{

        //建立MapState解析器
        //key 是规则组的唯一编码
        MapStateDescriptor<String, RedisPO>mapState = getMapState("cepBroadcastState",RedisPO.class);


        //转化为广播流
        BroadcastStream<RedisPO> broadcastStream = DataStreamUtil.broadcastStreamBuilder(
                rulesStream,
                mapState
        );

        //流合并
        BroadcastConnectedStream<EventPO, RedisPO> connectStream  =DataStreamUtil.streamConnect(keyedStream,broadcastStream);

        //将规则组写入行为事件
        return DataStreamUtil.processFuncWithKey(
                connectStream,
                new CepKeyedBroadcastProcessFunc(mapState,set_code)
        );
    }

}
