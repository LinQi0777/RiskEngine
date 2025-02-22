package flink;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

public class RuleJudgeJob {
    public static void main(String[] args) throws Exception {
        /* **********************
         *
         * Flink配置 (获取配置文件以及任务提交参数)
         *
         * *********************/
        //参数工具对象
        ParameterTool tool = ParameterUtil.getParameters(args);
        //获取任务提交参数传入的规则组唯一编码
        String set_code = tool.get(ConstantsUtil.ARGS_SET_CODE);
        //Properties属性对象 (Jdbc和debezium配置)
        Properties p = new Properties();

        /* **********************
         *
         * 消费Kafka, 生成行为事件流,并按照用户ID分组
         *
         * *********************/
        //消费Kafka
        DataStream<EventPO> eventStream = KafkaUtil.read(tool);
        //分组
        KeyedStream<EventPO, Integer> keyedStream = eventStream.keyBy(EventPO::getUser_id_int);
        //env
        StreamExecutionEnvironment env = KafkaUtil.env;

        /* **********************
         *
         * Flink-CDC 监听原子规则表
         *
         * *********************/
        //表名
        String ruleTableN = ConstantsUtil.TABLE_NAME_RULE;
        //数据源名称
        String ruleSourceN = ParameterConstantsUtil.FLINK_CDC_SINGLE_RULE_SOURCE_NAME;
        DataStream<SingleRulePO> ruleStream = DataStreamUtil.buildMysqlCDCStream(env,tool,p,ruleTableN,new RuleDebeziumDeserializer(),ruleSourceN,"1",new SingleRuleSerializableTimestampAssigner());

        /* **********************
         *
         * Flink-CDC 监听规则组表
         *
         * *********************/
        //表名
        String rulesTableN = ConstantsUtil.TABLE_NAME_RULE_SET;
        //数据源名称
        String rulesSourceN = ParameterConstantsUtil.FLINK_CDC_RULES_SOURCE_NAME;
        DataStream<RulesPO> rulesStream = DataStreamUtil.buildMysqlCDCStream(env,tool,p,rulesTableN,new RulesDebeziumDeserializer(),rulesSourceN,"2",new RulesSerializableTimestampAssigner());


        /* **********************
         *
         * 原子规则 和 规则组 双流Join
         *
         * *********************/
        //以规则流的 rule_code 作为Join的key
        KeyedStream<SingleRulePO, String> ruleKeyedStream = ruleStream.keyBy(SingleRulePO::getRule_code);
        //以规则组流的 rule_code 作为Join的key
        KeyedStream<RulesPO, String> rulesKeyedStream = rulesStream.keyBy(RulesPO::getRule_code);
        //使用 interval Join
        DataStream<RulesPO> joinStream = JoinUtil.intervalJoinStream(
                rulesKeyedStream,
                ruleKeyedStream,
                //数值根据实际情况调整
                -5,5,
                new RulesProcessJoinFunction());

        /* **********************
         *
         * 规则广播流合并行为事件流,并将规则组写入行为事件
         *
         * *********************/
        //合并后的事件流 (携带了指定的规则组)
        SingleOutputStreamOperator<EventPO> theRulesStream = RuleUtil.doRuleBroadcastStream(env,tool,joinStream,keyedStream,set_code);
        /* **********************
         *
         * 循环规则组, 将规则写入匹配的行为事件 (根据event_name匹配), 会产生冗余行为事件数据
         * 因为同一条行为事件, 会适配多个规则,
         *
         * *********************/
        SingleOutputStreamOperator<EventPO> eventRuleStream = theRulesStream.flatMap(new RulesFlatMap());

        /* **********************
         *
         * 基于Aviator进行规则判断
         *
         * *********************/
        SingleOutputStreamOperator<ActionPO> actionStream =
                eventRuleStream
                    .keyBy(EventPO::getUser_id_int)
                    //若需要窗口聚合, 在此处配置窗口聚合
                    //.window()
                    .process(new WarningKeyedProcessFunction());
        /* **********************
         *
         * 规则命中后告警动作
         * (项目中只将风险用户打印出来)
         *
         * *********************/
        //TODO

        env.execute();
    }
}
