package flink.utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.runtime.function.AbstractFunction;

/**
* @Author: 123
* @Description: AviatorUtil
* @DateTime: 2025/2/22
*/

public class AviatorUtil {

/**
* @Author: 123
* @Description: execute
* @DateTime: 2024
*/
    public static Object execute(String str) {

        //执行AviatorEvaluator 对象的 execute(),获取字符串表达式运算后结果
        return AviatorEvaluator.execute(str);

    }

 /**
 * @Author: 123
 * @Description:
 * @DateTime: 2024
 */
//    public static Object execute(
//            String str,
//            Map<String,Object> map) {
//
//        //将字符串表达式解析为 Expression 对象
//        Expression compileExp = AviatorEvaluator.compile(str,true);
//        //执行Expression 对象的 execute(),获取字符串表达式运算后结果
//        return compileExp.execute(map);
//
//    }

/**
* @Author: 123
* @Description: execute
* @DateTime: 2024
*/
    public static Object execute(
            String str,
            AbstractFunction func) {

        //注册自定义函数
        AviatorEvaluator.addFunction(func);
        //将字符串表达式解析为 Expression 对象
        Expression compileExp = AviatorEvaluator.compile(str,true);
        //执行Expression 对象的 execute(),获取字符串表达式运算后结果
        return compileExp.execute();

    }

}
