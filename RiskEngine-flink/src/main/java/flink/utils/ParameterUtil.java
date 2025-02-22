package flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;

/**
* @Author: 123
* @Description:
* @DateTime: 2024
*/

/* **********************
 *
 * 知识点：
 *
 * 1.
 *
 * Flink 读取参数的对象：
 *
 * a. Commons-cli: Apache提供的,需要引入依赖
 * b. ParameterTool：Flink内置
 *
 * ParameterTool 比 Commons-cli 使用上简便
 * ParameterTool能避免Jar包的依赖冲突
 *
 * 2.
 * Flink 读取对应环境的配置文件
 * 步骤：
 *
 * a. 获取环境配置变量
 * b. 根据环境配置变量, 读取对应的环境配置文件
 *
 *
 * 5.
 * ParameterTool 获取参数的3种方式：
 * a. fromPropertiesFile 配置文件
 * b. fromArgs 程序启动参数
 *    - 或者 -- 开头 空格分隔, 如：-name Juege --age 21
 * c. fromSystemProperties 系统环境变量, 包括程序 -D启动的变量
 *    内部调用的是 Java提供的 System.getProperties()
 *
 * 6.
 * ParameterTool 获取参数优先级
 * 可通过 mergeWith() 设置优先级, 但 mergeWith() 会覆盖前面的同名变量
 *
 * 7.
 * ParameterTool 是可序列化的, 所以可以将它作为参数传递给函数,
 * 然后在函数内部使用 ParameterTool 获取参数变量,
 * 这样在Flink Job的任何地方都可以通过 ParameterTool 获取到配置值
 *
 * 8.
 * ParameterTool 注册为 global 变量：
 * env.getConfig().setGlobalJobParameter()
 *
 * 这样, 在上下文中就能获取 ParameterTool
 * (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
 *
 *
 *
 * *********************/
public class ParameterUtil {

    /**
     * 默认配置文件
     */
    private static final String DEFAULT_CONFIG = ParameterConstantsUtil.FLINK_ROOT_FILE;
    /**
     * 带环境配置文件
     */
    private static final String FLINK_ENV_FILE = ParameterConstantsUtil.FLINK_ENV_FILE;
    /**
     * 环境变量
     */
    private static final String ENV_ACTIVE = ParameterConstantsUtil.FLINK_ENV_ACTIVE;

/**
* @Author: 123
* @Description: getParameters
* @DateTime: 2024
*/
    public static ParameterTool getParameters(final String[] args) {

        /* **********************
         *
         * 知识点：
         *
         * 3.
         *
         * Java读取资源的方式：
         *
         * a. Class.getResourceAsStream(Path): Path 必须以 “/”，表示从ClassPath的根路径读取资源
         * b. Class.getClassLoader().getResourceAsStream(Path)：Path 无须以 “/”, 默认从ClassPath的根路径读取资源
         *
         * 推荐使用第2种,也就是类加载器的方式获取静态资源文件, 不要通过ClassPath的相对路径查找
         *
         * 4.
         * idea 运行时会将 src/main/resources 的配置文件复制到 target/classes
         *
         * *********************/

        InputStream inputStream = ParameterUtil.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG);

        try {
            //读取根配置文件
            ParameterTool defaultPropertiesFile =
                    ParameterTool.fromPropertiesFile(inputStream);

            //获取环境参数
            String envActive = getEnvActiveValue(defaultPropertiesFile);

            //读取真正的配置环境 (推荐使用 Thread.currentThread() 读取配置文件)

            return ParameterTool
                    // ParameterTool读取变量优先级 系统环境变量>启动参数变量>配置文件变量

                    // 从配置文件获取配置
                    .fromPropertiesFile(
                            //当前线程
                            Thread.currentThread()
                                    //返回该线程的上下文信息, 获取类加载器
                                    .getContextClassLoader()
                                    .getResourceAsStream(envActive))
                    // 从启动参数中获取配置
                    .mergeWith(ParameterTool.fromArgs(args))
                    // 从系统环境变量获取配置
                    .mergeWith(ParameterTool.fromSystemProperties());
        }catch (IOException e) {
            throw new RuntimeException("");
        }

    }


/**
* @Author: 123
* @Description: getParameters
* @DateTime: 2024
*/
    public static ParameterTool getParameters() {


        InputStream inputStream = ParameterUtil.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG);

        /* **********************
         *
         * 注意：
         *
         * ParameterTool 读取配置文件需要抛出 IOException,
         * IOException 的捕捉就在这里 catch
         *
         * 以前代码是直接抛出,没有进行catch,要注意对以前代码的修改
         *
         * *********************/

        try {
            ParameterTool defaultPropertiesFile =
                    ParameterTool.fromPropertiesFile(inputStream);

            //获取环境参数
            String envActive = getEnvActiveValue(defaultPropertiesFile);

            //读取真正的配置环境 (推荐使用 Thread.currentThread() 读取配置文件)
            return ParameterTool
                    // ParameterTool读取变量优先级 系统环境变量>启动参数变量>配置文件变量

                    // 从配置文件获取配置
                    .fromPropertiesFile(
                            //当前线程
                            Thread.currentThread()
                                    //返回该线程的上下文信息, 获取类加载器
                                    .getContextClassLoader()
                                    .getResourceAsStream(envActive))
                    // 从系统环境变量获取配置
                    .mergeWith(ParameterTool.fromSystemProperties());
        }catch (Exception e) {
            throw new FlinkPropertiesException(FlinkPropertiesExceptionInfo.PROPERTIES_NULL);
        }
    }
/**
* @Author: 123
* @Description: getEnvActiveValue
* @DateTime: 2024
*/
    private static String getEnvActiveValue(ParameterTool defaultPropertiesFile) {
        //选择参数环境
        String envActive = null;
        if (defaultPropertiesFile.has(ENV_ACTIVE)) {
            envActive = String.format(FLINK_ENV_FILE, defaultPropertiesFile.get(ENV_ACTIVE));
        }

        return envActive;
    }
/**
* @Author: 123
* @Description: envWithConfig
* @DateTime: 2024
*/
    public static void envWithConfig(
            StreamExecutionEnvironment env,
            ParameterTool parameterTool
            ) {


        //##########  checkpoint 设置 ############//

        /* **********************
         *
         * 注意：
         *
         * 1.
         * 若checkpoint 时间不要设置太短,
         * 这里的时间包括了超时时间
         *
         * 2.
         * 设置了周期性checkpoint,
         * 若上一个周期的checkpoint没完成,
         * 下一个周期的checkpoint不会开始的.
         *
         * 3.
         * 若checkpoint的持续时间超过了超时时间,
         * 会出现排队,
         * 过多的checkpoint排队会耗费资源
         *
         * 4.
         * 为了解决checkpoint排队堆积,
         * 需要优化checkpoint的完成效率
         *
         * *********************/
        //每60秒触发checkpoint
        env.enableCheckpointing(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_INTERVAL));
        CheckpointConfig ck = env.getCheckpointConfig();
        // checkpoint 必须在60秒内结束，否则被丢弃
        ck.setCheckpointTimeout(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_TIMEOUT));
        //checkpoint间最小间隔 30秒 (指定了这个值, setMaxConcurrentCheckpoints自动默认为1)
        ck.setMinPauseBetweenCheckpoints(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_MINPAUSE));
        // checkpoint 语义设置为 精确一致( EXACTLY_ONCE )
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 最多允许 checkpoint 失败 3 次
        ck.setTolerableCheckpointFailureNumber(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_FAILURENUMBER));
        // 同一时间只允许一个 checkpoint 进行
        ck.setMaxConcurrentCheckpoints(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_MAXCONCURRENT));

        //##########  checkpoint 设置 end ############//

        //设置 State 存储
        env.setStateBackend(new HashMapStateBackend());
        //并行度设置
        env.setParallelism(parameterTool.getInt(ParameterConstantsUtil.FLINK_PARALLELISM));


    }


}
