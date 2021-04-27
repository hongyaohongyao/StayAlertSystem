package xyz.hyhy.stayalert.flink.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import xyz.hyhy.stayalert.flink.pojo.CountDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserInfo;
import xyz.hyhy.stayalert.flink.task.*;

import java.time.Duration;

public class StayAlertFlinkApp {
    private static final String CHECKPOINTS_PATH =
            "hdfs://hadoophost:9000/flink-checkpoints/stayalert-flink-app";
    private static final String OUTPUT_PATH =
            "hdfs://hadoophost:9000/user/hadoop/flink-outputs/stayalert-flink-app";

    public static void main(String[] args) throws Exception {
        String os = System.getProperty("os.name");
        boolean isOnWindows = os != null && os.toLowerCase().contains("windows");
        if (isOnWindows) {
            //如果是windows系统要设置如下参数
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            System.setProperty("hadoop.home.dir", "D:\\ProgramData\\winutils");
        }
        //=====创建环境=====
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //       env.setParallelism(1);
        //设置EXACTLY_ONCE语义
        env.enableCheckpointing(10000); // 启动分布式快照机制
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        boolean selectStateBackend = SystemUtils.IS_OS_WINDOWS;

        if (isOnWindows) {
            env.setStateBackend(new FsStateBackend("file:///D:\\hongyao\\temp\\checkpoints\\flink-checkpoints\\checkpoints"));
        } else {
            env.setStateBackend(new FsStateBackend(CHECKPOINTS_PATH));
        }
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000); //设置barrier传递间隔时间
        //=====数据源======
        //kafka数据源
        DataStream<String> sourceDS = StayAlertKafkaTask
                .createKafkaSource(env)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(1)));//设置水印和事件戳
        DataStream<Tuple2<String, UserInfo>> userInfoSourceDS = UserDataTask.createUserInfoSource(env);
        //=====转换========
        // 解析kafka的字符串
        DataStream<UserDataPOJO> resolvedDS = StayAlertFormatTask
                .resolveFromKafka(sourceDS);
        //预测是否分心
        DataStream<UserDataPOJO> predictedDS = StayAlertPredictTask
                .predict(resolvedDS)
                .setParallelism(2);
        //打印预测结果
//        predictedDS.print("predict");
        //添加用户信息
        DataStream<UserDataPOJO> withUserInfoDS = UserDataTask
                .combineUserInfo(predictedDS, userInfoSourceDS)
                .setParallelism(2);
//        withUserInfoDS.print("withUserInfoDS");
//        统计用户状态user data
        DataStream<UserDataPOJO> countedUserDS = UserDataTask
                .countUserState(withUserInfoDS)
                .map(o -> {
                    o.setDeviceFeature(null);
                    return o;
                });//丢弃特征数据;
//        countedUserDS.print("userDataDS");
        //统计全局信息count data
        DataStream<CountDataPOJO> unsavedCountDataDS = CountDataTask
                .countData(withUserInfoDS);
        DataStream<CountDataPOJO> countDataDS = CountDataTask.saveCountData(unsavedCountDataDS);
//        countDataDS.print("countDataDS");
        //转换成JSON字符串
        DataStream<String> userJsonDS = StayAlertFormatTask.format2JsonString(countedUserDS);
        DataStream<String> countDataJsonDS = StayAlertFormatTask.format2JsonString(countDataDS);
        //=====sink=====
        StayAlertKafkaTask.sinkUserData2Kafka(userJsonDS);
        StayAlertKafkaTask.sinkCountData2Kafka(countDataJsonDS);
        //=====启动========
        env.execute("StayAlertFlink");
    }

}
