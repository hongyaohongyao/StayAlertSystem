package xyz.hyhy.stayalert.flink.task;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserInfo;
import xyz.hyhy.stayalert.flink.task.function.CombineUserInfoFunc;
import xyz.hyhy.stayalert.flink.task.function.CountUserStateFunc;
import xyz.hyhy.stayalert.flink.task.source.UserInfoSQLSource;

public class UserDataTask {
    private UserDataTask() {

    }

    /**
     * 创建读取UserInfo信息的Source
     *
     * @param env
     * @return
     */
    public static DataStream<Tuple2<String, UserInfo>>
    createUserInfoSource(StreamExecutionEnvironment env) {
        return env.addSource(new UserInfoSQLSource());
    }

    /**
     * 为StayAlert数据流添加UserInfo信息
     *
     * @param ds
     * @param userInfoDS
     * @return
     */
    public static SingleOutputStreamOperator<UserDataPOJO> combineUserInfo(DataStream<UserDataPOJO> ds,
                                                                           DataStream<Tuple2<String, UserInfo>> userInfoDS) {
        //创建广播流
        BroadcastStream<Tuple2<String, UserInfo>> broadcastDS = CombineUserInfoFunc.getBroadcastDS(userInfoDS);
        //将数据流与广播流合并
        BroadcastConnectedStream<UserDataPOJO, Tuple2<String, UserInfo>>
                connectedDS = ds.connect(broadcastDS);
        //合并数据并返回
        return connectedDS.process(new CombineUserInfoFunc());
    }

    public static DataStream<UserDataPOJO> countUserState(DataStream<UserDataPOJO> ds) {
        return ds
                .keyBy(k -> k.getId()) //需要先分组
                .flatMap(new CountUserStateFunc());
    }

}
