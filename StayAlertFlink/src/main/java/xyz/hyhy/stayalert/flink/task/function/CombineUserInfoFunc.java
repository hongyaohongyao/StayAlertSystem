package xyz.hyhy.stayalert.flink.task.function;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserInfo;

public class CombineUserInfoFunc extends BroadcastProcessFunction<
        UserDataPOJO, //设备信息
        Tuple2<String, UserInfo>, //位置数据
        UserDataPOJO//<设备id, <区域id,到禁区的距离>>
        > {

    // 状态描述器
    private static final MapStateDescriptor<String, UserInfo> descriptor =
            new MapStateDescriptor<>("user-infos", Types.STRING, Types.POJO(UserInfo.class));

    //创建广播流
    public static final BroadcastStream<Tuple2<String, UserInfo>>
    getBroadcastDS(DataStream<Tuple2<String, UserInfo>> userInfoDS) {
        return userInfoDS.broadcast(descriptor);
    }


    @Override
    public void processElement(UserDataPOJO value,
                               ReadOnlyContext ctx,
                               Collector<UserDataPOJO> out) throws Exception {
        //获取设备id
        String id = value.getId();
        //根据状态描述器获取广播状态
        ReadOnlyBroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
        if (broadcastState != null) {
            UserInfo userInfo = broadcastState.get(id);
            if (userInfo != null) {
                value.setUserInfo(userInfo);
                out.collect(value);
            }
        }
    }

    @Override
    public void processBroadcastElement(Tuple2<String, UserInfo> value,
                                        Context ctx, Collector<UserDataPOJO> out) throws Exception {
        //value就是MySQLSource中每隔一段时间获取到的最新的map数据
        //先根据状态描述器获取历史的广播状态
        BroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
        //最后将最新的广播流数据放到state中（更新状态数据）
        broadcastState.put(value.f0, value.f1);
    }
}
