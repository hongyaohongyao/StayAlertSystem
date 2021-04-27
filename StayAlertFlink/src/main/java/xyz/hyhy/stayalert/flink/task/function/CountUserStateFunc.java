package xyz.hyhy.stayalert.flink.task.function;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import xyz.hyhy.stayalert.flink.constant.Constants;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserState;
import xyz.hyhy.stayalert.flink.utils.DateUtils;
import xyz.hyhy.stayalert.flink.utils.HBaseUtils;

import java.util.ArrayList;
import java.util.List;

public class CountUserStateFunc extends RichFlatMapFunction<
        UserDataPOJO,
        UserDataPOJO> {

    public static final String STATE_NAME = "user-state";
    public static final String HBASE_NAMESPACE = Constants.HBASE_NAMESPACE;
    public static final String TABLE_NAME = HBASE_NAMESPACE + ":" + Constants.HBASE_USER_DATA_TABLE;
    private static final long SAVE_INTERVAL = 5000;//millis
    //    public static final double SMOOTH_FACTOR = 0.5;// 计算滑动平均速度的平滑因子，越小越平滑
    //<user_state, timestamp>
    private transient ValueState<Tuple2<UserState, Long>> userState;
    private static final double FREQ = Constants.FREQ;

//    static {
//        //创建hBase表
//        if (!HBaseUtils.existTable(TABLE_NAME)) {
//            HBaseUtils.simpleCreateTable(TABLE_NAME, 1000, );
//        }
//    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<UserState, Long>> descriptor =
                new ValueStateDescriptor<>(
                        STATE_NAME, // the state name
                        Types.TUPLE(Types.POJO(UserState.class), Types.LONG) // type information
                );
        userState = getRuntimeContext().getState(descriptor);
    }

    @SneakyThrows
    @Override
    public void flatMap(UserDataPOJO userDataPOJO, Collector<UserDataPOJO> collector) throws Exception {
        try {
            //公里/小时换算成米/秒
            double speed = Double.parseDouble(userDataPOJO.getDeviceFeature().get("V1").toString()) / 36;
            boolean isAlert = userDataPOJO.getIsAlert();// 如果为false，则增加alertTimes
            //获取状态
            Tuple2<UserState, Long> valueState = userState.value();
            UserState currentState;
            long timestamp = 0;
            //判断是否在HBase中保存了历史数据
            String userId = userDataPOJO.getId();
            if (valueState == null) {
                currentState = getUserState(userId);
            } else {
                currentState = valueState.f0;
                timestamp = valueState.f1;
            }
            //判断是否要创建新的state
            if (currentState == null) {
                currentState = new UserState(speed, FREQ * speed, FREQ, isAlert ? FREQ : 0);
            } else {
                //设置速度
                currentState.setSpeed(speed);
                //更新使用里程
                currentState.setUsageDistance(currentState.getUsageDistance() + FREQ * speed);
                currentState.setUsageTime(currentState.getUsageTime() + FREQ);
                currentState.setAlertTime(currentState.getAlertTime() + (isAlert ? FREQ : 0));
            }
            //判断是否保存,间隔SAVE_INTERVAL毫秒保存,整点保存
            long dataTimestamp = userDataPOJO.getTimestamp();
            if ((dataTimestamp / SAVE_INTERVAL - timestamp / SAVE_INTERVAL) >= 1) {
                timestamp = dataTimestamp;
                saveUserState(userId, dataTimestamp, currentState);
//                System.out.println(userId + "==保存时间：" + DateUtils.getNowString(timestamp));
            }
            //更新函数状态
            userState.update(Tuple2.of(currentState, timestamp));
            //设置当前状态
            userDataPOJO.setUserState(currentState);
            collector.collect(userDataPOJO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取用户状态
     *
     * @param userId
     * @return
     */
    private UserState getUserState(String userId) {
        try {
            Result result = HBaseUtils.getRow(TABLE_NAME, userId, "state");
            if(result.isEmpty()){
                return null;
            }
            Double speed = getValue(result, "speed");
            Double usageDistance = getValue(result, "usageDistance");
            Double usageTime = getValue(result, "usageTime");
            Double alertTime = getValue(result, "alertTime");
            UserState userState = new UserState(speed, usageDistance, usageTime, alertTime);
            return userState;
        } catch (Exception e) {

        }
        return null;
    }

    private double getValue(Result result, String qualifier) {
        byte[] family = Bytes.toBytes("state");
        Cell cell = result.getColumnLatestCell(family, Bytes.toBytes(qualifier));
        return Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
    }

    private final List<Put> puts = new ArrayList<>();

    private void saveUserState(String userId, long timestamp, UserState userState) {
        puts.clear();
        puts.add(getPut(userId, timestamp, "speed", String.valueOf(userState.getSpeed())));
        puts.add(getPut(userId, timestamp, "usageDistance", String.valueOf(userState.getUsageDistance())));
        puts.add(getPut(userId, timestamp, "usageTime", String.valueOf(userState.getUsageTime())));
        puts.add(getPut(userId, timestamp, "alertTime", String.valueOf(userState.getAlertTime())));
        HBaseUtils.putRows(TABLE_NAME, puts);
    }

    private Put getPut(String userId, long timestamp, String qualifier, String data) {
        return HBaseUtils.createPutWithTimestamp(userId, "state", qualifier, data, timestamp);
    }

}
