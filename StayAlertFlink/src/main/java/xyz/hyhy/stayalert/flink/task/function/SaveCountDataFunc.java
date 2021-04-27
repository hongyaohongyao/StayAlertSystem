package xyz.hyhy.stayalert.flink.task.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import xyz.hyhy.stayalert.flink.constant.Constants;
import xyz.hyhy.stayalert.flink.pojo.CountDataPOJO;
import xyz.hyhy.stayalert.flink.utils.DateUtils;
import xyz.hyhy.stayalert.flink.utils.HBaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * 定期保存Count Data
 */
public class SaveCountDataFunc extends RichFlatMapFunction<CountDataPOJO, CountDataPOJO> {
    public static final String STATE_NAME = "count-data-save-time";
    private transient ValueState<Long> lastSaveTimeState;
    public transient List<Put> puts;
    private static final long SAVE_INTERVAL = 5000;//millis
    private static final long FORCE_SAVE_INTERVAL = 15000;//millis
    //
    public static final String HBASE_NAMESPACE = Constants.HBASE_NAMESPACE;
    public static final String TABLE_NAME = HBASE_NAMESPACE + ":" + Constants.HBASE_COUNT_DATA_TABLE;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            STATE_NAME, // the state name
                            Types.LONG // type information
                    );
            lastSaveTimeState = getRuntimeContext().getState(descriptor);

            puts = new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flatMap(CountDataPOJO countDataPOJO, Collector<CountDataPOJO> out) throws Exception {
        try {
            Long timestamp = countDataPOJO.getTimestamp();
            Long lastSaveTime = lastSaveTimeState.value();
            long currentTime = System.currentTimeMillis();
            if (lastSaveTime == null ||
                    //当前时间戳和最后一次保存的时间戳相差超过时间间隔
                    (timestamp / SAVE_INTERVAL - lastSaveTime / SAVE_INTERVAL) > 0 ||
                    // 最后一次保存时间不是当前时间戳，且当前时间与当前时间戳相差超过时间间隔
                    (lastSaveTime != timestamp &&
                            (currentTime / FORCE_SAVE_INTERVAL - timestamp / FORCE_SAVE_INTERVAL) > 0)) {
                String today = DateUtils.getTodayString(timestamp);
                puts.clear();
                puts.add(getPut(today, "sex-male",
                        getDataString(countDataPOJO.getSexMaleAlertRate()), timestamp));
                puts.add(getPut(today, "sex-female",
                        String.valueOf(countDataPOJO.getSexFemaleAlertRate()), timestamp));
                puts.add(getPut(today, "sex-female",
                        getDataString(countDataPOJO.getSexFemaleAlertRate()), timestamp));
                //添加年龄段数据
                for (Entry<Integer, CountDataPOJO.AlertRatePair> entry :
                        countDataPOJO.getAgeGroupAlertRate().entrySet()) {
                    puts.add(getPut(today, "ageGroup-" + entry.getKey(),
                            getDataString(entry.getValue()), timestamp));
                }
                //添加时间段数据
                for (Entry<Integer, CountDataPOJO.AlertRatePair> entry :
                        countDataPOJO.getPeriodAlertRate().entrySet()) {
                    puts.add(getPut(today, "period-" + entry.getKey(),
                            getDataString(entry.getValue()), timestamp));
                }
                HBaseUtils.putRows(TABLE_NAME, puts);
                //更新保存时间
                lastSaveTimeState.update(timestamp);
//                System.out.println(getRuntimeContext().getNumberOfParallelSubtasks() +
//                                "==保存时间：" +
//                                DateUtils.getNowString(timestamp)
//                        ((timestamp / SAVE_INTERVAL - lastSaveTime / SAVE_INTERVAL) > 0) +
//                        (lastSaveTime != timestamp) +
//                        ((currentTime / FORCE_SAVE_INTERVAL - timestamp / FORCE_SAVE_INTERVAL) > 0)
//                );
            }
            if (!timestamp.equals(lastSaveTime))
                out.collect(countDataPOJO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getDataString(CountDataPOJO.AlertRatePair alertRatePair) {
        return alertRatePair.getUsageTime() + "," + alertRatePair.getAlertTime();
    }

    private Put getPut(String rowKey, String qualifier, String data, long timestamp) {
        return HBaseUtils.createPutWithTimestamp(rowKey, "data", qualifier, data, timestamp);
    }


}
