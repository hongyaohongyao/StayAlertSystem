package xyz.hyhy.stayalert.flink.task;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import xyz.hyhy.stayalert.flink.pojo.CountDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.task.function.CountDataFunc;
import xyz.hyhy.stayalert.flink.task.function.SaveCountDataFunc;

public class CountDataTask {
    private static final Object CONST_KEY = new Object();

    private CountDataTask() {

    }

    public static SingleOutputStreamOperator<CountDataPOJO> countData(DataStream<UserDataPOJO> ds) {
        return ds
                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new CountDataFunc());
    }

    public static SingleOutputStreamOperator<CountDataPOJO> saveCountData(DataStream<CountDataPOJO> ds) {
        return ds.keyBy(x -> CONST_KEY)
                .flatMap(new SaveCountDataFunc());
    }
}
