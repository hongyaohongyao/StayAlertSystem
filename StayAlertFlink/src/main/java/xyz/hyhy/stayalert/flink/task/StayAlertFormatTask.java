package xyz.hyhy.stayalert.flink.task;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.UserInfo;

public class StayAlertFormatTask {
    private StayAlertFormatTask() {

    }

    public static DataStream<UserDataPOJO> resolveFromKafka(DataStream<String> ds) {
        return ds.flatMap(new RichFlatMapFunction<String, UserDataPOJO>() {
            /**
             * 创建持久化的返回对象，防止反复new新的变量，提高效率
             */
            private UserDataPOJO pojo;

            @Override
            public void open(Configuration parameters) throws Exception {
                pojo = new UserDataPOJO();
                pojo.setUserInfo(new UserInfo());
            }

            @Override
            public void flatMap(String s, Collector<UserDataPOJO> collector) throws Exception {
                JSONObject obj = JSONObject.parseObject(s);
                //设置StayAlertPOJO
                String id = obj.getString("id");
                long timestamp = obj.getLong("timestamp");

                pojo.setId(id);
                pojo.setTimestamp(timestamp);
                pojo.setDeviceFeature(obj);
                //收集数据
                collector.collect(pojo);
            }
        });
    }

    public static <T> DataStream<String> format2JsonString(DataStream<T> ds) {
        return ds.flatMap(new FlatMapFunction<T, String>() {
            @Override
            public void flatMap(T o, Collector<String> collector) throws Exception {
                collector.collect(JSONObject.toJSONString(o));
            }
        });
    }
}
