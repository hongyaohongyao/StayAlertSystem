package xyz.hyhy.stayalert.flink.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.xml.sax.SAXException;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.prediction.StayAlertPredictor;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class StayAlertPredictTask {
    private static StayAlertPredictor predictor;

    static {
        try {
            predictor = new StayAlertPredictor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    private StayAlertPredictTask() {

    }

    public static SingleOutputStreamOperator<UserDataPOJO> predict(DataStream<UserDataPOJO> ds) {
        return ds.flatMap(new FlatMapFunction<UserDataPOJO, UserDataPOJO>() {
            @Override
            public void flatMap(UserDataPOJO userDataPOJO,
                                Collector<UserDataPOJO> collector) throws Exception {
                try {
                    //判断是否分心
                    boolean isAlert = predictor.predict(userDataPOJO.getDeviceFeature());
                    userDataPOJO.setIsAlert(isAlert);
                    collector.collect(userDataPOJO);
                    userDataPOJO.setIsAlert(null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
