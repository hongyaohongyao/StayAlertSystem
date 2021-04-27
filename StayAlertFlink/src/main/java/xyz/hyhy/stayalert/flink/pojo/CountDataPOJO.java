package xyz.hyhy.stayalert.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 统计数据，宽表
 */
@Data
@AllArgsConstructor
public class CountDataPOJO {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class AlertRatePair {
        private double usageTime;
        private double alertTime;

        public void add(AlertRatePair a) {
            usageTime += a.usageTime;
            alertTime += a.alertTime;
        }
    }

    //时间戳
    private long timestamp = System.currentTimeMillis();//统计到最晚数据的时间

    //性别警惕率 计算lossAlertTime/usageTime [usageTime,lossAlertTime]
    private AlertRatePair sexMaleAlertRate;
    private AlertRatePair sexFemaleAlertRate;
    //年龄警惕率
    private Map<Integer, AlertRatePair> ageGroupAlertRate;
    //时间段警惕率
    private Map<Integer, AlertRatePair> periodAlertRate;

    public CountDataPOJO() {
        sexMaleAlertRate = new AlertRatePair();
        sexFemaleAlertRate = new AlertRatePair();
        ageGroupAlertRate = new HashMap<>();
        periodAlertRate = new HashMap<>();
    }
}
