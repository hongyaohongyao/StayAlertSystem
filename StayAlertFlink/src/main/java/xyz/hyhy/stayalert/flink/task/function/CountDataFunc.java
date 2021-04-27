package xyz.hyhy.stayalert.flink.task.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import xyz.hyhy.stayalert.flink.constant.Constants;
import xyz.hyhy.stayalert.flink.pojo.CountDataPOJO;
import xyz.hyhy.stayalert.flink.pojo.CountDataPOJO.AlertRatePair;
import xyz.hyhy.stayalert.flink.pojo.UserDataPOJO;
import xyz.hyhy.stayalert.flink.utils.DateUtils;
import xyz.hyhy.stayalert.flink.utils.HBaseUtils;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;

public class CountDataFunc implements AggregateFunction<UserDataPOJO, CountDataPOJO, CountDataPOJO> {
    public static final double FREQ = Constants.FREQ;
    public static final int AGE_GROUP_GAP = Constants.AGE_GROUP_GAP;
    //    private transient Calendar calendar = Calendar.getInstance();
    public static final String HBASE_NAMESPACE = Constants.HBASE_NAMESPACE;
    public static final String TABLE_NAME = HBASE_NAMESPACE + ":" + Constants.HBASE_COUNT_DATA_TABLE;


    /**
     * 先从hbase取数据，没取到就创建新的累积值
     *
     * @return
     */
    @Override
    public CountDataPOJO createAccumulator() {
        CountDataPOJO initAcc = getCountData(DateUtils.getTodayString());
        return initAcc == null ? new CountDataPOJO() : initAcc;
    }

    @Override
    public CountDataPOJO add(UserDataPOJO userDataPOJO, CountDataPOJO countDataPOJO) {
        try {
            //更新时间戳
            countDataPOJO.setTimestamp(Math.max(countDataPOJO.getTimestamp(), userDataPOJO.getTimestamp()));
            //统计男女警惕率
            String sex = userDataPOJO.getUserInfo().getSex();
            double alert = userDataPOJO.getIsAlert() ? FREQ : 0;
            if ("男".equals(sex)) {
                AlertRatePair sexAlertRate = countDataPOJO.getSexMaleAlertRate();
                sexAlertRate.setUsageTime(sexAlertRate.getUsageTime() + FREQ);
                sexAlertRate.setAlertTime(sexAlertRate.getAlertTime() + alert);
            } else if ("女".equals(sex)) {
                AlertRatePair sexAlertRate = countDataPOJO.getSexFemaleAlertRate();
                sexAlertRate.setUsageTime(sexAlertRate.getUsageTime() + FREQ);
                sexAlertRate.setAlertTime(sexAlertRate.getAlertTime() + alert);
            }
            //统计不同年龄段的警惕率
            {
                int ageGroup = userDataPOJO.getUserInfo().getAge() / AGE_GROUP_GAP * AGE_GROUP_GAP;
                Map<Integer, AlertRatePair> alertRates = countDataPOJO.getAgeGroupAlertRate();
                AlertRatePair alertRatePair = alertRates.get(ageGroup);
                if (alertRatePair == null)
                    alertRatePair = new AlertRatePair();
                alertRatePair.setUsageTime(alertRatePair.getUsageTime() + FREQ);
                alertRatePair.setAlertTime(alertRatePair.getAlertTime() + alert);
                alertRates.put(ageGroup, alertRatePair);
            }
            //统计不同小时的时间段的警惕率
            {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(userDataPOJO.getTimestamp());
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                Map<Integer, AlertRatePair> alertRates = countDataPOJO.getPeriodAlertRate();
                AlertRatePair alertRatePair = alertRates.get(hour);
                if (alertRatePair == null)
                    alertRatePair = new AlertRatePair();
                alertRatePair.setUsageTime(alertRatePair.getUsageTime() + FREQ);
                alertRatePair.setAlertTime(alertRatePair.getAlertTime() + alert);
                alertRates.put(hour, alertRatePair);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //返回结果
        return countDataPOJO;
    }

    @Override
    public CountDataPOJO getResult(CountDataPOJO countDataPOJO) {
        return countDataPOJO;
    }


    @Override
    public CountDataPOJO merge(CountDataPOJO c1, CountDataPOJO c2) {
        try {
            c1.setTimestamp(Math.max(c1.getTimestamp(), c2.getTimestamp()));
            //合并性别数据
            c1.getSexMaleAlertRate().add(c2.getSexMaleAlertRate());
            c1.getSexFemaleAlertRate().add(c2.getSexFemaleAlertRate());
            //合并年龄段数据
            {
                Map<Integer, AlertRatePair> arMap1 = c1.getAgeGroupAlertRate();
                Map<Integer, AlertRatePair> arMap2 = c2.getAgeGroupAlertRate();
                Set<Integer> keySet = arMap1.keySet();
                keySet.addAll(arMap2.keySet());
                for (int key : keySet) {
                    AlertRatePair a1 = arMap1.get(key);
                    AlertRatePair a2 = arMap2.get(key);
                    if (a1 == null) {
                        if (a2 != null) {
                            arMap1.put(key, a2);
                        }
                    } else {
                        if (a2 != null)
                            a1.add(a2);
                    }
                }
            }
            //合并时间段数据
            {
                Map<Integer, AlertRatePair> arMap1 = c1.getPeriodAlertRate();
                Map<Integer, AlertRatePair> arMap2 = c2.getPeriodAlertRate();
                Set<Integer> keySet = arMap1.keySet();
                keySet.addAll(arMap2.keySet());
                for (int key : keySet) {
                    AlertRatePair a1 = arMap1.get(key);
                    AlertRatePair a2 = arMap2.get(key);
                    if (a1 == null) {
                        if (a2 != null) {
                            arMap1.put(key, a2);
                        }
                    } else {
                        if (a2 != null)
                            a1.add(a2);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return c1;
    }

    /**
     * 根据日期获取统计数据 格式为 yyyy-MM-dd
     *
     * @param dayString
     * @return
     */
    public CountDataPOJO getCountData(String dayString) {
        CountDataPOJO countData = new CountDataPOJO();
        try {
            Result result = HBaseUtils.getRow(
                    TABLE_NAME,
                    dayString,
                    "data");
            if(result.isEmpty()){
                return null;
            }
            for (Cell cell : result.rawCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell)); //获取值
                //[对应分类，对应类型] e.g. sex-female
                String[] fieldValue = qualifier.split("-", 2);
                if ("sex".equals(fieldValue[0])) {
                    if ("male".equals(fieldValue[1])) {
                        countData.setSexMaleAlertRate(parseAlertRatePair(value));
                    } else if ("female".equals(fieldValue[1])) {
                        countData.setSexFemaleAlertRate(parseAlertRatePair(value));
                    }
                } else if ("ageGroup".equals(fieldValue[0])) {
                    countData.getAgeGroupAlertRate()
                            .put(Integer.parseInt(fieldValue[1]),
                                    parseAlertRatePair(value));
                } else if ("period".equals(fieldValue[0])) {
                    countData.getPeriodAlertRate()
                            .put(Integer.parseInt(fieldValue[1]),
                                    parseAlertRatePair(value));
                } else {
                    throw new Exception("从Hbase表中读到了未知字段");
                }
            }
            return countData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private AlertRatePair parseAlertRatePair(String value) {
        String[] values = value.split(",");
        return new AlertRatePair(
                Double.parseDouble(values[0]),
                Double.parseDouble(values[1]));
    }

    private double getValue(Result result, String qualifier) {
        byte[] family = Bytes.toBytes("data");
        Cell cell = result.getColumnLatestCell(family, Bytes.toBytes(qualifier));
        return Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
    }
}
