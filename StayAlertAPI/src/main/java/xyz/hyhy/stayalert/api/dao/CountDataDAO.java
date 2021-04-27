package xyz.hyhy.stayalert.api.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import xyz.hyhy.stayalert.api.constant.Constants;
import xyz.hyhy.stayalert.api.entity.CountData;
import xyz.hyhy.stayalert.api.entity.CountData.AlertRatePair;
import xyz.hyhy.stayalert.api.utils.DateUtils;
import xyz.hyhy.stayalert.api.utils.HBaseUtils;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository("countDataDAO")
public class CountDataDAO {
    private String namespace;
    private String tableName;
    private String tableNameInHBase;
    public transient List<Put> puts;

    @PostConstruct
    public void open() throws SQLException {
        namespace = Constants.HBASE_NAMESPACE;
        tableName = Constants.HBASE_COUNT_DATA_TABLE;
        tableNameInHBase = namespace + ":" + tableName;
        //hBase部分
        if (!HBaseUtils.existNamespace(namespace)) {
            HBaseUtils.createNameSpace(namespace, "hadoop");
        }
        if (!HBaseUtils.existTable(tableNameInHBase)) {
            HBaseUtils.simpleCreateTable(tableNameInHBase, "data");
        }
        puts = new ArrayList<>();
    }

    public CountData getTodayCountData() {
        return getCountData(DateUtils.getTodayString());
    }

    /**
     * 根据日期获取统计数据 格式为 yyyy-MM-dd
     *
     * @param dayString
     * @return
     */
    public CountData getCountData(String dayString) {
        CountData countData = new CountData();
        try {
            Result result = HBaseUtils.getRow(
                    tableNameInHBase,
                    dayString,
                    "data");
            if (result.isEmpty()) {
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

    public void putCountData(CountData countData) {
        long timestamp = countData.getTimestamp();
        String today = DateUtils.getTodayString(timestamp);
        puts.clear();
        puts.add(getPut(today, "sex-male",
                getDataString(countData.getSexMaleAlertRate()), timestamp));
        puts.add(getPut(today, "sex-female",
                String.valueOf(countData.getSexFemaleAlertRate()), timestamp));
        puts.add(getPut(today, "sex-female",
                getDataString(countData.getSexFemaleAlertRate()), timestamp));
        //添加年龄段数据
        for (Map.Entry<Integer, AlertRatePair> entry :
                countData.getAgeGroupAlertRate().entrySet()) {
            puts.add(getPut(today, "ageGroup-" + entry.getKey(),
                    getDataString(entry.getValue()), timestamp));
        }
        //添加时间段数据
        for (Map.Entry<Integer, AlertRatePair> entry :
                countData.getPeriodAlertRate().entrySet()) {
            puts.add(getPut(today, "period-" + entry.getKey(),
                    getDataString(entry.getValue()), timestamp));
        }
        HBaseUtils.putRows(tableNameInHBase, puts);
    }

    private String getDataString(AlertRatePair alertRatePair) {
        return alertRatePair.getUsageTime() + "," + alertRatePair.getAlertTime();
    }

    private Put getPut(String rowKey, String qualifier, String data, long timestamp) {
        return HBaseUtils.createPutWithTimestamp(rowKey, "data", qualifier, data, timestamp);
    }
}
