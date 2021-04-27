package xyz.hyhy.stayalert.api.service;

import org.springframework.stereotype.Service;
import xyz.hyhy.stayalert.api.dao.CountDataDAO;
import xyz.hyhy.stayalert.api.entity.CountData;
import xyz.hyhy.stayalert.api.entity.CountData.AlertRatePair;

import javax.annotation.Resource;

@Service("countDataService")
public class CountDataService {
    @Resource
    private CountDataDAO countDataDAO;

    /**
     * 获取今天的统计数据
     *
     * @return
     */
    public CountData getTodayCountDataDAO() {
        return countDataDAO.getTodayCountData();
    }

    public CountData getCountDataDAO(String dayString) {
        return countDataDAO.getCountData(dayString);
    }


    public void generateFakeCountData() {
        CountData countData = new CountData();
        //设置性别的alertRate
        assignFakeAlertRate(countData.getSexMaleAlertRate());
        assignFakeAlertRate(countData.getSexFemaleAlertRate());
        for (int i = 0; i < 24; i++) {
            countData
                    .getPeriodAlertRate()
                    .put(i, assignFakeAlertRate(new AlertRatePair()));
        }
        for (int i = 2; i < 6; i++) {
            countData
                    .getAgeGroupAlertRate()
                    .put(i * 10, assignFakeAlertRate(new AlertRatePair()));
        }
        countDataDAO.putCountData(countData);
    }

    private AlertRatePair assignFakeAlertRate(AlertRatePair alertRatePair) {
        alertRatePair.setUsageTime(Math.random() * 10);
        alertRatePair.setAlertTime(Math.random() * alertRatePair.getUsageTime());
        return alertRatePair;
    }
}
