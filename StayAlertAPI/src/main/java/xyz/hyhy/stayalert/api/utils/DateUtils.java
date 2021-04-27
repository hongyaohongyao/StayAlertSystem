package xyz.hyhy.stayalert.api.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    private DateUtils() {

    }

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static String getTodayString() {
        try {
            return simpleDateFormat.format(new Date());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getTodayString(long time) {
        try {
            return simpleDateFormat.format(new Date(time));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 计算年龄
     */
    public static int getAge(Date birthDay) throws Exception {
        if (birthDay == null) {
            throw new IllegalArgumentException(
                    "The birthDay is null.It's unbelievable!");
        }
        Calendar cal = Calendar.getInstance();

        if (cal.before(birthDay)) {
            throw new IllegalArgumentException(
                    "The birthDay is before Now.It's unbelievable!");
        }
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH);
        int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
        cal.setTime(birthDay);

        int yearBirth = cal.get(Calendar.YEAR);
        int monthBirth = cal.get(Calendar.MONTH);
        int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);

        int age = yearNow - yearBirth;

        if (monthNow <= monthBirth) {
            if (monthNow == monthBirth) {
                if (dayOfMonthNow < dayOfMonthBirth) age--;
            } else {
                age--;
            }
        }
        return age;
    }

}
