package xyz.hyhy.stayalert.api.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserData {
    private String id;
    //userInfo
    private String sex;
    private Date birthday;
    private int age;
    //userState
    private double usageDistance; //使用系统行驶距离
    private double usageTime; //使用系统的时常
    private double alertTime; //分心时长

}
