package xyz.hyhy.stayalert.flink.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInfo {
    //个人信息
    private String sex;
    private Date birthday;
    private int age;
}
