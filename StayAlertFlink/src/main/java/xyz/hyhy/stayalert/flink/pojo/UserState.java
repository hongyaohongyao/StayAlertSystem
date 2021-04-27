package xyz.hyhy.stayalert.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserState {
    //实时状态
    private double speed; //当前行驶速度
    //统计的状态
    private double usageDistance; //使用系统行驶距离
    private double usageTime; //使用系统的时常
    private double alertTime; //分心时长
}
