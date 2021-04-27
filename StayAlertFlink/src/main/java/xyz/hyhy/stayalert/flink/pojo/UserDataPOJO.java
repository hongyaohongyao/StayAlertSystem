package xyz.hyhy.stayalert.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDataPOJO {
    private String id;
    private UserInfo userInfo;
    private UserState userState;
    private Map<String, ?> deviceFeature;
    private Boolean isAlert;
    private long timestamp;

//    public StayAlertPOJO() {
//        userInfo = new UserInfo();
//        userState = new UserState();
//    }

}
