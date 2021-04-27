package xyz.hyhy.stayalert.flink;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Main {
    public static void main(String[] args) {
        System.out.println(Long.valueOf(100000000000L)==Long.valueOf(100000000000L));
    }

    public static <T> String obj2json(T o) {
        return JSONObject.toJSONString(o);
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class Haha {
    private String i;
    private int ho;
    private Haha haha;
}
