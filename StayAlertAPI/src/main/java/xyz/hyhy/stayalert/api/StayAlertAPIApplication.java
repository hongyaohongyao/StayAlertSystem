package xyz.hyhy.stayalert.api;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StayAlertAPIApplication {

    public static void main(String[] args) {
        String os = System.getProperty("os.name");
        if (os != null && os.toLowerCase().contains("windows")) {
            //如果是windows系统要设置如下参数
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            System.setProperty("hadoop.home.dir", "D:\\ProgramData\\winutils");
        }
        SpringApplication.run(StayAlertAPIApplication.class, args);
    }

}
