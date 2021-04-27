package xyz.hyhy.stayalert.api.constant;

public interface Constants {
    String HBASE_NAMESPACE = "stayalert";
    String MYSQL_DATABASE = "stayalert_sys";
    String HBASE_USER_DATA_TABLE = "user_data";
    String HBASE_COUNT_DATA_TABLE = "count_data";
    String MYSQL_URL = "jdbc:mysql://hadoophost:3306/stayalert_sys?useSSL=false";

    double FREQ = 0.5; //每条数据代表几秒的状态
    int AGE_GROUP_GAP = 10;
}
