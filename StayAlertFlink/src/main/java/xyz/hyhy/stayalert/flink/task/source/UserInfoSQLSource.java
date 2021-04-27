package xyz.hyhy.stayalert.flink.task.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import xyz.hyhy.stayalert.flink.constant.Constants;
import xyz.hyhy.stayalert.flink.pojo.UserInfo;
import xyz.hyhy.stayalert.flink.utils.DateUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

public class UserInfoSQLSource extends RichSourceFunction<Tuple2<String, UserInfo>> {
    private final long UPDATE_INTERVAL;
    private boolean flag = true;
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    public UserInfoSQLSource() {
        this(5000);
    }

    public UserInfoSQLSource(long updateInterval) {
        this.UPDATE_INTERVAL = updateInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(Constants.MYSQL_URL, "root", "Hongyao@2020");
        String sql = "select `user_id`, `sex`, `birthday` from `user_info`";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        if (conn != null) conn.close();
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }

    @Override
    public void run(SourceContext<Tuple2<String, UserInfo>> ctx) throws Exception {
        UserInfo userInfo = new UserInfo();
        while (flag) {
            rs = ps.executeQuery();
            while (rs.next()) {
                String userId = rs.getString("user_id");
                userInfo.setSex(rs.getString("sex"));
                Date birthday = rs.getDate("birthday");
                userInfo.setBirthday(birthday);
                userInfo.setAge(DateUtils.getAge(birthday)); //计算年龄
                ctx.collect(Tuple2.of(userId, userInfo));
            }

            Thread.sleep(UPDATE_INTERVAL);//默认UPDATE_INTERVAL=5000，每隔5s更新一下用户的配置信息!
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
