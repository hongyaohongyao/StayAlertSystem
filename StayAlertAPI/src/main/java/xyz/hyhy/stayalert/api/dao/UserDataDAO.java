package xyz.hyhy.stayalert.api.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import xyz.hyhy.stayalert.api.constant.Constants;
import xyz.hyhy.stayalert.api.entity.UserData;
import xyz.hyhy.stayalert.api.utils.DateUtils;
import xyz.hyhy.stayalert.api.utils.HBaseUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.*;
import java.util.Date;

@Repository("userDataDAO")
public class UserDataDAO {
    private String namespace;
    private String tableName;
    private String tableNameInHBase;
    //mysql
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    @PostConstruct
    public void open() throws SQLException {
        namespace = Constants.HBASE_NAMESPACE;
        tableName = Constants.HBASE_USER_DATA_TABLE;
        tableNameInHBase = namespace + ":" + tableName;
        //hBase部分
        if (!HBaseUtils.existNamespace(namespace)) {
            HBaseUtils.createNameSpace(namespace, "hadoop");
        }
        if (!HBaseUtils.existTable(tableNameInHBase)) {
            HBaseUtils.simpleCreateTable(tableNameInHBase, "state");
        }
        //mysql部分
        conn = DriverManager.getConnection(Constants.MYSQL_URL, "root", "Hongyao@2020");
        String sql = "select `sex`, `birthday` from `user_info` where `user_id`=?";
        ps = conn.prepareStatement(sql);
    }

    @PreDestroy
    public void destroy() throws SQLException {
        if (conn != null) conn.close();
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }

    public UserData getUserData(String userId) {
        UserData userData = new UserData();
        userData.setId(userId);
        try {
            //完善user info
            ps.setString(1, userId);
            rs = ps.executeQuery();
            if (rs.next()) {
                userData.setSex(rs.getString("sex"));
                Date birthday = rs.getDate("birthday");
                userData.setBirthday(birthday);
                userData.setAge(DateUtils.getAge(birthday)); //计算年龄
            } else {
                return null;
            }
            //完善user state
            Result result = HBaseUtils.getRow(tableNameInHBase, userId, "state");
            if (!result.isEmpty()) {
                userData.setUsageDistance(getValue(result, "usageDistance"));
                userData.setUsageTime(getValue(result, "usageTime"));
                userData.setAlertTime(getValue(result, "alertTime"));
            }
            return userData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private double getValue(Result result, String qualifier) {
        byte[] family = Bytes.toBytes("state");
        Cell cell = result.getColumnLatestCell(family, Bytes.toBytes(qualifier));
        return Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
    }
}
