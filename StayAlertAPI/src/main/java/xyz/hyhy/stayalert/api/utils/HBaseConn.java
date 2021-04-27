package xyz.hyhy.stayalert.api.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 定义HBase连接的工具类
 */
public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration; //hbase配置
    private static Connection connection; //hbase connection

    private HBaseConn() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "hadoophost:2181");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取内部连接
     * @return
     */
    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取HBase连接
     * @return
     */
    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 根据获取HBase的table对象
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}