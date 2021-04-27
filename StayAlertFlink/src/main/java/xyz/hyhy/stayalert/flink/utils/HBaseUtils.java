package xyz.hyhy.stayalert.flink.utils;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import xyz.hyhy.stayalert.flink.constant.Constants;

import java.io.IOException;
import java.util.List;

public class HBaseUtils {


    private HBaseUtils() {
        String namespace = Constants.HBASE_NAMESPACE;
        if (!existNamespace(namespace)) {
            createNameSpace(namespace, "hadoop");
        }
    }


    /**
     * 创建命名空间
     *
     * @param namespace
     * @param creator
     */
    public static void createNameSpace(String namespace, String creator) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            NamespaceDescriptor weiboNamespace = NamespaceDescriptor.create(namespace)
                    .addConfiguration("creator", creator)
                    .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                    .build();
            admin.createNamespace(weiboNamespace);
            System.out.println("命名空间创建成功：" + namespace);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断命名空间是否存在
     *
     * @param namespace
     * @return
     */
    public static boolean existNamespace(String namespace) {
        if (namespace == null || "".equals(namespace))
            return false;
        boolean flag = false;
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            //直接使用admin.getNamespaceDescriptor，可能会报错
            // org.apache.hadoop.hbase.NamespaceNotFoundException
            NamespaceDescriptor[] nds = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor nd : nds) {
                if (nd.getName().equals(namespace)) {
                    flag = true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 创建表
     *
     * @param tableName      创建表的表名称
     * @param columnFamilies 列簇的集合
     * @return retValue
     */
    public static void simpleCreateTable(String tableName, int maxVersion, String... columnFamilies) {
        ColumnFamilyDescriptor[] cols = new ColumnFamilyDescriptor[columnFamilies.length];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(columnFamilies[i]))
                    .setMaxVersions(maxVersion)
                    .build();
        }
        createTable(tableName, cols);
    }

    public static void simpleCreateTable(String tableName, String... columnFamilies) {
        ColumnFamilyDescriptor[] cols = new ColumnFamilyDescriptor[columnFamilies.length];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(columnFamilies[i]))
                    .build();
        }
        createTable(tableName, cols);
    }

    public static TableDescriptorBuilder createTable(String tableName, ColumnFamilyDescriptor... columnFamilies) {
        TableDescriptorBuilder builder = null;
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("table:{" + tableName + "} exists!");
            } else {
                builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
                    builder.setColumnFamily(columnFamily);
                }
                admin.createTable(builder.build());
                System.out.println("table:{" + tableName + "}  success!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return builder;
    }

    public static boolean existTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("table:{" + tableName + "} exists!");
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static TableDescriptorBuilder createTableDescriptorBuilder(String tableName, String... columnFamilies) {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
        }
        return builder;
    }


    /**
     * 删除表
     *
     * @param tableName 表名称
     * @return retValue
     */
    public static boolean deleteTable(String tableName) {
        boolean retValue = false;
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                retValue = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retValue;
    }

    public static boolean existRow(String tableName, String rowKey) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setCheckExistenceOnly(true);
            return table.exists(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param data
     * @return
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data) {
        boolean retValue = false;
        try (Table table = HBaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
            retValue = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retValue;
    }

    public static boolean putRow(String tableName, Put put) {
        boolean retValue = false;
        try (Table table = HBaseConn.getTable(tableName)) {
            table.put(put);
            retValue = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retValue;
    }

    /**
     * 批量出入数据
     *
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        boolean retValue = false;
        try (Table table = HBaseConn.getTable(tableName)) {
            table.put(puts);
            retValue = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retValue;
    }

    /**
     * 查询单条数据
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(String tableName, String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        return getRow(tableName, get);
    }

    public static Result getRow(String tableName, String rowKey, String family) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));
        return getRow(tableName, get);
    }

    public static Result getRow(String tableName, Get get) {
        try (Table table = HBaseConn.getTable(tableName)) {
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get多行数据
     *
     * @param tableName
     * @param gets
     * @return
     */
    public static Result[] getRows(String tableName, List<Get> gets) {
        try (Table table = HBaseConn.getTable(tableName)) {
            return table.get(gets);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * scan扫描数据，
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取一个Put对象
     *
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param data
     * @return
     */
    public static Put createPut(String rowKey, String cfName, String qualifier, String data) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
        return put;
    }

    /**
     * 获取一个带时间戳的Put对象
     *
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param timestamp
     * @param data
     * @return
     */
    public static Put createPutWithTimestamp(String rowKey, String cfName, String qualifier, String data, long timestamp) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(data));
        return put;
    }

    /**
     * 获取一个delete对象
     *
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @return
     */
    public static Delete createDelete(String rowKey, String cfName, String qualifier) {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
        return delete;
    }

    /**
     * 获取一个Get对象
     *
     * @param rowKey
     * @param cfName
     * @return
     */
    public static Get createGet(String rowKey, String cfName) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(cfName));
        return get;
    }

    public static Get createGet(String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        return get;
    }

    public static Get createGet(String rowKey, String cfName, String qualifier) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
        return get;
    }

    public static Get createMultiVersionGet(String rowKey, String cfName, String qualifier, long from, long to) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))
                .setTimeRange(from, to)
                .readAllVersions();//setMaxVersion的替代
        return get;
    }


}
