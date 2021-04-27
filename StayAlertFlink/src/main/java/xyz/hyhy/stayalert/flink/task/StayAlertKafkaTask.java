package xyz.hyhy.stayalert.flink.task;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

public class StayAlertKafkaTask {
    private StayAlertKafkaTask() {

    }

    private static final String BOOTSTRAP_SERVERS = "hadoophost:9092";
    private static final String SOURCE_TOPIC_NAME = "stayalert-flink-app_device-data";
    private static final String SOURCE_CONSUMER_GROUP_ID = "stayalert-flink-app";
    private static final String SINK_USER_DATA_TOPIC_NAME = "stayalert-flink-app_user-data";
    private static final String SINK_COUNT_DATA_TOPIC_NAME = "stayalert-flink-app_count-data";

    /**
     * 创建kafka数据源
     *
     * @return kafka数据源
     */
    public static DataStream<String> createKafkaSource(StreamExecutionEnvironment env) {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS); // 集群地址
        props.setProperty("group.id", SOURCE_CONSUMER_GROUP_ID); // 消费者组id
        props.setProperty("f1ink.partition-discovery.interval-millis", "5000"); //会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(SOURCE_TOPIC_NAME, new SimpleStringSchema(), props);
//        consumer.setStartFromGroupOffsets();
//        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));
        return env.addSource(consumer);
    }

    /**
     * 把用户的分析数据sink到kafka
     *
     * @return
     */
    public static DataStreamSink<String> sinkUserData2Kafka(DataStream<String> ds) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(SINK_USER_DATA_TOPIC_NAME, new SimpleStringSchema(), properties);
        return ds.addSink(producer).name("Sink User Data!");
    }

    /**
     * 把全局的分析数据sink到kafka
     *
     * @return
     */
    public static DataStreamSink<String> sinkCountData2Kafka(DataStream<String> ds) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(SINK_COUNT_DATA_TOPIC_NAME, new SimpleStringSchema(), properties);
        return ds.addSink(producer).name("Sink User Data!");
    }
}
