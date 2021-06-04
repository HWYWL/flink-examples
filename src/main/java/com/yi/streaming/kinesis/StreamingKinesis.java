package com.yi.streaming.kinesis;


import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/**
 * @author: YI
 * @description: 使用kinesis作为数据源
 * @date: create in 2021-6-3 17:48:09
 */
public class StreamingKinesis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源 DataSource
        Properties consumerConfig = new Properties();
        consumerConfig.put(AWSConfigConstants.AWS_REGION, "cn-northwest-1");
        // 默认使用系统凭证，即授权给EC2之类的，本地可以使用授权码
        if (SystemUtils.IS_OS_WINDOWS) {
            consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "xxxxxxxxxxxx");
            consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "xxxxxxxxxxxxxxx");
        }

        // LATEST、TRIM_HORIZON、AT_TIMESTAMP
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        consumerConfig.putIfAbsent(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, "EFO");
        consumerConfig.putIfAbsent(ConsumerConfigConstants.EFO_CONSUMER_NAME, "kinesis-ikylin-application-flink-test");

        DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>("ikylin-test-common-kinesis", new SimpleStringSchema(), consumerConfig));

        kinesis.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
