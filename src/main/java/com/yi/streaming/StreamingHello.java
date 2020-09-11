package com.yi.streaming;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author: YI
 * @description: 一个简单的hello
 * @date: create in 2020/9/10 10:15
 */
public class StreamingHello {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源 DataSource
        DataStreamSource<String> dataStreamSource = env.fromElements("Hello world");

        // 转换 Transformations

        // 输出 Sinks
        dataStreamSource.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
