package com.yi.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author: YI
 * @description: 将小写字母转为大写
 * @date: create in 2020/9/10 10:15
 */

public class StreamingMap {

    public static void main(String[] args) throws Exception {
        String[] words = {"apple","orange","banana","watermelon"};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源 DataSource
        DataStreamSource<String> dataStreamSource = env.fromElements(words);

        // 转换 Transformations。对DataStream中的每一个元素都会调用MapFunction的接口具体实现类，
        // map方法仅仅只是返回一个元素。
        DataStream<String> map = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        // 输出 Sinks
        map.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
