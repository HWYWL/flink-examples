package com.yi.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * @author: YI
 * @description: 将小写字母转为大写，并保留小写字母，然后打印字符串长度大于5的单词
 * @date: create in 2020/9/10 10:15
 */

public class StreamingFilter {

    public static void main(String[] args) throws Exception {
        String[] words = {"apple","orange","banana","watermelon"};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源 DataSource
        DataStreamSource<String> dataStreamSource = env.fromElements(words);

        // 转换 Transformations。对DataStream中的每一个元素都会调用FlatMapFunction的接口具体实现类，
        // flatMap方法可以返回任意多个元素，也可以不返回。
        DataStream<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
                out.collect(value.toUpperCase());
            }
        });

        // 只有单词长度大于5的才会被筛选出来
        DataStream<String> filter = flatMap.filter(value -> value.length() > 5);

        // 输出 Sinks
        filter.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
