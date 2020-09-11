package com.yi.streaming;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 统计商品的数量和名称
 * @date: create in 2020/9/10 10:15
 */

public class StreamingReduce {
    private static final String[] TYPE = {"苹果", "梨", "西瓜", "葡萄", "火龙果"};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源 DataSource
        DataStreamSource<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    // 每秒钟产生一个商品数据
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], 1));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, "orderInfo");

        orderSource
                // 按照第一个字段区分商品(商品名称)
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                // 将上一个元素与当前元素相加，返回给下一个元素
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                // 把数据打印到控制台
                .print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
