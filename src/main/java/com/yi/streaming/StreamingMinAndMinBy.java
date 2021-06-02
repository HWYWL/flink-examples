package com.yi.streaming;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: min与minBy的区别：min只返回最小数值而不带订单名，minBy既返回最小数值又返回此数值的订单名。
 * @date: create in 2020/9/10 10:15
 */

public class StreamingMinAndMinBy {
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
                    // 每秒钟产生N个商品数据
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], random.nextInt(10)));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, "orderInfo");

        WindowedStream<Tuple2<String, Integer>, Object, TimeWindow> windowedStream = orderSource.keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                // 每10秒统计一次1分钟内的订单
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)));

        // Tuple2索引0为商品名称，索引1为商品数量 统计索引为1的最小数量
        windowedStream.min(1).print("Min==>");
//                .minBy(1)
        System.out.println();
        windowedStream.max(1).print("Max==>");


        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
