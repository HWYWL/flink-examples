package com.yi.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 统计商品的数量和名称, 并输出所有的分类汇总,时间窗口分类统计，apply会对窗口内的数据进行处理。
 * @date: create in 2020/9/10 10:15
 */

public class StreamingApply {
    private static final String[] TYPE = {"苹果", "梨", "西瓜"};

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
                // 每10秒统计一次订单数量
                .timeWindowAll(Time.seconds(10))
                // apply会对窗口内的数据进行处理，这里我们过滤掉除苹果以外的商品信息
                .apply(new AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Object> out) throws Exception {
                        values.forEach(e ->{
                            if (e.f0.equals(TYPE[2])){
                                out.collect(e);
                            }
                        });
                    }
                })
                // 把数据打印到控制台
                .print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
