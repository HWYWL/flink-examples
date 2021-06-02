package com.yi.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 统计商品的数量和名称, 并输出所有的分类汇总
 * @date: create in 2020/9/10 10:15
 */

public class StreamingFold {
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


        // 按照商品名称进行统计
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = orderSource.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0);
        // 汇总求和统计
        keyedStream.sum(1).print();
        // 转为Map输出
//        keyedStream.sum(1).map((MapFunction<Tuple2<String, Integer>, Map<String, Integer>>) value -> {
//            Map<String, Integer> map = new HashMap<>();
//            map.put(value.f0, value.f1);
//            return map;
//        }).print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
