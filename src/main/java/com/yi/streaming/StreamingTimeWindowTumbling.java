package com.yi.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 统计商品的数量和名称, 并输出所有的分类汇总,时间窗口分类统计10s时间窗口内的订单量。
 * @date: create in 2020/9/10 10:15
 */

public class StreamingTimeWindowTumbling {
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

        orderSource.keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                // 每10秒统计一次最近1分钟内的订单数量
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                // 使用HashMap作为累加器
                .aggregate(new CountAggregateFunction())
                // 把数据打印到控制台
                .print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }

    /**
     * 累加统计逻辑
     */
    public static class CountAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>> {

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(Tuple2<String, Integer> value, HashMap<String, Integer> accMap) {
            // 数据累加计算
            int count = accMap.get(value.f0) == null ? 0 : accMap.get(value.f0);
            accMap.put(value.f0, count + value.f1);
            return accMap;
        }

        @Override
        public HashMap<String, Integer> getResult(HashMap<String, Integer> accMap) {
            return accMap;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> acc1, HashMap<String, Integer> acc2) {
            return null;
        }
    }
}
