package com.yi.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 统计商品的数量和名称, 并输出所有的分类汇总,每五个订单统一次前面20个订单的数据（超出20个的会被丢弃）。
 * @date: create in 2020/9/10 10:15
 */

public class StreamingCountWindowNumber {
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


                // 五个订单统计一次前面20个订单的数据
        orderSource.countWindowAll(20, 5).aggregate(new CountAggregateFunction()).print();

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
            accMap.put(value.f0, accMap.getOrDefault(value.f0, 0) + value.f1);
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
