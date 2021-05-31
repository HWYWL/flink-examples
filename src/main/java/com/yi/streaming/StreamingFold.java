//package com.yi.streaming;
//
//import org.apache.flink.api.common.functions.FoldFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//import java.util.HashMap;
//import java.util.Random;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author: YI
// * @description: 统计商品的数量和名称, 并输出所有的分类汇总
// * @date: create in 2020/9/10 10:15
// */
//
//public class StreamingFold {
//    private static final String[] TYPE = {"苹果", "梨", "西瓜", "葡萄", "火龙果"};
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 数据源 DataSource
//        DataStreamSource<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
//            private volatile boolean isRunning = true;
//            private final Random random = new Random();
//
//            @Override
//            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
//                while (isRunning) {
//                    // 每秒钟产生一个商品数据
//                    TimeUnit.SECONDS.sleep(1);
//                    ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], 1));
//                }
//            }
//
//            @Override
//            public void cancel() {
//                isRunning = false;
//            }
//        }, "orderInfo");
//
//        orderSource
//                // 按照第一个字段区分商品(商品名称)
//                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> "")
//                // 使用HashMap作为累加器
//                .fold(new HashMap<>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
//                    @Override
//                    public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
//                        accumulator.put(value.f0, accumulator.getOrDefault(value.f0, 0) + value.f1);
//                        return accumulator;
//                    }
//                })
//                // 把数据打印到控制台
//                .print();
//
//        env.execute("Flink Streaming Java API Skeleton Hello");
//    }
//}
