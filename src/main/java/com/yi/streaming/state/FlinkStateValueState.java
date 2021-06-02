package com.yi.streaming.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: state用来存放计算过程的节点中间结果或元数据等, 方便增量计算
 * @date: create in 2021/6/2 11:35
 */
public class FlinkStateValueState {
    private static final String[] TYPE = {"苹果", "梨", "西瓜", "葡萄", "火龙果"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 数据源 DataSource
        DataStreamSource<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    // 每秒钟产生N个商品数据
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], random.nextInt(TYPE.length)));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, "orderInfo");

        // 查看所有生成的数据
//        orderSource.keyBy(t ->t.f0).addSink(new SinkFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//                SinkFunction.super.invoke(value, context);
//                System.out.println("数据：" + value.f0 + value.f1);
//            }
//        });

        // Window：把不同的key分为不同的窗口、windowAll：把所有key聚合为一个窗口
        // 由Flink自动维护状态
        orderSource.keyBy(t -> t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).max(1).print("Flink自动维护：");

        orderSource.keyBy(t -> t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply(new MaxWindowFunction()).print("手动维护：");
        env.execute("Flink Streaming Java API Skeleton Hello");
    }

    /**
     * 手动维护状态
     * IN, OUT, KEY, Window
     */
    private static class MaxWindowFunction extends RichWindowFunction<Tuple2<String, Integer>, Map<String, Integer>, String, TimeWindow> {
        private ValueState<Integer> maxState = null;

        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Map<String, Integer>> out) throws Exception {
            // 因为使用的是Window窗口，所以每次调用需要先清除以前数据，防止key混杂的问题
            maxState.clear();
            for (Tuple2<String, Integer> tuple2 : input) {
                Integer current = tuple2.f1;
                Integer value = maxState.value();
                if (value == null || value < current) {
                    maxState.update(current);
                }
            }
            Map<String, Integer> map = new HashMap<>();
            map.put(key, maxState.value());
            out.collect(map);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> maxStateDescriptor = new ValueStateDescriptor<>("maxState", Integer.class);
            // 初始化状态缓存
            maxState = getRuntimeContext().getState(maxStateDescriptor);
        }
    }
}
