package com.yi.streaming.state;

import cn.hutool.core.util.RandomUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 使用MapState来维持中间数据，方便增量计算，这里用来做一个金币防作弊的功能并且带有过期的功能
 * @date: create in 2021/6/2 15:47
 */
public class FlinkStateMapStateTTL {
    private static final String[] TYPE = {"张三", "李四", "王五", "赵六", "火龙果"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<CurrencyChanges> source = env.addSource(new GoldSourceFunction());
        WindowedStream<CurrencyChanges, String, GlobalWindow> windowedStream = source.keyBy(CurrencyChanges::getUserName).countWindow(1);

        windowedStream.apply(new CheatingAlarmWindow()).print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }

    /**
     * 作弊功能窗口
     * IN, OUT, KEY, W extends Window
     */
    public static class CheatingAlarmWindow extends RichWindowFunction<CurrencyChanges, String, String, GlobalWindow> {
        MapState<String, CurrencyChanges> cheatState;

        @Override
        public void apply(String key, GlobalWindow window, Iterable<CurrencyChanges> input, Collector<String> out) throws Exception {
            for (CurrencyChanges changes : input) {
                String userName = changes.getUserName();
                CurrencyChanges currencyChanges = cheatState.get(userName);

                // 为空表示此人从未有过作弊行为
                if (currencyChanges == null) {
                    // 发现作弊
                    if (changes.getActualGoldCoins() > changes.getLimitGoldCoins()){
                        cheatState.put(userName, changes);
                        out.collect("发现作弊者：" + changes);
                    }
                }else {
                    // 发现再次作弊
                    if (changes.getActualGoldCoins() > changes.getLimitGoldCoins()){
                        cheatState.put(userName, changes);
                        out.collect("发现再次作弊者：" + changes + "\n" +
                                "上次作弊数据：" + currencyChanges);
                    }
                }
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态有效时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(3))
                    //设置状态更新类型
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            MapStateDescriptor<String, CurrencyChanges> mapStateDescriptor = new MapStateDescriptor<>("cheatState",
                    TypeInformation.of(String.class), TypeInformation.of(CurrencyChanges.class));
            // 设置过期配置
            mapStateDescriptor.enableTimeToLive(ttlConfig);

            // 初始化State
            this.cheatState = getRuntimeContext().getMapState(mapStateDescriptor);
        }
    }

    /**
     * 一秒自动一个产生数据
     */
    public static class GoldSourceFunction implements SourceFunction<CurrencyChanges> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<CurrencyChanges> ctx) throws Exception {
            while (isRunning) {
                TimeUnit.SECONDS.sleep(1);

                CurrencyChanges build = CurrencyChanges.builder()
                        .userName(TYPE[RandomUtil.randomInt(TYPE.length)])
                        .date(Integer.parseInt(LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)))
                        .limitGoldCoins(100).actualGoldCoins(RandomUtil.randomInt(96, 120)).build();

                ctx.collect(build);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
