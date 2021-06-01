package com.yi.streaming.leftjoin;

import com.yi.streaming.leftjoin.datasource.ExchangeRateDataSource;
import com.yi.streaming.leftjoin.datasource.OrderDataSource;
import com.yi.streaming.leftjoin.pojo.CurrencyType;
import com.yi.streaming.leftjoin.pojo.ExchangeRateInfo;
import com.yi.streaming.leftjoin.pojo.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * coGroup - 类似于left join
 *
 * @author: YI
 * @description: 汇率流由两种汇率构成，将订单流与cny->usd、cny->eur流相关联。如果在窗口期内能关联到则输出转换为美元的金额，如果关联不到则输出人民币金额。
 * 可以看出join 实际是  coGroup的一种特殊情况
 * @date: create in 2020-9-11 15:24:55
 */
public class StreamingLeftJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置每个事件时间独立,时间由自己指定
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // CNY -> USD 汇率流
        SingleOutputStreamOperator<ExchangeRateInfo> cnyToUsd = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6), "CNY-USD")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        // CNY -> USD 汇率流
        SingleOutputStreamOperator<ExchangeRateInfo> cnyToEur = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.EUR, 8, 7), "CNY-USD")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        // 订单流
        SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        DataStream<ExchangeRateInfo> union = cnyToUsd.union(cnyToEur);

        // left join
        orderDs.coGroup(union)
                // where和equalTo分别指定第一个和第二个输入的KeySelector，必须配对使用
                .where((KeySelector<OrderInfo, Object>) value -> value.getCurrencyType())
                .equalTo((KeySelector<ExchangeRateInfo, Object>) value -> value.getFrom())
                // 10秒窗口期
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

                // 数据处理
                .apply(new CoGroupFunction<OrderInfo, ExchangeRateInfo, Object>() {
                    @Override
                    public void coGroup(Iterable<OrderInfo> first, Iterable<ExchangeRateInfo> second, Collector<Object> out) throws Exception {
                        ExchangeRateInfo cnyToUsdExchange = null;

                        // 获取与美元汇率匹配的数据（汇率随机产生，流中不一定会有）,不过我们这个测试流中会有
                        for (ExchangeRateInfo exchangeRateInfo : second) {
                            if (exchangeRateInfo.getTo() == CurrencyType.USD) {
                                cnyToUsdExchange = exchangeRateInfo;
                            }
                        }

                        for (OrderInfo orderInfo : first) {
                            Map<String, Object> map = new HashMap<>(3);
                            map.put("TotalAmt", orderInfo.getTotalAmt());

                            if (cnyToUsdExchange != null) {
                                map.put("coefficient", cnyToUsdExchange.getCoefficient());
                                // 按照汇率转换
                                map.put("exchange", "$" + orderInfo.getTotalAmt().divide(cnyToUsdExchange.getCoefficient(), 2, BigDecimal.ROUND_HALF_UP).toPlainString());
                            } else {
                                map.put("coefficient", null);
                                // 按照汇率转换
                                map.put("exchange", "￥" + orderInfo.getTotalAmt().toPlainString());
                            }

                            out.collect(map);
                        }
                    }
                }).print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
