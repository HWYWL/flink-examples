package com.yi.streaming.intervaljoin;

import com.yi.streaming.intervaljoin.datasource.ExchangeRateDataSource;
import com.yi.streaming.intervaljoin.datasource.OrderDataSource;
import com.yi.streaming.intervaljoin.pojo.CurrencyType;
import com.yi.streaming.intervaljoin.pojo.ExchangeRateInfo;
import com.yi.streaming.intervaljoin.pojo.OrderInfo;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: YI
 * @description: interval join 无窗口关联，只关联做元素between范围内的右元素
 * 每秒产生一条汇率，每10秒产生一条订单数据，实时转换订单汇率
 * @date: create in 2020-9-11 15:24:55
 */
public class StreamingJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置每个事件时间独立,时间由自己指定
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // CNY -> USD 汇率流
        SingleOutputStreamOperator<ExchangeRateInfo> cnyToUsd = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6), "CNY-USD")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ExchangeRateInfo>(Time.milliseconds(100)) {
                    @Override
                    public long extractTimestamp(ExchangeRateInfo element) {
                        return element.getTimeStamp().getTime();
                    }
                });

        // 订单流
        SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.milliseconds(100)) {
                    @Override
                    public long extractTimestamp(OrderInfo element) {
                        return element.getTimeStamp().getTime();
                    }
                });

        KeyedStream<ExchangeRateInfo, Object> cnyToUsdKeyedStream = cnyToUsd.keyBy((KeySelector<ExchangeRateInfo, Object>) value -> value.getFrom());
        KeyedStream<OrderInfo, Object> orderDsKeyedStream = orderDs.keyBy((KeySelector<OrderInfo, Object>) value -> value.getCurrencyType());

        // 订单流 interval Join 汇率流
        cnyToUsdKeyedStream.intervalJoin(orderDsKeyedStream)
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                // 设置上界不包含边界
                .upperBoundExclusive()
                // 设置下界不包含边界
                .lowerBoundExclusive()
                // 类似apply 对数据加工
                .process(new ProcessJoinFunction<ExchangeRateInfo, OrderInfo, Object>() {
                    @Override
                    public void processElement(ExchangeRateInfo exchangeRateInfo, OrderInfo orderInfo, Context ctx, Collector<Object> out) throws Exception {
                        Map<String, Object> map = new HashMap<>(3);
                        map.put("coefficient", exchangeRateInfo.getCoefficient());
                        map.put("TotalAmt", orderInfo.getTotalAmt());

                        // 按照汇率转换
                        map.put("exchange", orderInfo.getTotalAmt().divide(exchangeRateInfo.getCoefficient(), 2, BigDecimal.ROUND_HALF_UP).toPlainString());

                        out.collect(map);
                    }
                })
                .print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
