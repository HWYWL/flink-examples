package com.yi.streaming.join;

import com.yi.streaming.join.datasource.ExchangeRateDataSource;
import com.yi.streaming.join.datasource.OrderDataSource;
import com.yi.streaming.join.pojo.CurrencyType;
import com.yi.streaming.join.pojo.ExchangeRateInfo;
import com.yi.streaming.join.pojo.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: YI
 * @description: join 关联流，关联订单和汇率，实时计算当前订单的外汇金额
 * @date: create in 2020-9-11 11:47:56
 */
public class StreamingJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置每个事件时间独立,时间由自己指定
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // CNY -> USD 汇率流
        SingleOutputStreamOperator<ExchangeRateInfo> cnyToUsd = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6), "CNY-USD")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        // 订单流
        SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        // 订单流 inner join 汇率流
        orderDs.join(cnyToUsd)
                .where((KeySelector<OrderInfo, Object>) orderInfo -> orderInfo.getCurrencyType())
                .equalTo((KeySelector<ExchangeRateInfo, Object>) exchangeRateInfo -> exchangeRateInfo.getFrom())

                // 转换10秒窗口期的订单为美元
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 将订单金额人民币转换为美元
                .apply((JoinFunction<OrderInfo, ExchangeRateInfo, Object>) (orderInfo, exchangeRateInfo) -> {
                    Map<String, Object> map = new HashMap<>(3);

                    map.put("coefficient", exchangeRateInfo.getCoefficient());
                    map.put("TotalAmt", orderInfo.getTotalAmt());
                    // 按照汇率转换
                    map.put("exchange", orderInfo.getTotalAmt().divide(exchangeRateInfo.getCoefficient(), 2, BigDecimal.ROUND_HALF_UP).toPlainString());

                    return map;
                })
                .print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
