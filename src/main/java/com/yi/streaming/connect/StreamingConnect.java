package com.yi.streaming.connect;

import com.yi.streaming.connect.datasource.ExchangeRateDataSource;
import com.yi.streaming.connect.datasource.OrderDataSource;
import com.yi.streaming.connect.pojo.CurrencyType;
import com.yi.streaming.connect.pojo.ExchangeRateInfo;
import com.yi.streaming.connect.pojo.OrderInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: YI
 * @description: connect - 将订单流和汇率流合为一个流，并转换为字符串流
 * @date: create in 2020-9-11 15:24:55
 */
public class StreamingConnect {

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

        cnyToUsd.connect(orderDs)
                .map(new CoMapFunction<ExchangeRateInfo, OrderInfo, Object>() {
                    @Override
                    public Object map1(ExchangeRateInfo value) throws Exception {
                        return value.toString();
                    }

                    @Override
                    public Object map2(OrderInfo value) throws Exception {
                        return value.toString();
                    }
                }).print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
