package com.yi.streaming.split;

import com.yi.streaming.split.datasource.ExchangeRateDataSource;
import com.yi.streaming.split.datasource.OrderDataSource;
import com.yi.streaming.split.pojo.CurrencyType;
import com.yi.streaming.split.pojo.ExchangeRateInfo;
import com.yi.streaming.split.pojo.OrderInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: YI
 * @description: connect - 将订单流和汇率流合成的流，拆分为两个流
 * @date: create in 2020-9-11 17:51:05
 */
public class StreamingSplit {
    private static final OutputTag<String> EXCHANGE_RATE_INFO = new OutputTag<>("exchangeRateInfo", TypeInformation.of(String.class));
    private static final OutputTag<String> ORDER_INFO = new OutputTag<>("orderInfo", TypeInformation.of(String.class));

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

        // 两个流合成一个
        DataStream<String> map = cnyToUsd.connect(orderDs)
                .map(new CoMapFunction<ExchangeRateInfo, OrderInfo, String>() {
                    @Override
                    public String map1(ExchangeRateInfo value) throws Exception {
                        return value.toString();
                    }

                    @Override
                    public String map2(OrderInfo value) throws Exception {
                        return value.toString();
                    }
                });

        // 分流
        SingleOutputStreamOperator<Object> sideOutStream = sideOutStream(map);
        // 获取不同的流
        DataStream<String> exchangeRateInfoSideOutput = sideOutStream.getSideOutput(EXCHANGE_RATE_INFO);
        DataStream<String> orderInfoSideOutput = sideOutStream.getSideOutput(ORDER_INFO);
        exchangeRateInfoSideOutput.print();
        orderInfoSideOutput.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }

    /**
     * 拆分不同的流
     *
     * @param rawLogStream 流
     * @return
     */
    private static SingleOutputStreamOperator<Object> sideOutStream(DataStream<String> rawLogStream) {
        return rawLogStream.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
                // 根据日志等级，给对象打上不同的标记
                if (value.startsWith("OrderInfo")) {
                    ctx.output(ORDER_INFO, value);
                } else {
                    ctx.output(EXCHANGE_RATE_INFO, value);
                }
            }
        });
    }
}
