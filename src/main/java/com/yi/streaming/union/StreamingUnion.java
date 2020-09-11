package com.yi.streaming.union;

import com.yi.streaming.union.datasource.ExchangeRateDataSource;
import com.yi.streaming.union.pojo.CurrencyType;
import com.yi.streaming.union.pojo.ExchangeRateInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: YI
 * @description: union 流合并， 将多个流合并为一个流
 * @date: create in 2020/9/10 10:15
 */
public class StreamingUnion {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //USD -> CNY 汇率流
        DataStreamSource<ExchangeRateInfo> usdToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.USD, CurrencyType.CNY, 7, 6), "USD-CNY");
        //EUR -> CNY 汇率流
        DataStreamSource<ExchangeRateInfo> eurToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.EUR, CurrencyType.CNY, 8, 7), "EUR-CNY");
        //AUD -> CNY 汇率流
        DataStreamSource<ExchangeRateInfo> audToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.AUD, CurrencyType.CNY, 5, 4), "AUD-CNY");

        // 把三个流合一
        DataStream<ExchangeRateInfo> allExchangeRate = usdToCny.union(eurToCny).union(audToCny);

        // 输出到控制台
        allExchangeRate.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
