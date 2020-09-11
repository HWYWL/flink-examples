package com.yi.streaming.connect.datasource;

import com.yi.streaming.connect.pojo.CurrencyType;
import com.yi.streaming.connect.pojo.ExchangeRateInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 每10秒产生一个随机的汇率
 * @date: create in 2020/9/11 11:26
 */
public class ExchangeRateDataSource implements SourceFunction<ExchangeRateInfo> {
    private static final long serialVersionUID = 4836546999687545904L;
    private volatile boolean isRunning = true;

    private CurrencyType from;
    private CurrencyType to;
    private int max = 0;
    private int min = 0;

    public ExchangeRateDataSource(CurrencyType from, CurrencyType to, int max, int min) {
        this.from = from;
        this.to = to;
        this.max = max;
        this.min = min;
    }

    @Override
    public void run(SourceContext<ExchangeRateInfo> ctx) throws Exception {
        while (isRunning) {
            TimeUnit.SECONDS.sleep(10);

            BigDecimal bigDecimal = new BigDecimal(min + ((max - min) * new Random().nextFloat())).setScale(2, BigDecimal.ROUND_HALF_UP);
            ExchangeRateInfo exchangeRateInfo = new ExchangeRateInfo(from, to, bigDecimal, new Date());

            ctx.collectWithTimestamp(exchangeRateInfo, System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
