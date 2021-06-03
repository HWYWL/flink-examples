package com.yi.practice.datasource;

import com.yi.practice.pojo.CurrencyType;
import com.yi.practice.pojo.Goods;
import com.yi.practice.pojo.GoodsType;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description: 随机生成订单
 * @date: create in 2021-6-3 11:02:09
 */
public class OrderDataSource implements SourceFunction<Goods> {
    private static final long serialVersionUID = -218080338675267439L;
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    private Goods getRandomGoods() {
        GoodsType[] goodsTypes = GoodsType.values();
        BigDecimal unitPrice = new BigDecimal(random.nextDouble() * 100).setScale(2, BigDecimal.ROUND_HALF_UP);
        int num = random.nextInt(20);
        return new Goods(UUID.randomUUID().toString(), goodsTypes[random.nextInt(goodsTypes.length)], unitPrice,
                CurrencyType.CNY, num);
    }

    @Override
    public void run(SourceContext<Goods> ctx) throws Exception {
        while (isRunning) {
            TimeUnit.MILLISECONDS.sleep(20);
            Goods randomGoods = getRandomGoods();
            ctx.collect(randomGoods);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
