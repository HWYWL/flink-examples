package com.yi.streaming.project.datasource;

import com.yi.streaming.project.pojo.CurrencyType;
import com.yi.streaming.project.pojo.Goods;
import com.yi.streaming.project.pojo.GoodsType;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description:
 * @date: create in 2020/9/11 11:56
 */
public class OrderDataSource implements SourceFunction<Tuple5<String, GoodsType, BigDecimal, CurrencyType, Integer>> {
    private static final long serialVersionUID = -218080338675267439L;
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    public Goods getRandomGoods() {
        GoodsType[] goodsTypes = GoodsType.values();
        BigDecimal unitPrice = new BigDecimal(random.nextDouble() * 100).setScale(2, BigDecimal.ROUND_HALF_UP);
        int num = random.nextInt(20);
        return new Goods(UUID.randomUUID().toString(), goodsTypes[random.nextInt(goodsTypes.length)], unitPrice,
                CurrencyType.CNY, num);
    }

    @Override
    public void run(SourceContext<Tuple5<String, GoodsType, BigDecimal, CurrencyType, Integer>> ctx) throws Exception {
        while (isRunning) {
            TimeUnit.SECONDS.sleep(1);
            Goods randomGoods = getRandomGoods();
            ctx.collect(Tuple5.of(randomGoods.getGoodsNo(), randomGoods.getGoodsType(), randomGoods.getUnitPrice(), randomGoods.getCurrencyType(), randomGoods.getNum()));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
