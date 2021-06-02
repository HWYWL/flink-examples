package com.yi.streaming.sql.datasource;

import com.yi.streaming.sql.pojo.CurrencyType;
import com.yi.streaming.sql.pojo.Goods;
import com.yi.streaming.sql.pojo.GoodsType;
import com.yi.streaming.sql.pojo.OrderInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: YI
 * @description:
 * @date: create in 2020/9/11 11:56
 */
public class OrderDataSource implements SourceFunction<OrderInfo> {
    private static final long serialVersionUID = -218080338675267439L;
    private volatile boolean isRunning = true;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private final Random random = new Random();

    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        while (isRunning) {
            TimeUnit.SECONDS.sleep(1);

            Date date = new Date();
            String orderId = sdf.format(date) + String.format("%03d", random.nextInt(1000));

            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(orderId);
            orderInfo.setTimeStamp(date);

            orderInfo = getRandomGoods(orderInfo);

            ctx.collect(orderInfo);
        }
    }

    /**
     * 获取随机订单
     *
     * @param orderInfo 订单
     * @return
     */
    private OrderInfo getRandomGoods(OrderInfo orderInfo) {
        List<Goods> goodsList = new ArrayList<>();
        GoodsType[] goodsTypes = GoodsType.values();
        BigDecimal totalAmt = BigDecimal.ZERO;

        for (int i = 0, size = random.nextInt(10); i < size; i++) {
            BigDecimal unitPrice = BigDecimal.valueOf(random.nextDouble() * 100).setScale(2, BigDecimal.ROUND_HALF_UP);
            int num = random.nextInt(20);
            goodsList.add(new Goods(goodsTypes[random.nextInt(goodsTypes.length)], unitPrice, CurrencyType.CNY, num));
            totalAmt = totalAmt.add(unitPrice.multiply(BigDecimal.valueOf(num)));
        }

        orderInfo.setGoods(goodsList);
        orderInfo.setTotalAmt(totalAmt);
        orderInfo.setCurrencyType(CurrencyType.CNY);

        return orderInfo;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
