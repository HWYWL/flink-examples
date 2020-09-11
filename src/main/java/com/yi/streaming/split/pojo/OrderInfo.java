package com.yi.streaming.split.pojo;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * @author: YI
 * @description: 订单详情
 * @date: create in 2020-9-11 11:25:30
 */
public class OrderInfo {
    private String orderId;
    private Date timeStamp;
    private BigDecimal totalAmt;
    private List<Goods> goods;
    private CurrencyType currencyType;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public BigDecimal getTotalAmt() {
        return totalAmt;
    }

    public void setTotalAmt(BigDecimal totalAmt) {
        this.totalAmt = totalAmt;
    }

    public List<Goods> getGoods() {
        return goods;
    }

    public void setGoods(List<Goods> goods) {
        this.goods = goods;
    }

    public CurrencyType getCurrencyType() {
        return currencyType;
    }

    public void setCurrencyType(CurrencyType currencyType) {
        this.currencyType = currencyType;
    }

    @Override
    public String toString() {
        return "OrderInfo [orderId=" + orderId + ", timeStamp=" + timeStamp + ", totalAmt=" + totalAmt + ", goods="
                + goods + ", currencyType=" + currencyType + "]";
    }


}
