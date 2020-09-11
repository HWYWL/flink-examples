package com.yi.streaming.intervaljoin.pojo;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author: YI
 * @description: 汇率信息
 * @date: create in 2020-9-11 11:25:30
 */
public class ExchangeRateInfo {
    private CurrencyType from;
    private CurrencyType to;
    private BigDecimal coefficient;
    private Date timeStamp;

    public ExchangeRateInfo(CurrencyType from, CurrencyType to, BigDecimal coefficient, Date timeStamp) {
        this.from = from;
        this.to = to;
        this.coefficient = coefficient;
        this.timeStamp = timeStamp;
    }

    public CurrencyType getFrom() {
        return from;
    }

    public void setFrom(CurrencyType from) {
        this.from = from;
    }

    public CurrencyType getTo() {
        return to;
    }

    public void setTo(CurrencyType to) {
        this.to = to;
    }

    public BigDecimal getCoefficient() {
        return coefficient;
    }

    public void setCoefficient(BigDecimal coefficient) {
        this.coefficient = coefficient;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "ExchangeRateInfo [from=" + from + ", to=" + to + ", coefficient=" + coefficient + ", timeStamp="
                + timeStamp + "]";
    }
}
