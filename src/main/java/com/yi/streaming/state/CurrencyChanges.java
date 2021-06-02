package com.yi.streaming.state;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: YI
 * @description: 用户货币变化
 * @date: create in 2021/6/2 15:43
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class CurrencyChanges {
    /**
     * 用户名
     */
    private String userName;

    /**
     * 产生时间
     */
    private Integer date;

    /**
     * 限制最大产生的金币数
     */
    private Integer limitGoldCoins;

    /**
     * 实际产生的金币数
     */
    private Integer actualGoldCoins;
}
