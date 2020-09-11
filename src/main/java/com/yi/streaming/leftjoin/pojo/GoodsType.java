package com.yi.streaming.leftjoin.pojo;

/**
 * @author: YI
 * @description: 商品类型
 * @date: create in 2020-9-11 11:25:30
 */
public enum GoodsType {
    apple("苹果"), pear("梨"), grape("葡萄"), watermellon("西瓜"), pitaya("火龙果");

    private final String name;

    GoodsType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
