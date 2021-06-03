package com.yi.practice.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: YI
 * @description: 存储一天计算的聚合结果
 * @date: create in 2021/6/3 11:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CategoryPojo {
    /**
     * 分类名称
     */
    private String category;
    /**
     * 该分类总销售额
     */
    private double totalPrice;
    /**
     * 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
     */
    private String dateTime;
}
