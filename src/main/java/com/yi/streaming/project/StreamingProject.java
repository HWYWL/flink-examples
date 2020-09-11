package com.yi.streaming.project;

import com.yi.streaming.project.datasource.OrderDataSource;
import com.yi.streaming.project.pojo.CurrencyType;
import com.yi.streaming.project.pojo.GoodsType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @author: YI
 * @description: 定制流字段，只能用于tuple类型
 * @date: create in 2020-9-11 17:51:05
 */
public class StreamingProject {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple5<String, GoodsType, BigDecimal, CurrencyType, Integer>> dataStreamSource = env.addSource(new OrderDataSource());

        // 只要0、2、4索引之下的数据
        SingleOutputStreamOperator<Tuple> project = dataStreamSource.project(0, 2, 4);

        project.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
