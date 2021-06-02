package com.yi.streaming.sql;

import com.yi.streaming.sql.datasource.OrderDataSource;
import com.yi.streaming.sql.pojo.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author: YI
 * @description: 使用SQL，像查表一样查询数据
 * @date: create in 2021-6-2 18:32:11
 */
public class FlinkSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 订单流
        DataStreamSource<OrderInfo> streamSource = env.addSource(new OrderDataSource());
        // 查询方式一，将DataStream转为视图
//        tableEnv.createTemporaryView("order_info", streamSource);
//        Table table = tableEnv.sqlQuery("select * from order_info where totalAmt>=20");
//        DataStream<OrderInfo> orderInfoDataStream = tableEnv.toAppendStream(table, OrderInfo.class);
//        orderInfoDataStream.print();

        // 查询方式二，将DataStream转为table
        Table scTable = tableEnv.fromDataStream(streamSource);
        Table resultTable = tableEnv.sqlQuery("select * from " + scTable + " where totalAmt>=20");
        DataStream<OrderInfo> result = tableEnv.toAppendStream(resultTable, TypeInformation.of(OrderInfo.class));
        result.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
