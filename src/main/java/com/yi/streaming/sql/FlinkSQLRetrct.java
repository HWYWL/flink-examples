package com.yi.streaming.sql;

import com.yi.streaming.sql.datasource.OrderDataSource;
import com.yi.streaming.sql.pojo.OrderInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: YI
 * @description: 使用SQL，像查表一样查询数据,可以聚合更新
 * @date: create in 2021-7-20 11:39:56
 */
public class FlinkSQLRetrct {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 订单流
        DataStreamSource<OrderInfo> streamSource = env.addSource(new OrderDataSource());
        // 查询方式一，将DataStream转为视图
        tableEnv.createTemporaryView("order_info", streamSource);
        Table table = tableEnv.sqlQuery("select * from order_info where totalAmt>=20 ");
        DataStream<Tuple2<Boolean, OrderInfo>> retractStream = tableEnv.toRetractStream(table, OrderInfo.class);
        retractStream.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
