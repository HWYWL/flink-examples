package com.yi.streaming.sql;

import com.yi.streaming.sql.datasource.OrderDataSource;
import com.yi.streaming.sql.pojo.OrderInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: YI
 * @description: 使用SQL，像查表一样查询数据
 * @date: create in 2021-6-2 18:32:11
 */
public class FlinkSQLUnion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 订单流
        DataStreamSource<OrderInfo> orderDs1 = env.addSource(new OrderDataSource());
        DataStreamSource<OrderInfo> orderDs2 = env.addSource(new OrderDataSource());

        // 将DataStream转为视图
        tableEnv.createTemporaryView("order_info", orderDs1);

        // 将DataStream转为table
        Table scTable = tableEnv.fromDataStream(orderDs2);

        String sql = "SELECT * FROM " + scTable + " WHERE totalAmt >= 20 UNION ALL SELECT * FROM order_info WHERE  totalAmt >= 25";

        // union 视图与表
        Table resultTable = tableEnv.sqlQuery(sql);

        // 打印结果
        DataStream<OrderInfo> result = tableEnv.toAppendStream(resultTable, TypeInformation.of(OrderInfo.class));
        result.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
