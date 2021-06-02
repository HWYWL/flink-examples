package com.yi.streaming.sql;

import com.yi.streaming.sql.datasource.OrderDataSource;
import com.yi.streaming.sql.pojo.OrderInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 表操作数据
 *
 * @author Administrator
 */
public class FlinkTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 订单流
        DataStreamSource<OrderInfo> streamSource = env.addSource(new OrderDataSource());

        // 将DataStream转为table
        Table scTable = tableEnv.fromDataStream(streamSource);
        Table table = scTable.select($("orderId"), $("timeStamp"), $("totalAmt"), $("goods"), $("currencyType"))
                .filter($("totalAmt").isGreater(2000));
        DataStream<OrderInfo> result = tableEnv.toAppendStream(table, OrderInfo.class);
        result.print();

        env.execute("Flink Streaming Java API Skeleton Hello");
    }
}
