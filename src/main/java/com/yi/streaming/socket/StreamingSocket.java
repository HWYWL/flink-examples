//package com.yi.streaming.socket;
//
//import cn.hutool.core.lang.Dict;
//import cn.hutool.core.util.StrUtil;
//import cn.hutool.json.JSONUtil;
//import com.yi.streaming.socket.pojo.KinesisData;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FoldFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.HashMap;
//
///**
// * @author: YI
// * @description: 统计商品的数量和名称, 并输出所有的分类汇总
// * @date: create in 2020/9/10 10:15
// */
//
//public class StreamingSocket {
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 数据源 DataSource
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 7777, "\n");
//
//        SingleOutputStreamOperator<KinesisData> map = dataStreamSource.map(new MapFunction<String, KinesisData>() {
//            @Override
//            public KinesisData map(String value) throws Exception {
//                return JSONUtil.toBean(value, KinesisData.class);
//            }
//        });
//
//        String template = "appId:{appId} platformId:{platformId} ";
//        map.filter((FilterFunction<KinesisData>) value -> value.getEvent_id() == 102)
////                .keyBy((KeySelector<KinesisData, String>) value -> StrUtil.format(template, Dict.create().set("appId", value.getApp_id()).set("platformId", value.getPlatform_id())))
//                .keyBy((KeySelector<KinesisData, String>) value -> "")
//                .fold(new HashMap<>(), new FoldFunction<KinesisData, HashMap<String, Integer>>() {
//                    @Override
//                    public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, KinesisData value) throws Exception {
//                        Integer appId = value.getApp_id();
//                        Integer platformId = value.getPlatform_id();
//
//                        String key = StrUtil.format(template, Dict.create().set("appId", appId).set("platformId", platformId));
//
//                        accumulator.put(key, accumulator.getOrDefault(key, 0) + 1);
//
//                        return accumulator;
//                    }
//                }).print();
//
//
//        env.execute("Flink Streaming Java API Skeleton Hello");
//    }
//}
