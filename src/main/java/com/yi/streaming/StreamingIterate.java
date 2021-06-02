//package com.yi.streaming;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.IterativeStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.util.Collector;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//
///**
// * @author: YI
// * @description: iterate - 利用iterate的反馈机制制作fib，在到达指定fib数后，停止反馈
// * @date: create in 2020/9/10 10:15
// */
//
//public class StreamingIterate {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        List<Long> initial = new ArrayList<>();
//        initial.add(1L);
//
//        DataStreamSource<Long> ds = env.fromCollection(initial);
//        IterativeStream<Long> iterate = ds.iterate(1000);
//
//        SingleOutputStreamOperator<HashMap<String, Long>> fold = iterate.
//
////        // 设置fib数量的最大上限
////        SplitStream<Long> split = fold.map(value -> value.get("next"))
////                .setParallelism(1)
////                .split(value -> {
////                    List<String> outPut = new ArrayList<>(2);
////                    if (value < 100L) {
////                        outPut.add("feedback");
////                    } else {
////                        outPut.add("out");
////                    }
////
////                    return outPut;
////                });
////
////        // 关闭流
////        DataStream<Long> closeWith = iterate.closeWith(split.select("feedback"));
////        DataStream<Long> out = split.select("out");
//////        closeWith.print();
////        out.print();
//
//        env.execute("Flink Streaming Java API Skeleton Hello");
//    }
//}
