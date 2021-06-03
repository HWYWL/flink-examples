package com.yi.practice;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.yi.practice.datasource.OrderDataSource;
import com.yi.practice.pojo.CategoryPojo;
import com.yi.practice.pojo.Goods;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 双十一大屏展示
 * 1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
 * 2.计算出各个分类的销售top3
 * 3.每秒钟更新一次统计结果
 *
 * @author: YI
 * @date: create in 2021/6/3 11:06
 */
public class SinglesDayBigScreen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 设置结束或者取消任务后的数据保留，便于重启恢复数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.enableCheckpointing(1000);
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m，默认是0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //1.配置了Checkpoint的情况下不做任务配置:默认是无限次重启并自动恢复,可以解决小问题,但是可能会隐藏真正的bug
        //2.单独配置无重启策略,只要有异常,程序立刻就挂了
        //env.setRestartStrategy(RestartStrategies.noRestart());
        //3.固定延迟重启--开发中常用,允许三次的异常抛出，最多重启3次数，重启时间间隔为5秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)
        ));
        //默认值为0，表示不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///G:/work/logs/flink"));
        } else {
            // HDFS地址可以使用 hdfs getconf -confKey fs.default.name 查看,不过现在HDFS的jar包停止维护
//            env.setStateBackend(new FsStateBackend("hdfs://ip-172-31-47-84.cn-northwest-1.compute.internal:8020/flink-checkpoint/checkpoint"));
            // 这里使用可以使用S3
//            env.setStateBackend(new FsStateBackend("s3://kylin-data/FlinkCheckpoint/"));
            env.setStateBackend(new FsStateBackend("file:///home/hadoop/app/checkpoint"));
        }

        // 数据源
        DataStreamSource<Goods> source = env.addSource(new OrderDataSource());

        // 按照商品分类、统计、聚合
        DataStream<CategoryPojo> tempAggResult = source.keyBy(t -> t.getGoodsType().getName())
                // 窗口统计一天的数据，服务器事件是0时区，本地开发机是八东区，所以需要设置一下时间
                .window(TumblingProcessingTimeWindows.of(Time.days(1), SystemUtils.IS_OS_WINDOWS ? Time.hours(0) : Time.hours(8)))
                // 自定义触发器时间间隔，这里为一秒
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new PriceAggregate(), new WindowResult());
        tempAggResult.print("初步聚合分类销售总额：");

        // sink-使用上面初步聚合的结果(每隔1s聚合一下截止到当前时间的各个分类的销售总金额),实现业务需求:
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new FinalResultWindowProcess()).print();

        env.execute("Flink Singles Day Big Screen");
    }

    /**
     * 对每个类型的结果进行统计计算
     * <IN> – The type of the values that are aggregated (input values)
     * <ACC> – The type of the accumulator (intermediate aggregate state).
     * <OUT> – The type of the aggregated result
     */
    private static class PriceAggregate implements AggregateFunction<Goods, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Goods value, Double accumulator) {
            return value.getUnitPrice().doubleValue() + accumulator;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double acc1, Double acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 收集聚合的结果
     * <IN> – The type of the input value.
     * <OUT> – The type of the output value.
     * <KEY> – The type of the key.
     * <W> – The type of Window that this window function can be applied on.
     */
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            Double totalPrice = input.iterator().next();
            out.collect(new CategoryPojo(key, totalPrice, DateUtil.now()));
        }
    }

    /**
     * 最终结合结果，我们的业务逻辑
     * <IN> – The type of the input value.
     * <OUT> – The type of the output value.
     * <KEY> – The type of the key.
     * <W> – The type of Window that this window function can be applied on.
     */
    private static class FinalResultWindowProcess extends ProcessWindowFunction<CategoryPojo, Map<String, String>, String, TimeWindow> {
        // 需要的top n
        private static final int TOP_N = 3;

        @Override
        public void process(String time, Context context, Iterable<CategoryPojo> elements, Collector<Map<String, String>> out) throws Exception {
            //用来记录销售总额
            double total = 0D;
            // 使用小顶堆完成排序top n
            Queue<CategoryPojo> queue = new PriorityQueue<>(TOP_N, (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);
            for (CategoryPojo element : elements) {
                double price = element.getTotalPrice();
                total += price; //销售总营业额
                if (queue.size() < TOP_N) {
                    //或offer入队,进来的就是从小到大的排序,前面定义排序规则了
                    queue.add(element);
                } else {
                    //如果堆顶元素 > 新数，则删除堆顶，加入新数入堆
                    if (price >= queue.peek().getTotalPrice()) {
                        //移除堆顶元素
                        queue.poll();
                        queue.add(element);
                    }
                }
            }

            Map<String, String> map = new HashMap<>(2);
            List<String> top3List = queue.stream()
                    .sorted((c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? -1 : 1)
                    .map(c -> "分类:" + c.getCategory() + " 金额:" + c.getTotalPrice())
                    .collect(Collectors.toList());


            // 销售总额四舍五入保留2位小数
            double roundResult = new BigDecimal(total).setScale(2, RoundingMode.HALF_UP).doubleValue();
            map.put("time", "时间: " + time + " 总金额 :" + roundResult + StrUtil.LF);
            map.put("top", "TOP" + TOP_N + StrUtil.COLON + StrUtil.LF + StringUtils.join(top3List, StrUtil.LF));

            out.collect(map);
        }
    }
}
