# flink-examples
Flink code examples

### 配置
```shell
cd /home/hadoop/flink-1.12.4/conf
vim flink-conf.yaml
# 我这边机器内存是32G，所以把任务槽调整为8个
taskmanager.numberOfTaskSlots: 8
```

### 启动
```shell
./home/hadoop/flink-1.12.4/bin/flink run -c com.yi.streaming.state.FlinkStateMapState /home/hadoop/app/flink-examples-1.0-SNAPSHOT.jar 
```

### git版本分支
```
matser: 对应Flink 1.12.x之前版本的代码
1.12.x: 对应Flink 1.12.x版本的代码
```