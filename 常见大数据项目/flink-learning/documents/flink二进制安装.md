# flink 二进制安装：
## 1. 下载 & 解压
```
cd /usr/local/;
wget https://mirror.bit.edu.cn/apache/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz
tar -xvf ./flink-1.10.0-bin-scala_2.11.tgz;
chown -R hadoop:hadoop ./flink-1.10.0;
ln -s ./flink-1.10.0 ./flink;
```
## 2. hadoo用户下 配置环境变量 vi ~/.bash_profile
```
#set Java environment
export JAVA_HOME=/usr/java/latest
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib:$CLASSPATH
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
export HADOOP_HOME=/home/hadoop/apps/hadoop
export HBASE_HOME=/home/hadoop/apps/hbase
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin:$PATH

export HADOOP_CLASSPATH=${HADOOP_HOME}/etc/hadoop:$HADOOP_HOME/share/hadoop/client/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*
```

## 3. 测试
```
cd /usr/local/flink/;
./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar
```

## 4. 到 yarn web 页去查看任务运行情况 ( 下面两个节点，哪个是 Active，就看那个)
```
http://plt-hdf-datanode-v01:8088/cluster/cluster
http://plt-hdf-datanode-v02:8088/cluster/cluster
```


