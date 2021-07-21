



Yarn Pre-Job模式
/flink run -m yarn-cluster -yn 3 -ys 3 -ynm bjsxt02  -c com.test.flink.wc.StreamWordCount ./appjars/test-1.0-SNAPSHOT.jar


Yarn Session模式

Yarn Application模式
./bin/flink run-application -t yarn-application hdfs://hadoop-master:9000/shadow/FlinkSQLTest-1.0-SNAPSHOT.jar


yarn application -kill application_1601372571363_0001