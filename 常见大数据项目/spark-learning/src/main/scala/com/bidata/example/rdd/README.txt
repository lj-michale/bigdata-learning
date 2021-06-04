
RDD的转换（Transformation）操作
1.map(func)
2. mapPartitions(func) 尽量使用mapPartitions
3.glom
4. flatMap(func) map后再扁平化
5.filter(func)
6.mapPartitionsWithIndex(func)
7.sample(withReplacement, fraction, seed)
8.distinct([numTasks]))
9.partitionBy
10.coalesce(numPartitions)
11. repartition(numPartitions)
12.repartitionAndSortWithinPartitions(partitioner)
13.sortBy(func,[ascending], [numTasks])
14.union(otherDataset)
15.subtract (otherDataset)
16.intersection(otherDataset)
17.cartesian(otherDataset)
18.pipe(command, [envVars])
19.join(otherDataset, [numTasks])
20.cogroup(otherDataset, [numTasks])
21.reduceByKey(func, [numTasks])
22.groupByKey
23.combineByKey[C]
24.aggregateByKey
25.foldByKey
26.sortByKey([ascending], [numTasks])
27. mapValues

原文链接：https://blog.csdn.net/qq_43081842/article/details/100676870