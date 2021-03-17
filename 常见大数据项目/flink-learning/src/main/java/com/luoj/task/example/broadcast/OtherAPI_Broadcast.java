package com.luoj.task.example.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author lj
 * Flink支持广播。可以将数据广播到TaskManager上就可以供TaskManager中的SubTask/task去使用，数据存储到内存中。这样可以减少大量的shuffle操作，而不需要多次传递给集群节点；
 * 比如在数据join阶段，不可避免的就是大量的shuffle操作，我们可以把其中一个dataSet广播出去，一直加载到taskManager的内存中，可以直接在内存中拿数据，避免了大量的shuffle，导致集群性能下降；
 *
 * Desc 演示Flink广播变量
 * 编程步骤：
 * 1：广播数据
 * .withBroadcastSet(DataSet, "name");
 * 2：获取广播的数据
 * Collection<> broadcastSet =     getRuntimeContext().getBroadcastVariable("name");
 * 3:使用广播数据
 * <p>
 * 需求:
 * 将studentDS(学号,姓名)集合广播出去(广播到各个TaskManager内存中)
 * 然后使用scoreDS(学号,学科,成绩)和广播数据(学号,姓名)进行关联,得到这样格式的数据:(姓名,学科,成绩)
 */
public class OtherAPI_Broadcast {

    public static void main(String[] args) throws Exception {
        //1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        //学生数据集(学号,姓名)
        DataSource<Tuple2<Integer, String>> studentDS = env.fromCollection(
                Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五"))
        );

        //成绩数据集(学号,学科,成绩)
        DataSource<Tuple3<Integer, String, Integer>> scoreDS = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))
        );

        //3.Transformation
        //将studentDS(学号,姓名)集合广播出去(广播到各个TaskManager内存中)
        //然后使用scoreDS(学号,学科,成绩)和广播数据(学号,姓名)进行关联,得到这样格式的数据:(姓名,学科,成绩)
        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreDS.map(
                new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
                    //定义一集合用来存储(学号,姓名)
                    Map<Integer, String> studentMap = new HashMap<>();

                    //open方法一般用来初始化资源，每个subtask任务只被调用一次
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //-2.获取广播数据
                        List<Tuple2<Integer, String>> studentList = getRuntimeContext().getBroadcastVariable("studentInfo");
                        for (Tuple2<Integer, String> tuple : studentList) {
                            studentMap.put(tuple.f0, tuple.f1);
                        }
                        //studentMap = studentList.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1));
                    }

                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                        //-3.使用广播数据
                        Integer stuID = value.f0;
                        String stuName = studentMap.getOrDefault(stuID, "");
                        //返回(姓名,学科,成绩)
                        return Tuple3.of(stuName, value.f1, value.f2);
                    }
                    //-1.广播数据到各个TaskManager
                }).withBroadcastSet(studentDS, "studentInfo");

        //4.Sink
        result.print();
    }

}