package com.luoj.task.example.distributedCache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author lj
 * 分布式缓存
 * API解释
 * Flink提供了一个类似于Hadoop的分布式缓存，让并行运行实例的函数可以在本地访问。
 * 这个功能可以被使用来分享外部静态的数据，例如：机器学习的逻辑回归模型等
 *
 * 注意
 * 广播变量是将变量分发到各个TaskManager节点的内存上，分布式缓存是将文件缓存到各个TaskManager节点上；
 *
 * 编码步骤:
 * 1：注册一个分布式缓存文件
 * env.registerCachedFile(“hdfs:///path/file”, “cachefilename”)
 * 2：访问分布式缓存文件中的数据
 * File myFile = getRuntimeContext().getDistributedCache().getFile(“cachefilename”);
 * 3：使用
 *
 * 需求
 * 将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
 *
 * Desc 演示Flink分布式缓存
 * 编码步骤:
 * 1：注册一个分布式缓存文件
 *  env.registerCachedFile("hdfs:///path/file", "cachefilename")
 * 2：访问分布式缓存文件中的数据
 *  File myFile = getRuntimeContext().getDistributedCache().getFile("cachefilename");
 * 3：使用
 *
 * 需求:
 * 将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
 */
public class OtherAPI_DistributedCache {
    public static void main(String[] args) throws Exception {
        //1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        //注意:先将本地资料中的distribute_cache_student文件上传到HDFS
        //-1.注册分布式缓存文件
        //env.registerCachedFile("hdfs://node01:8020/distribute_cache_student", "studentFile");
        env.registerCachedFile("data/input/distribute_cache_student", "studentFile");

        //成绩数据集(学号,学科,成绩)
        DataSource<Tuple3<Integer, String, Integer>> scoreDS = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))
        );

        //3.Transformation
        //将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreDS.map(
                new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
                    //定义一集合用来存储(学号,姓名)
                    Map<Integer, String> studentMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //-2.加载分布式缓存文件
                        File file = getRuntimeContext().getDistributedCache().getFile("studentFile");
                        List<String> studentList = FileUtils.readLines(file);
                        for (String str : studentList) {
                            String[] arr = str.split(",");
                            studentMap.put(Integer.parseInt(arr[0]), arr[1]);
                        }
                    }

                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                        //-3.使用分布式缓存文件中的数据
                        Integer stuID = value.f0;
                        String stuName = studentMap.getOrDefault(stuID, "");
                        //返回(姓名,学科,成绩)
                        return Tuple3.of(stuName, value.f1, value.f2);
                    }
                });

        //4.Sink
        result.print();
    }
}
