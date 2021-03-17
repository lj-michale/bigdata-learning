package com.luoj.task.example.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Author erainm
 * API
 * Flink累加器：
 * Flink中的累加器，与Mapreduce counter的应用场景类似，可以很好地观察task在运行期间的数据变化，如在Flink job任务中的算子函数中操作累加器，在任务执行结束之后才能获得累加器的最终结果。
 * Flink有以下内置累加器，每个累加器都实现了Accumulator接口。
 *
 * IntCounter
 * LongCounter
 * DoubleCounter
 * 编码步骤:
 * 1.创建累加器
 * private IntCounter numLines = new IntCounter();
 * 2.注册累加器
 * getRuntimeContext().addAccumulator(“num-lines”, this.numLines);
 * 3.使用累加器
 * this.numLines.add(1);
 * 4.获取累加器的结果
 * myJobExecutionResult.getAccumulatorResult(“num-lines”)
 * ————————————————
 * 版权声明：本文为CSDN博主「erainm」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/eraining/article/details/114396559
 * Desc 演示Flink累加器,统计处理的数据条数
 */
public class OtherAPI_Accumulator {
    public static void main(String[] args) throws Exception {
        //1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        DataSource<String> dataDS = env.fromElements("aaa", "bbb", "ccc", "ddd");

        //3.Transformation
        MapOperator<String, String> result = dataDS.map(new RichMapFunction<String, String>() {
            //-1.创建累加器
            private IntCounter elementCounter = new IntCounter();
            Integer count = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //-2注册累加器
                getRuntimeContext().addAccumulator("elementCounter", elementCounter);
            }

            @Override
            public String map(String value) throws Exception {
                //-3.使用累加器
                this.elementCounter.add(1);
                count+=1;
                System.out.println("不使用累加器统计的结果:"+count);
                return value;
            }
        }).setParallelism(2);

        //4.Sink
        result.writeAsText("data/output/test", FileSystem.WriteMode.OVERWRITE);

        //5.execute
        //-4.获取加强结果
        JobExecutionResult jobResult = env.execute();
        int nums = jobResult.getAccumulatorResult("elementCounter");
        System.out.println("使用累加器统计的结果:"+nums);
    }
}
