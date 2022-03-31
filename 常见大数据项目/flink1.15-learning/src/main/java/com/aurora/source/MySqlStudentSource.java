package com.aurora.source;

import com.aurora.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class MySqlStudentSource extends RichParallelSourceFunction<Student> {

    private String executeSql = "select * from t_student";

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    private boolean flag = true;

    /**
     * open只执行一次
     * @param parameters 配置参数
     * @throws Exception 异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://blog.ljxwtl.cn:3306/flink", "root", "wtl199201180271");
        preparedStatement = connection.prepareStatement(executeSql);
    }

    @Override
    public void run(SourceFunction.SourceContext<Student> ctx) throws Exception {
        while (flag){
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                ctx.collect(new Student(id,name,age));
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }

}
