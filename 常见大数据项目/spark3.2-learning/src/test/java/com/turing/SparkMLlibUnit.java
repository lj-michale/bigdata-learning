package com.turing;

import com.turing.bean.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.List;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-20
 */
public class SparkMLlibUnit {

    @Test
    public void testDense(){
        // 定义密集型向量
        Vector vd = Vectors.dense(9, 5, 2, 7);
        // 获取下标为2的值
        double v = vd.apply(2);

        System.out.println(v); //2
    }

    @Test
    public void testSparse() {
        int[] indexes = new int[]{0, 1, 2, 5};
        double[] values = new double[]{9, 5, 2, 7};

        // 定义稀疏型向量
        Vector vd = Vectors.sparse(6, indexes, values);
        double v = vd.apply(2); //获取下标为2的值
        double v2 = vd.apply(4); //获取下标为4的值
        double v3 = vd.apply(5); //获取下标为5的值

        System.out.println(v); //2
        System.out.println(v2); //0
        System.out.println(v3); //7
    }

    @Test
    public void testLabeledPoint(){
        Vector vd = Vectors.dense(9, 5, 2, 7); //定义密集型向量
        LabeledPoint lp = new LabeledPoint(1, vd); //定义标签向量
        System.out.println(lp.features());
        System.out.println(lp.label());
        System.out.println(lp);
        System.out.println("------------");

        int[] indexes = new int[]{0, 1, 2, 3};
        double[] values = new double[]{9, 5, 2, 7};
        Vector vd2 = Vectors.sparse(4, indexes, values); //定义稀疏型向量
        LabeledPoint lp2 = new LabeledPoint(2, vd2);
        System.out.println(lp2.features());
        System.out.println(lp2.label());
        System.out.println(lp2);
    }

    @Test
    public void testMatrix() {
        double[] values = new double[]{1, 2, 3, 4, 5, 6};
        Matrix mx = Matrices.dense(2, 3, values);
        System.out.println(mx);
    }

    // 分布式矩阵
    @Test
    public void testRowMatrix() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkMLlib")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = jsc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\a.txt");

        JavaRDD<Vector> rdd2 = rdd1.map(v1 -> {
            String[] ss = v1.split(" ");
            double[] ds = new double[ss.length];
            for (int i = 0; i < ss.length; i++) {
                ds[i] = Double.valueOf(ss[i]);
            }
            return Vectors.dense(ds);
        });

        RowMatrix rmx = new RowMatrix(rdd2.rdd());

        System.out.println(rmx.numRows()); //2
        System.out.println(rmx.numCols()); //3
    }

    @Test
    public void testIndexedRowMatrix() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkMLlib")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd1 = jsc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\a.txt");

        JavaRDD<Vector> rdd2 = rdd1.map(v1 -> {
            String[] ss = v1.split(" ");
            double[] ds = new double[ss.length];
            for (int i = 0; i < ss.length; i++) {
                ds[i] = Double.valueOf(ss[i]);
            }
            return Vectors.dense(ds);
        });

        JavaRDD<IndexedRow> rdd3 = rdd2.map(v1 -> new IndexedRow(v1.size(), v1)); //建立待索引的行
        IndexedRowMatrix irmx = new IndexedRowMatrix(rdd3.rdd()); //行索引

        JavaRDD<IndexedRow> rdd4 = irmx.rows().toJavaRDD();
        List<IndexedRow> collect = rdd4.collect();
        collect.forEach(indexedRow -> System.out.println(indexedRow)); //打印行内容
    }

    @Test
    public void testDataSet() {
        SparkSession sparkSession = SparkSession.builder().appName("SparkMLlib").master("local[*]").getOrCreate();
        // 读取文件
        JavaRDD<String> source = sparkSession
                .read()
                .textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\users.txt")
                .toJavaRDD();

        JavaRDD<Student> rowRDD = source.map(line -> {
            String parts[] = line.split(",");
            Student student = new Student();
            student.setId(Long.valueOf(parts[0]));
            student.setName(parts[1]);
            student.setAge(Integer.valueOf(parts[2]));
            return student;
        });

        Dataset<Row> df = sparkSession.createDataFrame(rowRDD, Student.class);
        df.select("id", "name").orderBy(df.col("id").desc()).show();

    }

}
