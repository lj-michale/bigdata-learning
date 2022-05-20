package com.turing.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.List;

/**
 * @descri 基于Spark mllib包下的ALS算法
 *
 * @author lj.michale
 * @date 2022-05-20
 */
public class MovieRecommendByALS {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("MovieRecommendByALS")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // 设置日志级别
        jsc.setLogLevel("WARN");

        // 设置数据集文件
        JavaRDD<String> rawData = jsc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\u.data");
        // 将数据按照\t分割
        JavaRDD<String[]> rawRatings = rawData.map(v1 -> v1.split("\t"));
        //装载样本评分数据，其中最后一列Timestamp取除10的余数作为key，Rating为值，即(Int，Rating)
        JavaPairRDD<Long, Rating> ratings = rawRatings.mapToPair(v1 -> {
            Rating rating = new Rating(Integer.valueOf(v1[0]), Integer.valueOf(v1[1]), Double.valueOf(v1[2]));
            return new Tuple2<>(Long.valueOf(v1[3]) % 10, rating);
        });

        // 装载电影目录对照表(电影ID->电影标题)
        List<Tuple2> movies = jsc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\u.item")
                .map(v1 -> {
            String[] ss = v1.split("\\|");
            return new Tuple2(ss[0], ss[1]);
        }).collect();

        // 统计有用户数量和电影数量以及用户对电影的评分数目
        Long numRatings = ratings.count();
        Long numUsers = ratings.map(v1 -> ((Rating) v1._2()).user()).distinct().count();
        Long numMovies = ratings.map(v1 -> ((Rating) v1._2()).product()).distinct().count();
        System.out.println("用户：" + numUsers + "电影：" + numMovies + "评论：" + numRatings);

        // 将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
        // 该数据在计算过程中要多次应用到，所以cache到内存
        // 分区数
        Integer numPartitions = 4;
        // 训练集
        JavaRDD<Rating> training = ratings
                .filter(v -> v._1() < 6)
                .values()
                .repartition(numPartitions)
                .cache();

        // 校验集
        JavaRDD<Rating> validation = ratings
                .filter(v -> v._1() >= 6 && v._1() < 8)
                .values()
                .repartition(numPartitions).cache();

        // 测试集
        JavaRDD<Rating> test = ratings
                .filter(v -> v._1() >= 8)
                .values()
                .cache();

        Long numTraining = training.count();
        Long numValidation = validation.count();
        Long numTest = test.count();
        System.out.println("训练集：" + numTraining + " 校验集：" + numValidation + " 测试集：" + numTest);

        // 训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模
        int[] ranks = new int[]{10, 11, 12};
//        double[] lambdas = new double[]{0.01, 0.03, 0.1, 0.3, 1, 3};
        double[] lambdas = new double[]{0.01};
//        int[] numIters = new int[]{8, 9, 10, 11, 12, 13, 14, 15};
        int[] numIters = new int[]{8, 9, 10};

        MatrixFactorizationModel bestModel = null;
        double bestValidationRmse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestLambda = -0.01;
        int bestNumIter = 0;

        // 三层for循环,如果训练更多的参数更多次循环
        for (int rank : ranks) {
            for (int numIter : numIters) {
                for (double lambda : lambdas) {
                    // 训练集得到模型
                    MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);
                    // 校验集进行预测数据和实际数据之间的均方根误差,得到误差最小的,之后返回最好的参数
                    Double validationRmse = computeRmse(model, validation, numValidation);
                    System.out.println("RMSE(校验集) = " + validationRmse + ", rank = " + rank + ", lambda = " + lambda + ", numIter = " + numIter);

                    if (validationRmse < bestValidationRmse) {
                        bestModel = model;
                        bestValidationRmse = validationRmse;
                        bestRank = rank;
                        bestLambda = lambda;
                        bestNumIter = numIter;
                    }
                }
            }
        }

        // 测试集用于看看提升了多少准确率
        double testRmse = computeRmse(bestModel, test, numTest);
        System.out.println("测试数据集在 最佳训练模型 rank = " + bestRank + ", lambda = " + bestLambda + ", numIter = " + bestNumIter + ", RMSE = " + testRmse);

        // 计算均值
        Double meanRating = training.union(validation).mapToDouble(v -> v.rating()).mean();
        // 计算标准误差值
        Double baselineRmse = Math.sqrt(test.map(v -> (meanRating - v.rating()) * (meanRating - v.rating())).reduce((v1, v2) -> (v1 + v2) / numTest));
        // 计算准确率提升了多少
        double improvement = (baselineRmse - testRmse) / baselineRmse * 100;
        System.out.println("最佳训练模型的准确率提升了：" + String.format("%.2f", improvement) + "%.");

        // 构建最佳训练模型
        bestModel = ALS.train(ratings.values().rdd(), bestRank, bestNumIter, bestLambda);
        Rating[] recommendProducts = bestModel.recommendProducts(789, 10);

        // 打印推荐结果
        for (Rating rating : recommendProducts) {
            System.out.println(rating.user() + "->" + rating.product()+": " + rating.rating());
        }
    }

    /**
     * 校验集预测数据和实际数据之间的均方根误差
     **/
    public static Double computeRmse(MatrixFactorizationModel model,
                                     JavaRDD<Rating> data,
                                     Long n) {
        // 进行预测
        JavaRDD<Rating> predictions = model.predict(data.mapToPair(v -> new Tuple2<>(v.user(), v.product())));
        JavaRDD<Tuple2<Double, Double>> predictionsAndRatings = predictions
                .mapToPair(v -> new Tuple2<>(new Tuple2<>(v.user(), v.product()), v.rating()))
                .join(data.mapToPair(v -> new Tuple2<>(new Tuple2<>(v.user(), v.product()), v.rating()))).values();
        Double reduce = predictionsAndRatings.map(v -> (v._1 - v._2) * (v._1 - v._2))
                .reduce((v1, v2) -> (v1 + v2) / n);

        // 正平方根
        return Math.sqrt(reduce);
    }

}
