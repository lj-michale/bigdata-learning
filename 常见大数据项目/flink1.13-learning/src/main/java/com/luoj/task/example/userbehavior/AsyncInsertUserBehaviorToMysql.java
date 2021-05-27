//package com.luoj.task.example.userbehavior;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collections;
//
///**
// * @author lj.michale
// * @description  异步将用户行为数据插入mysql
// * @date 2021-05-27
// */
//public class AsyncInsertUserBehaviorToMysql extends RichAsyncFunction {
//
//    Logger logger = LoggerFactory.getLogger(AsyncInsertUserBehaviorToMysql.class);
//
//    //创建mybatis 会话工厂
//    private transient SqlSession sqlSession ;
//
//    /**
//
//     * open 方法中初始化链接
//
//     *
//
//     * @param parameters
//
//     * @throws Exception
//
//     */
//
//    @Override
//
//    public void open(Configuration parameters) throws Exception {
//        System.out.println("async function for mysql java open ..."+Thread.currentThread().getName());
//        super.open(parameters);
//        sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
//
//    }
//
//    /**
//     * use asyncUser.getId async get asyncUser phone
//     *
//     * @param asyncUser
//     * @param resultFuture
//     * @throws Exception
//     */
//
//    @Override
//    public void asyncInvoke(UserBehavingInfo asyncUser, ResultFuture resultFuture) throws Exception {
//        Integer insertNum = 0;
//        try{
//            UserBehaviorDetailsMapper mapper = sqlSession.getMapper(UserBehaviorDetailsMapper.class);
//            insertNum = mapper.insertUserBehavior(asyncUser);
//            sqlSession.commit();
//            System.out.println("插入数据库"+insertNum);
//        }catch (Exception throwable){
//            sqlSession.rollback();
//            System.out.println("异常回滚"+ throwable);
//        }finally {
//               // 一定要记得放回 resultFuture，不然数据全部是timeout 的
//            resultFuture.complete(Collections.singletonList(insertNum));
//        }
//    }
//
//    /**
//     * close function
//     *
//     * @throws Exception
//     */
//    @Override
//    public void close() throws Exception {
//        logger.info("async function for mysql java close ...");
//        //关闭会话，释放资源
//        sqlSession.close();
//        super.close();
//    }
//
//}
