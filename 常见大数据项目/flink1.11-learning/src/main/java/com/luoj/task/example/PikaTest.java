package com.luoj.task.example;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-30
 */
import java.util.List;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import com.luoj.common.ExampleConstant;

public class PikaTest {

    public static void main(String[] args) {

        /** 构造请求参数对象 */
        String iceLocator = ExampleConstant.JCACHE_ICE_LOCATOR;
        String proxyName = ExampleConstant.JCACHE_PROXY_NAME;
        String key = "one";
        String resourceName = "BigDataTest";
        JcacheTemplate jcache = new JcacheTemplate(iceLocator, proxyName, resourceName);
        jcache.set(key, "Test_Key_02设置时间为20180427！");
        String result = jcache.get(key);
        System.out.println("响应结果为：" + result);
        jcache.del(key);
        result = jcache.get(key);
        System.out.println("del后响应结果为：" + result);

        jcache.set(key, "Test_Key_02设置时间为20180427！");
        result = jcache.get(key);
        System.out.println("Test_Key_02响应结果为：" + result);
        jcache.expire(key, 2);
        result = jcache.get(key);
        System.out.println("expire后响应结果为：" + result);

        jcache.set(key, "one");
        jcache.set("two", "two");
        jcache.set("three", "33");
        jcache.set("four", "44");
        jcache.set("five", "55");
        jcache.set("six", "66");
        jcache.set("seven", "seven");
        jcache.set("eight", "eight");
        jcache.set("nine", "nine");
        jcache.set("ten", "ten");
        System.out.println("-------------------------------------单个key的测试结束-------------------------------------");

        String[] fields = new String[]{"one","two","three","four","five","six","seven","eight","nine","ten"};
        List<String> list = jcache.mget(fields);
        for(String str : list) {
            System.out.println("mget结果：" + str);
        }
        System.out.println("-------------------------------------mget测试结束-------------------------------------");

        System.out.println("---------------------------------------");
        PipelineTemplate pipeline = jcache.pipelined();
        pipeline.set("eleven", "eleven");
        pipeline.set("twelve", "twelve");
        pipeline.set("thirteen", "thirteen");
        pipeline.set("fourteen", "fourteen");
        pipeline.sync();
        System.out.println("-------------------pipeline测试结束--------------------");

        fields = new String[]{"eleven","twelve","three","thirteen","fourteen"};
        list = jcache.mget(fields);
        for(String str : list) {
            System.out.println("mget结果：" + str);
        }
        System.out.println("-------------------------------------mget测试结束-------------------------------------");

        jcache.closeIceContext();

    }

}
