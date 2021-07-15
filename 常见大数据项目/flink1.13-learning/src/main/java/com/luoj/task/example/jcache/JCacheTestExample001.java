package com.luoj.task.example.jcache;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import com.luoj.common.ExampleConstant;

import java.util.List;
import java.util.Map;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-05
 */
public class JCacheTestExample001 {

    public static void main(String[] args) {

        /** 构造请求参数对象 */
        String iceLocator = ExampleConstant.JCACHE_ICE_LOCATOR;
        String proxyName = ExampleConstant.JCACHE_PROXY_NAME;
        String resourceName = ExampleConstant.JCACHE_CLUSTER_RESOURCE_NAME;

        JcacheTemplate jcache = new JcacheTemplate(iceLocator, proxyName, resourceName);

        String key = "luojie";
        jcache.set(key, "one00000000000000000000000000000000001");
        System.out.println("-------------------------------------单个key的测试-------------------------------------");
        String result = jcache.get(key);
        System.out.println("expire后响应结果1为：" + result);
        Map<String, String> results = jcache.hgetAll(key);


        System.out.println("-------------------------------------pipeline测试-------------------------------------");
        PipelineTemplate pipeline = jcache.pipelined();
        pipeline.set("elevenmmmmmm", "elevenpppppppppppppppppppppppppppppppppp");
        pipeline.hset("jiguang0002", "lllllll", String.valueOf(10000));

        pipeline.sync();
        System.out.println("expire后响应结果4为：" + jcache.hget("jiguang0002", "lllllll"));
        System.out.println("expire后响应结果2为：" + jcache.get("elevenmmmmmm"));

        System.out.println("-------------------------------------hdecrBy测试-------------------------------------");
        Long slong = jcache.hdecrBy("jiguang0001", "lllllll", -2000000);
        System.out.println(">>>>>>>>>>>>>slong: " + slong);
        System.out.println("hdecrBy响应结果3为：" + jcache.hget("jiguang0001", "lllllll"));

        Long sslong = jcache.hincrBy("jiguang0003", "lllllll", 20);
        System.out.println("hincrBy响应结果3为：" + jcache.hget("jiguang0003", "lllllll"));

        System.out.println("-------------------------------------Flink写入测试测试-------------------------------------");
        System.out.println(">>>>>>>>>>Flink写入JCache之后的查询结果为：" + jcache.hget("JG-97632672", "Good-3424687624"));

        System.out.println(">>>>>>>>>>Flink2写入JCache之后的查询结果为：" + jcache.hgetAll("LLLLLLLLLLLLLLLLLLLLLLLLLL"));

        jcache.closeIceContext();

    }
}
