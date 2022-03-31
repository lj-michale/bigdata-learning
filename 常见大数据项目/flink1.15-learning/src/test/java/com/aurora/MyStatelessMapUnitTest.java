package com.aurora;

import com.aurora.feature.func.MyStatelessMap;
import lombok.extern.slf4j.Slf4j;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @descri flink 单元测试
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class MyStatelessMapUnitTest extends TestCase {

        @Test
        public void testMap() throws Exception {
            MyStatelessMap statelessMap = new MyStatelessMap();
            String out = statelessMap.map("world");
            System.out.println(out);
            Assert.assertEquals("hello world", out);
        }

}
