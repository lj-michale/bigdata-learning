package com.turing.common;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-05
 */
@Slf4j
public class PropertiesUtils {

    /**
     * @descri 获取某一个properties文件的properties对象，以方便或得文件里面的值
     *
     * @param filePath：properties文件所在路径
     * @return Properties对象
     */
    public static Properties getProperties(String filePath) {
        final Properties properties = new Properties();

        try {
            properties.load(PropertiesUtils.class.getClassLoader().getResourceAsStream(filePath));
        } catch (IOException e) {
            log.error("获取某一个properties文件的properties对象失败：{}", e.getMessage());
            e.printStackTrace();
        }
        return properties;
    }

}
