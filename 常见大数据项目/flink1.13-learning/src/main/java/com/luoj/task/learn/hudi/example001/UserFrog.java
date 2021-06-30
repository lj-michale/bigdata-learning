package com.luoj.task.learn.hudi.example001;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-29
 */
public class UserFrog implements Frog<User> {
    private Logger logger;

    private Random r;
    private List<Date> DATE_CACHE;
    private final Integer DATE_MAX_NUM = 100000;             // 日期数量
    private final Integer MAX_DELAY_MILLS = 60 * 1000;       // 最大延迟毫秒数
    private final Integer DELAY_USER_INTERVAL = 10;          // N个User中就会有一个延迟
    private final List<String> NAME_CACHE;                   // 姓名缓存
    private final List<String> POSITION_CACHE;               // 岗位缓存
    private Long genIndex = 0L;                              // 本次实例生成用户个数

    private UserFrog() {
        r = new Random();
        synchronized (UserFrog.class) {
            logger = LogManager.getLogger(UserFrog.class);

            logger.debug("从names.txt中加载姓名缓存...");
            // 加载姓名缓存
            NAME_CACHE = loadResourceFileAsList("names.txt");
            logger.debug(String.format("已加载 %d 个姓名.", NAME_CACHE.size()));

            logger.debug("从positions.txt加载岗位职位缓存...");
            // 加载职位缓存
            POSITION_CACHE = loadResourceFileAsList("positions.txt");
            logger.debug(String.format("已加载 %d 个岗位.", POSITION_CACHE.size()));

            logger.debug("自动生成日期缓存...");
            // 加载日期缓存
            initDateCache();
            logger.debug(String.format("已加载 %d 个日期", DATE_CACHE.size()));
        }
    }

    public static UserFrog build() {
        return new UserFrog();
    }

    /**
     * 加载socket_datagen目录中的资源文件到列表中
     * @param fileName
     */
    private List<String> loadResourceFileAsList(String fileName) {
        try(InputStream resourceAsStream =
                    User.class.getClassLoader().getResourceAsStream("socket_datagen/" + fileName);
            InputStreamReader inputStreamReader =
                    new InputStreamReader(resourceAsStream);
            BufferedReader br = new BufferedReader(inputStreamReader)) {
            return br.lines()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            logger.fatal(e);
        }

        return ListUtils.EMPTY_LIST;
    }

    private void initDateCache() {
        // 生成出生年月缓存
        final Calendar instance = Calendar.getInstance();

        instance.set(Calendar.YEAR, 1970);
        instance.set(Calendar.MONTH, 1);
        instance.set(Calendar.DAY_OF_MONTH, 1);
        Long startTimestamp = instance.getTimeInMillis();

        instance.set(Calendar.YEAR, 2021);
        instance.set(Calendar.MONTH, 3);
        Long endTimestamp = instance.getTimeInMillis();

        DATE_CACHE = LongStream.range(0, DATE_MAX_NUM)
                .map(n -> RandomUtils.nextLong(startTimestamp, endTimestamp))
                .mapToObj(t -> new Date(t))
                .collect(Collectors.toList());
    }

    @Override
    public User getOne() {
        int userId = r.nextInt(Integer.MAX_VALUE);
        Date birthday = DATE_CACHE.get(r.nextInt(DATE_CACHE.size()));
        String name = NAME_CACHE.get(r.nextInt(NAME_CACHE.size()));
        String position = POSITION_CACHE.get(r.nextInt(POSITION_CACHE.size()));

        final User user = new User(userId, name, birthday, position);
        if(genIndex % DELAY_USER_INTERVAL == 0) {
            final int delayMills = r.nextInt(MAX_DELAY_MILLS);
            logger.debug(String.format("生成延迟数据 - User ID=%d, 延迟: %.1f 秒", userId, delayMills / 1000.0));
            user.setCreateTime(new Date(System.currentTimeMillis() - delayMills));
        }
        ++genIndex;

        return user;
    }

    public static void main(String[] args) {
        final UserFrog userBuilder = UserFrog.build();
        IntStream.range(0, 1000)
                .forEach(n -> {
                    System.out.println(userBuilder.getOne().toString());
                });
    }
}