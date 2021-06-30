package com.luoj.task.learn.hudi.example001;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-29
 */
/**
 * Socket方式的数据生成器
 */
public class SocketProducer implements Runnable {

    private static Logger logger = LogManager.getLogger(SocketProducer.class);
    private static Short LSTN_PORT = 9999;
    private static Short PRODUCE_INTEVAL = 1;           // 生成消息的时间间隔
    private static Short MAX_CONNECTIONS = 10;          // 最大10个连接
    private static Frog userFrog = UserFrog.build();

    private ServerSocket server;

    public SocketProducer(ServerSocket srv) {
        this.server = srv;
    }

    @Override
    public void run() {
        // 数据总条数、耗时
        long totalNum = 0;
        long startMillseconds = 0;

        try {
            final Socket client = server.accept();
            String clientInfo = String.format("主机名:%s, IP:%s"
                    , client.getInetAddress().getHostName()
                    , client.getInetAddress().getHostAddress());

            // 设置线程元数据
            Thread.currentThread().setName(clientInfo);

            logger.info(String.format("客户端[%s]已经连接到服务器.", clientInfo));

            OutputStream outputStream = client.getOutputStream();
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
            BufferedWriter writer = new BufferedWriter(outputStreamWriter);

            startMillseconds = System.currentTimeMillis();

            // 发送消息
            while(true) {
                String msg = userFrog.getOne().toString();
                logger.debug(msg);
                writer.write(msg);
                writer.newLine();
                writer.flush();
                totalNum++;

                try {
                    TimeUnit.MILLISECONDS.sleep(PRODUCE_INTEVAL);
                } catch (InterruptedException e) {
                    logger.warn(e);
                    logger.warn(String.format("客户端[%s]断开", clientInfo));
                    break;
                }
            }
        } catch (IOException e) {
            logger.fatal(e);
        }

        if(startMillseconds == -1) {
            logger.warn("统计耗时失败！客户端连接异常断开！");
        }
        else {
            long endMillseconds = System.currentTimeMillis();
            // 耗时
            double elapsedInSeconds = (endMillseconds - startMillseconds) / 1000.0;
            double rate = totalNum / elapsedInSeconds;
            logger.info(String.format("共计生成数据：%d条, 耗时：%.1f秒, 速率：%.1f条/s"
                    , totalNum
                    , elapsedInSeconds
                    , rate));
        }
    }

    public static void main(String[] args) {
        try {
            logger.debug(String.format("Socket服务器配置:\n"
                            + "-----------------------\n"
                            + "监听端口号:%d \n"
                            + "生成消息事件间隔: %d(毫秒)\n"
                            + "最大连接数: %d \n"
                            + "-----------------------"
                    , LSTN_PORT
                    , PRODUCE_INTEVAL
                    , MAX_CONNECTIONS));

            ServerSocket serverSocket = new ServerSocket(LSTN_PORT);
            logger.info(String.format("启动服务器，监听端口: %d", LSTN_PORT));
            ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONNECTIONS);

            IntStream.range(0, MAX_CONNECTIONS)
                    .forEach(n -> {
                        executorService.submit(new SocketProducer(serverSocket));
                    });

            while(true) {
                if(executorService.isShutdown() || executorService.isTerminated()) {
                    IntStream.range(0, MAX_CONNECTIONS)
                            .forEach(n -> {
                                executorService.submit(new SocketProducer(serverSocket));
                            });
                }
            }
        } catch (IOException e) {
            logger.fatal(e);
        }
    }
}
