package com.bidata.example.sparksql;

import java.io.Serializable;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-16
 */
public class AppInfo implements Serializable{
    public String logTime;
    public String msg;
    public String logInfo;

    public String getLogTime() {
        return logTime;
    }

    public String getMsg() {
        return msg;
    }

    public String getLogInfo() {
        return logInfo;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setLogInfo(String logInfo) {
        this.logInfo = logInfo;
    }

    public void createData(Integer i) {
        this.logTime="logTime"+i;
        this.msg="msg"+i;
        this.logInfo="logInfo"+i;
    }
}
