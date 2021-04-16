package com.bidata.example.sparksql;

import java.io.Serializable;
import java.util.List;
/**
 * @author lj.michale
 * @description
 * @date 2021-04-16
 */

public class LogInfo implements Serializable{
    private static final long serialVersionUID = 4053810260183406530L;
    public String logFilePath;
    public  List<AppInfo> appInfo ;
    public String getLogFilePath() {
        return logFilePath;
    }
    public List<AppInfo> getAppInfo() {
        return appInfo;
    }
    public void setLogFilePath(String logFilePath) {
        this.logFilePath = logFilePath;
    }
    public void setAppInfo(List<AppInfo> appInfo) {
        this.appInfo = appInfo;
    }
}