//package com.aurora.common.enums;
//
//import io.swagger.annotations.ApiModelProperty;
//import io.swagger.annotations.ApiParam;
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//
///**
// * @descri Flink计算作业枚举
// *
// * @author lj.michale
// * @date 2022-03-05
// */
//@Getter
//@AllArgsConstructor
//public enum FlinkPipelineEnum {
//
//    /**
//     * 图灵计算广告计算业务Pipeline
//     */
//    TUERING_AD_PIPELINE("SZ-SO213139282GF-SL2811", "TuringComputeAd","Java" ,"com.turing.pipeline.ad.TueringAdPipeline","图灵计算广告业务计算Pipeline", "李世民"),
//
//    /**
//     * 禅游科技禅机计算Pipeline Scala版
//     */
//    TUERING_SCALA_CHAN_JI_PIPELINE("SZ-JO213139282GF-SL2812", "TuringScalaChanJiPipeline", "Scala" ,"com.bigdata.pipeline.TuringScalaChanJiEtl","禅游科技禅机计算Pipeline", "lj.michale"),
//
//    /**
//     * 禅游科技禅机计算Pipeline Java版
//     */
//    TUERING_JAVA_CHAN_JI_PIPELINE("SZ-JO213139282GF-SL2813", "TuringJavaChanJiPipeline", "Java" ,"com.zengame.stat.pipeline.TuringJavaChanJiEtl","禅游科技禅机计算Pipeline", "lj.michale");
//
//    @ApiModelProperty("Pipeline Id")
//    @ApiParam(value = "Pipeline Id", defaultValue = "", required = true)
//    private String pipelineId;
//
//    @ApiModelProperty("Pipeline 名称")
//    @ApiParam(value = "Pipeline 名称", defaultValue = "", required = true)
//    private String pipelineName;
//
//    @ApiModelProperty("编程语言")
//    @ApiParam(value = "编程语言", defaultValue = "", required = true)
//    private String language;
//
//    @ApiModelProperty("Pipeline 主类名")
//    @ApiParam(value = "Pipeline 主类名", defaultValue = "", required = true)
//    private String mainClass;
//
//    @ApiModelProperty("Pipeline 描述")
//    @ApiParam(value = "Pipeline 描述", defaultValue = "", required = true)
//    private String description;
//
//    @ApiModelProperty("Pipeline 开发者")
//    @ApiParam(value = "Pipeline 开发者", defaultValue = "", required = true)
//    private String author;
//
//    /**
//     * @descri 根据pipelineId获取pipelineName
//     *
//     * @param pipelineId
//     */
//    public static String getPipelineName(String pipelineId) {
//        String pipelineName = "";
//        for(FlinkPipelineEnum FlinkPipelineEnum : FlinkPipelineEnum.values()) {
//            if(FlinkPipelineEnum.pipelineId.equals(pipelineId)) {
//                pipelineName = FlinkPipelineEnum.pipelineName;
//                break;
//            }
//        }
//        return pipelineName;
//    }
//
//    /**
//     * @descri 根据pipelineId获取author
//     *
//     * @param pipelineId
//     */
//    public static String getAuthor(String pipelineId) {
//        String author = "";
//        for(FlinkPipelineEnum FlinkPipelineEnum : FlinkPipelineEnum.values()) {
//            if(FlinkPipelineEnum.pipelineId.equals(pipelineId)) {
//                author = FlinkPipelineEnum.author;
//                break;
//            }
//        }
//        return author;
//    }
//
//    /**
//     * @descri 根据pipelineId获取mainClass
//     *
//     * @param pipelineId
//     */
//    public static String getMainClass(String pipelineId) {
//        String mainClassPath = "";
//        for(FlinkPipelineEnum FlinkPipelineEnum : FlinkPipelineEnum.values()) {
//            if(FlinkPipelineEnum.pipelineId.equals(pipelineId)) {
//                mainClassPath = FlinkPipelineEnum.mainClass;
//                break;
//            }
//        }
//        return mainClassPath;
//    }
//
//    /**
//     * @descri 根据pipelineId获取language
//     *
//     * @param pipelineId
//     */
//    public static String getLanguage(String pipelineId) {
//        String language = "";
//        for(FlinkPipelineEnum FlinkPipelineEnum : FlinkPipelineEnum.values()) {
//            if(FlinkPipelineEnum.pipelineId.equals(pipelineId)) {
//                language = FlinkPipelineEnum.language;
//                break;
//            }
//        }
//        return language;
//    }
//
//
//}
