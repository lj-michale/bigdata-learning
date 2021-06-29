//package com.bidata.example.hudi.example;
//
///**
// * @author lj.michale
// * @description
// * @date 2021-06-29
// */
//
//import org.apache.commons.collections.ListUtils;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
///**
// * 从Hudi分区路径中获取分区字段
// */
//public class DayPartitionValueExtractor implements PartitionValueExtractor {
//
//    @Override
//    public List<String> extractPartitionValuesInPath(String partitionPath) {
//        final String[] dateField = partitionPath.split("-");
//
//        if(dateField != null && dateField.length >= 3) {
//            return Collections.singletonList(IntStream.range(0, 3)
//                    .mapToObj(idx -> dateField[idx])
//                    .collect(Collectors.joining("-")));
//        }
//
//        return ListUtils.EMPTY_LIST;
//    }
//}