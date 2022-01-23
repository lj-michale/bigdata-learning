package com.aurora;

import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * @author lj.michale
 * @date 2022-01-19
 */
public class AnalysisExampleUnit {

    /**
     * @descr: 创建指定长度的int型数组，并生成100以内随机数为数组中的每个元素赋值
     *        定义一个带参带返回值的方法，通过参数传入数组的长度，返回赋值后的数组
     */
    public static int[] getArray(int length) {
        int[] nums = new int[length];
        for (int i = 0; i < nums.length; i++) {
            nums[i] = (int)(Math.random()*100);
        }
        return nums;
    }

    /**
     * @descr: 打印数组
     */
    public static void printArray(int[] arr){
        for(int i=0;i<arr.length;i++){
            System.out.print(arr[i]+" ");
        }
        System.out.println();
    }

    /**
     * @descr: 生成一个新的数组，新数组中最后一位包含追加的随机数值
     */
    public static int[] insertArray(int[] arr, int value){
        int[] res = new int[arr.length + 1];
        int index = 0;
        for(int i = 0; i < arr.length; i++){
            res[index] = arr[i];
            index++;
        }
        res[res.length-1] = value;
        return res;
    }

    /**
     * @descr: 两个数组合并
     */
    private static int[] concat(int a[], int b[]) {
        int c[] = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    public static <T> T[] concatAll(T[] first, T[]... rest) {
        int totalLength = first.length;
        for (T[] array : rest) {
            totalLength += array.length;
        }

        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;

        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length; }
        return result;
    }

    /**
     * @descr: 实现无重复抽样
     */
    public static int[] reservoir(int[] array, int m) {
        int[] result = new int[m];
        int n = array.length;

        for(int i = 0; i < n; i++) {
            int current_num = array[i];
            if(i < m) {
                result[i] = current_num;
            } else {
                int tmp = RandomUtils.nextInt(0, i+1);
                if(tmp < m) {
                    result[tmp] = current_num;
                }
            }
        }
        return result;
    }

    /**
     * @descr: 数组求交集
     * @return
     */
    private static int[] Intersection(int[] a1, int[] a2){
        ArrayList<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i < a1.length; i++)
            for(int j = 0; j < a2.length; j++)
                if(a1[i] == a2[j]) {
                    list.add(a2[j]);
                }
        return list.stream().mapToInt(k -> k).toArray();
    }

    /**
     * @descr: 求两个数组的差集
     * @return
     */
    public static int[] substract(int[] arr1, int[] arr2) {
        LinkedList<Integer> list = new LinkedList<Integer>();
        for (int str : arr1) {
            if(!list.contains(str)) {
                list.add(str);
            }
        }
        for (int str : arr2) {
            if (list.contains(str)) {
                list.remove(str);
            }
        }
        return list.stream().mapToInt(k -> k).toArray();
    }

    public static void main(String[] args) {

        int[] arr1 = getArray(8);
        int[] arr2 = getArray(88);
        int[] arr3 = getArray(30);
        int[] arr4 = getArray(50);
//        System.out.println(Arrays.toString(arr1));
//        System.out.println(Arrays.toString(arr2));
//        System.out.println(Arrays.toString(arr3));
//        System.out.println(Arrays.toString(arr4));
        int[] arr5 = concat(concat(concat(arr1, arr2), arr3), arr4);
//        printArray(arr5);

//        printArray(reservoir(arr5, 10));
        printArray(substract(arr5, reservoir(arr5, 10)));

    }



}
