package com.luoj.common;

import javafx.util.Pair;
import org.apache.commons.beanutils.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lj.michale
 * @description 反射工具类
 * @date 2021-07-08
 */
public class ReflectUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReflectUtils.class);

    //为了处理变长参数,将输入数据类型转化为method类型，
    public static Object[] warpperParams(Method method, Object[] params) {
        Class[] methodClazz = method.getParameterTypes();
        Object[] warpedParams = new Object[methodClazz.length];
        for (int i = 0; i < methodClazz.length; i++) {
            if (i == methodClazz.length - 1) {
                if (methodClazz[i].isArray() && (!params[i].getClass().isArray())) {
                    Object[] last = Arrays.copyOfRange(params, i, params.length);
                    Object typedParams = Array.newInstance(methodClazz[i].getComponentType(),last.length);
                    for(int j=0;j<last.length;j++){
                        Array.set(typedParams,j,last[j]);
                    }
                    warpedParams[i]=typedParams;
                } else {
                    warpedParams[i] = params[i];
                }
            } else {
                warpedParams[i] = params[i];
            }
        }
        return warpedParams;
    }

    //比较类型是否相同，拆装箱视为相同，变长参数和其component视为相同
    public static boolean compareType(Class source, Class dist) {
        if (source == null || dist == null) {
            return false;
        }
        if (!source.equals(dist)) {
            Boolean arrayEqual = source.isArray() || dist.isArray() ? compareType(source.getComponentType(), dist) || compareType(source, dist.getComponentType()) : false;
            if (!arrayEqual) {
                Boolean primitiveEqual = source.isPrimitive() ? source.equals(MethodUtils.getPrimitiveType(dist))
                        : source.equals(MethodUtils.getPrimitiveWrapper(dist));
                if (primitiveEqual) {
                    return primitiveEqual;
                }
            }
            return arrayEqual;
        }

        return true;
    }

    public static Method getMethodsIgnoreCase
            (Class<?> clazz, String methodName, Class<?>[] paramTypes) {
        List<Method> methods = Arrays.stream(clazz.getMethods())
                .filter(m -> m.getName()
                        .equalsIgnoreCase(methodName))
                .collect(Collectors.toList());
        for (Method method : methods) {
            //选择合适的方法返回
            Class[] methodClazz = method.getParameterTypes();
            Boolean typeEqual = true;
            if(methodClazz.length>paramTypes.length){
                continue;
            }
            for (int i = 0; i < methodClazz.length; i++) {
                if(i==methodClazz.length){
                    for(int j=i;j<paramTypes.length;j++){
                        typeEqual= compareType(methodClazz[i], paramTypes[j]);
                    }
                }else{
                    typeEqual = compareType(methodClazz[i], paramTypes[i]);
                }
                if (!typeEqual){
                    break;
                }
            }
            if (typeEqual) {
                return method;
            }
        }
        return null;
    }

    public static Object invokeMethod(Object obj, String methodName, Object... params) {
        Class<?> aClass = obj.getClass();
        Object result = null;
        try {
            /**
             * 反射获取方法不支持自动装箱或拆箱，那反射调用方法支撑自动拆装箱。
             */
            Class[] parameterTypes = Arrays.stream(params).map(param -> param.getClass()).toArray(Class[]::new);
            logger.debug("class {}, method {}, parameterTypes {}", aClass, methodName, Arrays.stream(params).map(param -> param.getClass().toString()).collect(Collectors.joining(" , ")));
            Method method = getMethodsIgnoreCase(aClass, methodName.trim(), Arrays.stream(params).map(param -> param.getClass()).toArray(Class[]::new));
            logger.debug("method is {}", method.getName());
            result = method.invoke(obj, warpperParams(method, params));
        } catch (Exception e) {
            List<Object> paramList = new ArrayList(params.length);
            Collections.addAll(paramList, params);
            throw new RuntimeException("Reflect invoke method fail, objClass : "
                    + aClass + ",method:  " + methodName
                    + ", params: " + paramList, e);
        }
        return result;
    }

}
