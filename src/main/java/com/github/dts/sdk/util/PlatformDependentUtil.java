package com.github.dts.sdk.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class PlatformDependentUtil {
    public static final Constructor JACKSON_OBJECT_MAPPER_CONSTRUCTOR;
    public static final Method JACKSON_READ_VALUE_METHOD;
    public static final Method FASTJSON_PARSE_OBJECT_METHOD;
    public static final Class REDIS_CONNECTION_FACTORY_CLASS;

    static {
        Constructor<?> jacksonObjectMapperConstructor;
        Method readValueMethod;
        try {
            Class<?> objectMapperClass = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
            jacksonObjectMapperConstructor = objectMapperClass.getConstructor();
            readValueMethod = objectMapperClass.getMethod("readValue", String.class, Class.class);
        } catch (Throwable e) {
            jacksonObjectMapperConstructor = null;
            readValueMethod = null;
        }
        JACKSON_OBJECT_MAPPER_CONSTRUCTOR = jacksonObjectMapperConstructor;
        JACKSON_READ_VALUE_METHOD = readValueMethod;

        Method parseObjectMethod;
        try {
            Class<?> fastjsonClass = Class.forName("com.alibaba.fastjson2.JSON");
            parseObjectMethod = fastjsonClass.getDeclaredMethod("parseObject", String.class, Class.class);
        } catch (Throwable e) {
            try {
                Class<?> fastjsonClass = Class.forName("com.alibaba.fastjson.JSON");
                parseObjectMethod = fastjsonClass.getDeclaredMethod("parseObject", String.class, Class.class);
            } catch (Throwable e1) {
                parseObjectMethod = null;
            }
        }
        FASTJSON_PARSE_OBJECT_METHOD = parseObjectMethod;

        Class redisConnectionFactory;
        try {
            redisConnectionFactory = Class.forName("org.springframework.data.redis.connection.RedisConnectionFactory");
        } catch (Throwable e) {
            redisConnectionFactory = null;
        }
        REDIS_CONNECTION_FACTORY_CLASS = redisConnectionFactory;
    }

    public static boolean isSupportSpringframeworkRedis() {
        return REDIS_CONNECTION_FACTORY_CLASS != null;
    }

}
