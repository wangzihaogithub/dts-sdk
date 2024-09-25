package com.github.dts.sdk.util;

import java.io.IOException;

public class JsonUtil {
    private JsonUtil() {
    }

    public static ObjectReader objectReader() {
        if (PlatformDependentUtil.JACKSON_OBJECT_MAPPER_CONSTRUCTOR != null && PlatformDependentUtil.JACKSON_READ_VALUE_METHOD != null) {
            try {
                Object jacksonObjectMapper = PlatformDependentUtil.JACKSON_OBJECT_MAPPER_CONSTRUCTOR.newInstance();
                jacksonConfigure(jacksonObjectMapper);
                return new ObjectReader() {
                    @Override
                    public <T> T readValue(String json, Class<T> type) throws IOException {
                        try {
                            return (T) PlatformDependentUtil.JACKSON_READ_VALUE_METHOD.invoke(jacksonObjectMapper, json, type);
                        } catch (Exception e) {
                            Util.sneakyThrows(e);
                            return null;
                        }
                    }
                };
            } catch (Exception ignored) {
            }
        }

        if (PlatformDependentUtil.FASTJSON_PARSE_OBJECT_METHOD != null) {
            return new ObjectReader() {
                @Override
                public <T> T readValue(String json, Class<T> type) throws IOException {
                    try {
                        return (T) PlatformDependentUtil.FASTJSON_PARSE_OBJECT_METHOD.invoke(json, type);
                    } catch (Exception e) {
                        Util.sneakyThrows(e);
                        return null;
                    }
                }
            };
        }
        throw new UnsupportedOperationException("objectReader#jsonToBean");
    }

    private static void jacksonConfigure(Object jacksonObjectMapper) {
        if (PlatformDependentUtil.JACKSON_DESERIALIZATION_FEATURE_CLASS == null || PlatformDependentUtil.JACKSON_CONFIGURE_METHOD == null) {
            return;
        }
        try {
            Enum failOnUnknownProperties = Enum.valueOf(PlatformDependentUtil.JACKSON_DESERIALIZATION_FEATURE_CLASS, "FAIL_ON_UNKNOWN_PROPERTIES");
            PlatformDependentUtil.JACKSON_CONFIGURE_METHOD.invoke(jacksonObjectMapper, failOnUnknownProperties, false);
        } catch (Exception e) {
            System.err.println("jacksonConfigure fail " + e);
        }
    }

    public interface ObjectReader {
        <T> T readValue(String json, Class<T> type) throws IOException;
    }

}