package com.github.dts.sdk.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtil {
    private JsonUtil() {
    }

    public static ObjectReader objectReader() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new ObjectReader() {
            @Override
            public <T> T readValue(String json, Class<T> type) throws IOException {
                return objectMapper.readValue(json, type);
            }
        };
    }

    public interface ObjectReader {
        <T> T readValue(String json, Class<T> type) throws IOException;
    }

}