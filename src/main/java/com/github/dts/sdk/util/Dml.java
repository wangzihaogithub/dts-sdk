package com.github.dts.sdk.util;

import java.util.List;
import java.util.Map;

public interface Dml {
    default SQL sql() {
        return SQL.DEFAULT_BUILDER.convert(this);
    }

    default String getSql() {
        return null;
    }

    default Boolean getDdl() {
        return false;
    }

    String getTableName();

    String getDatabase();

    List<String> getPkNames();

    Long getEs();

    Long getTs();

    String getType();

    Map<String, Object> getOld();

    Map<String, Object> getData();

}
