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

    public String getTableName();

    public String getDatabase();

    public List<String> getPkNames();

    public Long getEs();

    public Long getTs();

    public String getType();

    public Map<String, Object> getOld();

    public Map<String, Object> getData();

}
