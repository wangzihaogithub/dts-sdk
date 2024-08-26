package com.github.dts.sdk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SQL implements Cloneable {
    public static final Builder DEFAULT_BUILDER = new Builder();
    private static final Logger log = LoggerFactory.getLogger(SQL.class);
    private String type;
    /**
     * 全部字段，修改后的
     */
    private Map<String, Object> data;
    /**
     * 修改前字段的数据 update set name = '123'
     * old.keySet() 就是update set的字段名称， 值为改之前的
     */

    private Map<String, Object> old;
    private String database;
    private String preparedSql;
    private List<Value> values;
    private int sqlVersion;
    private SQL originalSQL;
    /**
     * 是否丢弃这条sql,默认丢弃
     */
    private boolean discardFlag = true;
    private String table;
    private boolean skipDuplicateKeyFlag;
    private boolean skipTableNoExistFlag;
    private boolean skipUnknownDatabase = true;
    private List<String> pkNames;
    private Timestamp executeTime;

    public SQL() {

    }

    public SQL(String type, Map<String, Object> data, Map<String, Object> old,
               String database, String table, List<String> pkNames,
               Long executeTime) {
        this.type = type;
        this.data = data;
        this.old = old;
        this.database = database;
        this.table = table;
        this.pkNames = pkNames;
        this.executeTime = executeTime == null ? null : new Timestamp(executeTime);
    }

    private static <T> T invokeMethod(Object target, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        try {
            Method method = target.getClass().getMethod(methodName, parameterTypes);
            method.setAccessible(true);
            Object result = method.invoke(target);
            return (T) result;
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw e;
        }
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public String getDatabase() {
        return database;
    }

    public String getPreparedSql() {
        return preparedSql;
    }

    public List<Value> getValues() {
        return values;
    }

    public int getSqlVersion() {
        return sqlVersion;
    }

    public SQL getOriginalSQL() {
        return originalSQL;
    }

    public boolean isDiscardFlag() {
        return discardFlag;
    }

    public void setDiscardFlag(boolean discardFlag) {
        this.discardFlag = discardFlag;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isSkipDuplicateKeyFlag() {
        return skipDuplicateKeyFlag;
    }

    public void setSkipDuplicateKeyFlag(boolean skipDuplicateKeyFlag) {
        this.skipDuplicateKeyFlag = skipDuplicateKeyFlag;
    }

    public boolean isSkipTableNoExistFlag() {
        return skipTableNoExistFlag;
    }

    public void setSkipTableNoExistFlag(boolean skipTableNoExistFlag) {
        this.skipTableNoExistFlag = skipTableNoExistFlag;
    }

    public boolean isSkipUnknownDatabase() {
        return skipUnknownDatabase;
    }

    public void setSkipUnknownDatabase(boolean skipUnknownDatabase) {
        this.skipUnknownDatabase = skipUnknownDatabase;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Timestamp getExecuteTime() {
        return executeTime;
    }

    public Map<String, Object> getAfterFields() {
        switch (type) {
            case "INSERT": {
                return getData();
            }
            case "UPDATE": {
                Map<String, Object> map = new LinkedHashMap<>();
                for (Map.Entry<String, Object> entry : getData().entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : getOld().entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                return map;
            }
            case "DELETE":
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public Map<String, Object> getBeforeFields() {
        Map<String, Object> before = getData();
        return before;
    }

    public Integer getTenantId() {
        return getFields("tenant_id", Integer.class);
    }

    public <T> T getFields(String fieldName, Class<T> type) {
        Map<String, Object> old = getOld();
        Map<String, Object> data = getData();
        Object value = null;
        if (old != null) {
            value = old.get(fieldName);
        }
        if (value == null && data != null) {
            value = data.get(fieldName);
        }
        return value == null ? null : TypeUtil.cast(value, type);
    }

    public <T> T getPrimaryKey(String pkName, Class<T> primaryKeyType) {
        Map<String, Object> old = getOld();
        Map<String, Object> data = getData();
        if (old != null && old.containsKey(pkName)) {
            return TypeUtil.cast(old.get(pkName), primaryKeyType);
        } else if (data != null && data.containsKey(pkName)) {
            return TypeUtil.cast(data.get(pkName), primaryKeyType);
        } else {
            return null;
        }
    }

    public Map<String, Object> getChangeBeforeFields() {
        switch (type) {
            case "DELETE": {
                return getData();
            }
            case "UPDATE": {
                return getOld();
            }
            case "INSERT":
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public Map<String, Object> getChangeAfterFields() {
        switch (type) {
            case "INSERT": {
                return getData();
            }
            case "UPDATE": {
                Map<String, Object> map = new LinkedHashMap<>();
                for (String name : getOld().keySet()) {
                    map.put(name, getData().get(name));
                }
                return map;
            }
            case "DELETE":
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public void updateSqlVersion() {
        sqlVersion++;
    }

    public abstract void flushSQL();

    public boolean existPrimaryKey() {
        return getPkNames() != null && !getPkNames().isEmpty();
    }

    public boolean isDdl() {
        return false;
    }

    public boolean isDml() {
        return true;
    }

    @Override
    public SQL clone() {
        try {
            SQL sql = (SQL) super.clone();
            if (this.data != null) {
                sql.data = invokeMethod(this.data, "clone");
            }
            if (this.old != null) {
                sql.old = invokeMethod(this.old, "clone");
            }
            if (this.pkNames != null) {
                sql.pkNames = invokeMethod(this.pkNames, "clone");
            }
            if (this.values != null) {
                sql.values = invokeMethod(this.values, "clone");
            }
            return sql;
        } catch (CloneNotSupportedException | NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new IllegalStateException("clone error. error=" + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        flushSQL();

        if (values == null || values.isEmpty()) {
            return preparedSql;
        }

        Object[] objects = new Object[values.size()];
        for (int i = 0; i < objects.length; i++) {
            Object o = values.get(i).data;
            if (o instanceof Date) {
                o = "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(o) + "'";
            } else if (o instanceof Number) {
                o = o.toString();
            } else if (o instanceof byte[]) {
                o = Arrays.toString((byte[]) o);
            } else if (o == null) {
                o = "null";
            } else {
                o = "'" + o + "'";
            }
            objects[i] = o;
        }
        try {
            String replace = preparedSql.replace("?", "%s");
            return String.format(replace, objects);
        } catch (Exception e) {
            return preparedSql;
        }
    }

    public static class Builder {
        private final InsertSqlBuilder insertSqlBuilder = new InsertSqlBuilder();
        private final UpdateSqlBuilder updateSqlBuilder = new UpdateSqlBuilder();
        private final DeleteSqlBuilder deleteSqlBuilder = new DeleteSqlBuilder();
        private final DdlSqlBuilder ddlSqlBuilder = new DdlSqlBuilder();

        public List<SQL> convert(List<Dml> dmlList) {
            if (dmlList == null || dmlList.isEmpty()) {
                return new ArrayList<>();
            }
            List<SQL> list = new ArrayList<>(dmlList.size());
            for (Dml dml : dmlList) {
                SQL sql = convert(dml);
                list.add(sql);
            }
            return list;
        }

        public SQL convert(Dml dml) {
            if (dml == null) {
                return null;
            }
            boolean isDdl = Boolean.TRUE.equals(dml.getDdl());
            Function<Dml, SQL> sqlBuilderFunction;
            String type = dml.getType().toUpperCase();
            switch (type) {
                case "INIT":
                case "INSERT":
                    sqlBuilderFunction = insertSqlBuilder;
                    break;
                case "UPDATE":
                    sqlBuilderFunction = updateSqlBuilder;
                    break;
                case "DELETE":
                    sqlBuilderFunction = deleteSqlBuilder;
                    break;
                default: {
                    if (isDdl) {
                        sqlBuilderFunction = ddlSqlBuilder;
                    } else {
                        sqlBuilderFunction = null;
                        log.warn("No support dml = {}", dml);
                    }
                }
            }
            if (sqlBuilderFunction == null) {
                return null;
            }
            return sqlBuilderFunction.apply(dml);
        }

        public DdlSqlBuilder getDdlSqlBuilder() {
            return ddlSqlBuilder;
        }

        public DeleteSqlBuilder getDeleteSqlBuilder() {
            return deleteSqlBuilder;
        }

        public InsertSqlBuilder getInsertSqlBuilder() {
            return insertSqlBuilder;
        }

        public UpdateSqlBuilder getUpdateSqlBuilder() {
            return updateSqlBuilder;
        }
    }

    public static class DdlSqlBuilder implements Function<Dml, SQL> {
        @Override
        public SQL apply(Dml dml) {
            SQL sql = new DdlSQL(dml.getType(), dml.getSql(),
                    dml.getDatabase(), dml.getTableName(),
                    dml.getPkNames(), dml.getEs());
            sql.originalSQL = sql.clone();
            return sql;
        }

        private static class DdlSQL extends SQL {
            DdlSQL(String type, String sql, String database, String table, List<String> pkNames, Long es) {
                super(type, new HashMap<>(), new HashMap<>(), database, table, pkNames, es);
                super.values = new ArrayList<>();
                super.preparedSql = sql;
            }

            @Override
            public boolean isDdl() {
                return true;
            }

            @Override
            public boolean isDml() {
                return false;
            }

            @Override
            public void flushSQL() {

            }
        }
    }

    public static class InsertSqlBuilder implements Function<Dml, SQL> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public SQL apply(Dml dml) {
            SQL sql = new InsertSQL(dml.getData(), null,
                    dml.getDatabase(), dml.getTableName(),
                    dml.getPkNames(), dml.getEs());
            sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
            sql.originalSQL = sql.clone();
            return sql;
        }

        private static class InsertSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            InsertSQL(Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, Long es) {
                super("INSERT", data, old, database, table, pkNames, es);
            }

            @Override
            public void flushSQL() {
                Map<String, Object> data = getData();
                if (data == null || data.isEmpty()) {
                    setDiscardFlag(true);
                    return;
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();

                List<String> skipKeys = data.keySet().stream()
                        .filter(e -> e.startsWith("#alibaba_rds"))
                        .collect(Collectors.toList());
                data = new LinkedHashMap<>(data);
                for (String skipKey : skipKeys) {
                    data.remove(skipKey);
                }
                sqlBuilder.append("INSERT INTO ").append(getTable()).append(" (`")
                        .append(String.join("`,`", data.keySet())).append("`) VALUES (");
                IntStream.range(0, data.size()).forEach(j -> sqlBuilder.append("?,"));
                sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(")");

                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    valueList.add(new Value(entry.getKey(), entry.getValue()));
                }
                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }
        }
    }

    public static class UpdateSqlBuilder implements Function<Dml, SQL> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public SQL apply(Dml dml) {
            Map<String, Object> data = dml.getData();
            Map<String, Object> old = dml.getOld();

            //拼接SET字段
            SQL sql = new UpdateSQL(data, old, dml.getDatabase(), dml.getTableName(),
                    dml.getPkNames(), dml.getEs());
            sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
            sql.originalSQL = sql.clone();
            return sql;
        }

        private static class UpdateSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            UpdateSQL(Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, Long es) {
                super("UPDATE", data, old, database, table, pkNames, es);
            }

            @Override
            public void flushSQL() {
                Map<String, Object> old = getOld();
                if (old == null || old.isEmpty()) {
                    setDiscardFlag(true);
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();

                sqlBuilder.append("UPDATE ").append(getTable()).append(" SET ");
                for (Map.Entry<String, Object> entry : getOld().entrySet()) {
                    sqlBuilder.append('`').append(entry.getKey()).append('`').append("=?, ");
                    valueList.add(new Value(entry.getKey(), getData().get(entry.getKey())));
                }

                int len = sqlBuilder.length();
                sqlBuilder.delete(len - 2, len).append(" WHERE ");

                // 拼接主键
                List<String> pkNames = getPkNames();
                if (pkNames != null) {
                    for (String targetColumnName : pkNames) {
                        sqlBuilder.append(targetColumnName).append("=? AND ");
                        //如果改ID的话
                        Object pk;
                        if (getOld().containsKey(targetColumnName)) {
                            pk = getOld().get(targetColumnName);
                        } else {
                            pk = getData().get(targetColumnName);
                        }
                        valueList.add(new Value(targetColumnName, pk));
                    }
                    sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
                }
                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }

        }
    }

    public static class DeleteSqlBuilder implements Function<Dml, SQL> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public SQL apply(Dml dml) {
            Map<String, Object> data = dml.getData();
            SQL sql = new DeleteSQL(data, null, dml.getDatabase(), dml.getTableName(),
                    dml.getPkNames(), dml.getEs());
            sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
            sql.originalSQL = sql.clone();
            return sql;
        }

        private static class DeleteSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            DeleteSQL(Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, Long es) {
                super("DELETE", data, old, database, table, pkNames, es);
            }

            @Override
            public void flushSQL() {
                List<String> pkNames = getPkNames();
                if (pkNames == null || pkNames.isEmpty()) {
                    setDiscardFlag(true);
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();
                sqlBuilder.append("DELETE FROM ").append(getTable()).append(" WHERE ");

                // 拼接主键
                if (pkNames != null) {
                    for (String targetColumnName : pkNames) {
                        sqlBuilder.append('`').append(targetColumnName).append('`').append("=? AND ");
                        valueList.add(new Value(targetColumnName, getData().get(targetColumnName)));
                    }
                    sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
                }

                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }
        }
    }

    public static class Value {
        private String columnName;
        private Object data;

        public Value(String columnName, Object data) {
            this.columnName = columnName;
            this.data = data;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return String.valueOf(data);
        }
    }
}
