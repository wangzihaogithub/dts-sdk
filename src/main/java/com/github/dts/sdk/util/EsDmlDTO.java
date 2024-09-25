package com.github.dts.sdk.util;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class EsDmlDTO implements Dml {
    private static final Object[] EMPTY = new Object[0];
    private String tableName;
    private String database;
    private List<String> pkNames;
    private Long es;
    private Long ts;
    private String type;
    private Map<String, Object> old;
    private Map<String, Object> data;
    private List<Dependent> dependents;
    private String adapterName;

    private transient Object[] id;
    private transient String toStringCache;

    public boolean isEffect() {
        if (dependents == null || dependents.isEmpty()) {
            return false;
        }
        for (Dependent dependent : dependents) {
            if (Boolean.TRUE.equals(dependent.effect)) {
                return true;
            }
        }
        return false;
    }

    public String getIdString() {
        Object id = getId();
        return id == null ? null : id.toString();
    }

    public Long getIdLong() {
        Object id = getId();
        return id == null ? null : id instanceof Long ? (Long) id : Long.valueOf(id.toString());
    }

    public Integer getIdInteger() {
        Object id = getId();
        return id == null ? null : id instanceof Integer ? (Integer) id : Integer.valueOf(id.toString());
    }

    public Object getId() {
        Object[] ids = getIds();
        return ids.length > 0 ? ids[0] : null;
    }

    public Object[] getIds() {
        if (id == null) {
            Object[] ids;
            if (pkNames == null || pkNames.isEmpty()) {
                ids = EMPTY;
            } else {
                ids = new Object[pkNames.size()];
                for (int i = 0; i < pkNames.size(); i++) {
                    String pkName = pkNames.get(i);
                    Object rowId = null;
                    if (old != null && old.containsKey(pkName)) {
                        rowId = old.get(pkName);
                    } else if (data != null && data.containsKey(pkName)) {
                        rowId = data.get(pkName);
                    }
                    ids[i] = rowId;
                }
            }
            id = ids;
        }
        return id;
    }

    @Override
    public String toString() {
        if (toStringCache == null) {
            toStringCache = "EsDmlDTO{" +
                    sql() +
                    ", effect=" + isEffect() +
                    ", es=" + new Timestamp(es) +
                    ", adapterName=" + adapterName +
                    '}';
        }
        return toStringCache;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public void setAdapterName(String adapterName) {
        this.adapterName = adapterName;
    }

    public List<Dependent> getDependents() {
        return dependents;
    }

    public void setDependents(List<Dependent> dependents) {
        this.dependents = dependents;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    @Override
    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    @Override
    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    @Override
    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public static class Dependent {
        private String name;
        private Boolean effect;
        private String esIndex;

        @Override
        public String toString() {
            return "Dependent{" +
                    "name='" + name + '\'' +
                    ", effect=" + effect +
                    ", esIndex='" + esIndex + '\'' +
                    '}';
        }

        public String getEsIndex() {
            return esIndex;
        }

        public void setEsIndex(String esIndex) {
            this.esIndex = esIndex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getEffect() {
            return effect;
        }

        public void setEffect(Boolean effect) {
            this.effect = effect;
        }
    }
}
