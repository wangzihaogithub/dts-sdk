package com.github.dts.sdk.util;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class DmlDTO {
    private static final Object[] EMPTY = new Object[0];
    private String tableName;
    private String database;
    private List<String> pkNames;
    private Long es;
    private Long ts;
    private String type;
    private Map<String, Object> old;
    private Map<String, Object> data;
    private List<String> indexNames;
    private List<String> desc;
    private Boolean effect;
    private transient Object[] id;

    public Boolean getEffect() {
        return effect;
    }

    public void setEffect(Boolean effect) {
        this.effect = effect;
    }

    public String getIdString() {
        Object id = getId();
        return id == null ? null : id.toString();
    }

    public Long getIdLong() {
        Object id = getId();
        return id == null ? null : Long.valueOf(id.toString());
    }

    public Integer getIdInteger() {
        Object id = getId();
        return id == null ? null : Integer.valueOf(id.toString());
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
        return "DmlDTO{" +
                "tableName='" + tableName + '\'' +
                ", database='" + database + '\'' +
                ", es=" + new Timestamp(es) +
                ", type='" + type + '\'' +
                ", indexNames=" + indexNames +
                ", desc=" + desc +
                '}';
    }

    public List<String> getDesc() {
        return desc;
    }

    public void setDesc(List<String> desc) {
        this.desc = desc;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public List<String> getIndexNames() {
        return indexNames;
    }

    public void setIndexNames(List<String> indexNames) {
        this.indexNames = indexNames;
    }
}
