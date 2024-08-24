package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;

import java.util.*;
import java.util.function.BiPredicate;

public class Filters {

    public static UniquePrimaryKey primaryKey(String tableName, Object id) {
        return new UniquePrimaryKey(tableName, id);
    }

    public static UniquePrimaryKey primaryKey(String tableName, Iterable<?> ids) {
        return new UniquePrimaryKey(tableName, ids);
    }

    public static UnionPrimaryKey unionPrimaryKey(String tableName, Object[] id) {
        return new UnionPrimaryKey(tableName, id);
    }

    public static UnionPrimaryKey unionPrimaryKey(String tableName, Iterable<Object[]> ids) {
        return new UnionPrimaryKey(tableName, ids);
    }

    public static class UniquePrimaryKey implements BiPredicate<Long, EsDmlDTO> {
        private final String tableName;
        private final Set<String> primaryKeyStringSet;

        public UniquePrimaryKey(String tableName, Object id) {
            this.tableName = tableName;
            this.primaryKeyStringSet = Collections.singleton(Objects.toString(id, null));
        }

        public UniquePrimaryKey(String tableName, Iterable<?> ids) {
            this.tableName = tableName;
            Set<String> primaryKeyStringSet;
            if (ids instanceof Collection) {
                primaryKeyStringSet = new HashSet<>((int) (((Collection<?>) ids).size() / 0.75 + 1));
            } else {
                primaryKeyStringSet = new HashSet<>();
            }
            for (Object id : ids) {
                primaryKeyStringSet.add(Objects.toString(id, null));
            }
            this.primaryKeyStringSet = primaryKeyStringSet;
        }

        public int rowCount() {
            return primaryKeyStringSet.size();
        }

        @Override
        public boolean test(Long messageId, EsDmlDTO dml) {
            if (!dml.getTableName().equalsIgnoreCase(tableName)) {
                return false;
            }
            Object[] ids = dml.getIds();
            if (ids.length != 1) {
                return false;
            }
            String rowIdString = Objects.toString(ids[0], null);
            return primaryKeyStringSet.contains(rowIdString);
        }

        @Override
        public String toString() {
            return "UniquePrimaryKeyTester{" +
                    "tableName='" + tableName + '\'' +
                    ", id=" + primaryKeyStringSet +
                    '}';
        }
    }

    public static class UnionPrimaryKey implements BiPredicate<Long, EsDmlDTO> {
        private final String tableName;
        private final Set<String> primaryKeyStringSet;
        private final int unionCount;

        public UnionPrimaryKey(String tableName, Object[] id) {
            this.tableName = tableName;
            this.primaryKeyStringSet = Collections.singleton(join(id));
            this.unionCount = id.length;
        }

        public UnionPrimaryKey(String tableName, Iterable<Object[]> ids) {
            this.tableName = tableName;
            Set<String> primaryKeyStringSet;
            if (ids instanceof Collection) {
                primaryKeyStringSet = new HashSet<>((int) (((Collection<?>) ids).size() / 0.75 + 1));
            } else {
                primaryKeyStringSet = new HashSet<>();
            }
            int unionCount = 0;
            for (Object[] id : ids) {
                if (unionCount == 0) {
                    unionCount = id.length;
                }
                primaryKeyStringSet.add(join(id));
            }
            this.primaryKeyStringSet = primaryKeyStringSet;
            this.unionCount = unionCount;
        }

        private static String join(Object[] objects) {
            StringJoiner joiner = new StringJoiner("-");
            for (Object object : objects) {
                joiner.add(String.valueOf(object));
            }
            return joiner.toString();
        }

        public int rowCount() {
            return primaryKeyStringSet.size();
        }

        @Override
        public boolean test(Long messageId, EsDmlDTO dml) {
            if (!dml.getTableName().equalsIgnoreCase(tableName)) {
                return false;
            }
            Object[] ids = dml.getIds();
            if (ids.length != unionCount) {
                return false;
            }
            String rowIdString = join(ids);
            return primaryKeyStringSet.contains(rowIdString);
        }

        @Override
        public String toString() {
            return "UnionPrimaryKeyTester{" +
                    "tableName='" + tableName + '\'' +
                    ", id=" + primaryKeyStringSet +
                    '}';
        }
    }
}
