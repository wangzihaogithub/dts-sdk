package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DtsEsRowFutureBuilder {
    public static final long DEFAULT_ROW_TIMEOUT = 500;
    private final BeforeBuilderListenEs listenEs;
    private final CompletableFuture<ListenEsResponse> future = new CompletableFuture<>();
    private final AtomicBoolean build = new AtomicBoolean();

    public DtsEsRowFutureBuilder(DtsSdkClient client, Collection<String> tableNames, long rowTimeout) {
        this.listenEs = new BeforeBuilderListenEs(client, tableNames, rowTimeout);
        client.listenEs(listenEs);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client) {
        return new DtsEsRowFutureBuilder(client, null, DEFAULT_ROW_TIMEOUT);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client, long rowTimeout) {
        return new DtsEsRowFutureBuilder(client, null, rowTimeout);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client, long rowTimeout, String... tableNames) {
        return new DtsEsRowFutureBuilder(client, Arrays.asList(tableNames), rowTimeout);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client, String... tableNames) {
        return new DtsEsRowFutureBuilder(client, Arrays.asList(tableNames), DEFAULT_ROW_TIMEOUT);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client, Collection<String> tableNames) {
        return new DtsEsRowFutureBuilder(client, tableNames, DEFAULT_ROW_TIMEOUT);
    }

    public static DtsEsRowFutureBuilder builder(DtsSdkClient client, long rowTimeout, Collection<String> tableNames) {
        return new DtsEsRowFutureBuilder(client, tableNames, rowTimeout);
    }

    private static ArrayList<EsDmlDTO> getHitList(List<ListenEsResponse> list) {
        int hitListSize = 0;
        for (ListenEsResponse response : list) {
            List<EsDmlDTO> hitList1 = response.getHitList();
            if (hitList1 != null) {
                hitListSize += hitList1.size();
            }
        }
        ArrayList<EsDmlDTO> result = new ArrayList<>(hitListSize);
        for (ListenEsResponse response : list) {
            List<EsDmlDTO> hitList1 = response.getHitList();
            if (hitList1 != null) {
                result.addAll(hitList1);
            }
        }
        return result;
    }

    public DtsEsRowFutureBuilder addPrimaryKey(Object id) {
        addPrimaryKey(null, id, listenEs.rowTimeout);
        return this;
    }

    public DtsEsRowFutureBuilder addPrimaryKey(String tableName, Object id) {
        addPrimaryKey(tableName, id, listenEs.rowTimeout);
        return this;
    }

    public DtsEsRowFutureBuilder addPrimaryKey(String tableName, Object id, long timeout) {
        synchronized (listenEs) {
            if (build.get()) {
                throw new IllegalStateException("please call before build!");
            }
            Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, id);
            CompletableFuture<ListenEsResponse> future = new TimeoutCompletableFuture<>(timeout, listenEs.client.getScheduled());
            DtsEsRowListener listener = new DtsEsRowListener(future, filter, filter.rowCount());
            listenEs.add(listener);
        }
        return this;
    }

    public DtsEsRowFutureBuilder addPrimaryKey(String tableName, Iterable<?> ids, long timeout) {
        synchronized (listenEs) {
            if (build.get()) {
                throw new IllegalStateException("please call before build!");
            }
            Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, ids);
            CompletableFuture<ListenEsResponse> future = new TimeoutCompletableFuture<>(timeout, listenEs.client.getScheduled());
            DtsEsRowListener listener = new DtsEsRowListener(future, filter, filter.rowCount());
            listenEs.add(listener);
        }
        return this;
    }

    public DtsEsRowFutureBuilder add(DtsEsRowListener listener) {
        synchronized (listenEs) {
            if (build.get()) {
                throw new IllegalStateException("please call before build!");
            }
            listenEs.add(listener);
        }
        return this;
    }

    public CompletableFuture<ListenEsResponse> build() {
        if (build.compareAndSet(false, true)) {
            synchronized (listenEs) {
                int size = listenEs.listenerList.size();
                long startTimestamp = listenEs.startTimestamp == 0L ? System.currentTimeMillis() : listenEs.startTimestamp;
                if (size == 0) {
                    future.complete(new ListenEsResponse(new ArrayList<>(), startTimestamp));
                } else {
                    AtomicInteger counter = new AtomicInteger(size);
                    List<ListenEsResponse> list = new ArrayList<>(size);
                    for (DtsEsRowListener listener : listenEs.listenerList) {
                        listener.future().whenComplete((listenEsResponse, throwable) -> {
                            if (future.isDone()) {
                                return;
                            }
                            if (throwable != null) {
                                future.completeExceptionally(throwable);
                            } else {
                                if (listenEsResponse != null) {
                                    list.add(listenEsResponse);
                                }
                                if (counter.decrementAndGet() == 0) {
                                    ArrayList<EsDmlDTO> hitList = getHitList(list);
                                    list.clear();
                                    future.complete(new ListenEsResponse(hitList, startTimestamp));
                                }
                            }
                        });
                    }
                }
            }
            listenEs.done();
        }
        return future;
    }

    private static class BeforeBuilderListenEs implements ListenEs {
        private final long rowTimeout;
        private final DtsSdkClient client;
        private final Set<String> tableNames;
        private final List<DtsEsRowListener> listenerList = new ArrayList<>();
        private final ArrayList<Event> collectList = new ArrayList<>();
        private long startTimestamp;
        private volatile boolean done;

        private BeforeBuilderListenEs(DtsSdkClient client, Collection<String> tableNames, long rowTimeout) {
            this.client = client;
            this.tableNames = tableNames == null || tableNames.isEmpty() ? null : new HashSet<>(tableNames);
            this.rowTimeout = rowTimeout;
        }

        private void add(DtsEsRowListener listenEs) {
            if (startTimestamp == 0L) {
                startTimestamp = System.currentTimeMillis();
            }
            // read
            synchronized (collectList) {
                client.listenEs(listenEs);
                boolean done = false;
                for (Event event : collectList) {
                    listenEs.onEvent(event.messageId, event.dml);
                    if (listenEs.isDone()) {
                        done = true;
                        break;
                    }
                }
                if (!done) {
                    listenerList.add(listenEs);
                }
            }
        }

        @Override
        public boolean isDone() {
            return done;
        }

        private void done() {
            // delete
            synchronized (collectList) {
                this.done = true;
                collectList.clear();
                collectList.trimToSize();
            }
        }

        @Override
        public void onEvent(Long messageId, EsDmlDTO dml) {
            if (done) {
                // delete
                synchronized (collectList) {
                    collectList.clear();
                    collectList.trimToSize();
                }
            } else {
                // insert
                synchronized (collectList) {
                    if (tableNames == null || tableNames.contains(dml.getTableName())) {
                        collectList.add(new Event(messageId, dml));
                    }
                }
            }
        }

        static class Event {
            Long messageId;
            EsDmlDTO dml;

            Event(Long messageId, EsDmlDTO dml) {
                this.messageId = messageId;
                this.dml = dml;
            }
        }
    }
}
