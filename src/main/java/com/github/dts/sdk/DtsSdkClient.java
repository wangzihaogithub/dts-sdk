package com.github.dts.sdk;

import com.github.dts.sdk.client.DiscoveryService;
import com.github.dts.sdk.client.ServerInstanceClient;
import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.EsDmlDTO;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

public class DtsSdkClient {
    public static final long DEFAULT_ROW_TIMEOUT = 2000;
    private static final Logger log = LoggerFactory.getLogger(DtsSdkClient.class);
    private final LinkedList<ListenEs> listenEsList = new LinkedList<>();
    private final ScheduledExecutorService scheduled = Util.newScheduled(
            1, () -> "DTS-scheduled", e -> log.warn("Scheduled error {}", e.toString(), e));
    private final ThreadPoolExecutor executor = Util.newFixedThreadPool(
            0, 1000, 30000, "DTS-dump", true, true);
    private final AtomicInteger dumpCount = new AtomicInteger(0);

    public DtsSdkClient(DtsSdkConfig config, DiscoveryService discoveryService) {
        discoveryService.registerSdkInstance();
        ClearListener clearListener = new ClearListener(listenEsList);
        scheduled.scheduleWithFixedDelay(clearListener, config.getClearDoneInterval(), config.getClearDoneInterval(), TimeUnit.MILLISECONDS);

        DtsDumpListener dumpListener = new DtsDumpListener(listenEsList);
        try (ReferenceCounted<List<ServerInstanceClient>> ref = discoveryService.getServerListRef()) {
            for (ServerInstanceClient client : ref.get()) {
                executor.execute(() -> executeDump(client, dumpListener, config, dumpCount));
            }
        }
        discoveryService.addServerListener(new DiscoveryService.ServerListener() {
            @Override
            public <E extends ServerInstanceClient> void onChange(DiscoveryService.ServerChangeEvent<E> event) {
                for (E client : event.insertList) {
                    executor.execute(() -> executeDump(client, dumpListener, config, dumpCount));
                }
            }
        });
    }

    private static void executeDump(ServerInstanceClient client, DtsDumpListener dumpListener, DtsSdkConfig config, AtomicInteger dumpCount) {
        dumpCount.incrementAndGet();
        try {
            client.dump(dumpListener, config.getRequestRetrySleep(), config.getRequestMaxRetry());
        } finally {
            dumpCount.decrementAndGet();
        }
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(String tableName, Object id) {
        if (id instanceof Iterable) {
            Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, (Iterable<?>) id);
            return listenEsRow(filter, filter.rowCount(), DEFAULT_ROW_TIMEOUT);
        } else {
            return listenEsRow(Filters.primaryKey(tableName, id), 1, DEFAULT_ROW_TIMEOUT);
        }
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(String tableName, Object id, long timeout) {
        if (id instanceof Iterable) {
            Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, (Iterable<?>) id);
            return listenEsRow(filter, filter.rowCount(), timeout);
        } else {
            return listenEsRow(Filters.primaryKey(tableName, id), 1, timeout);
        }
    }

    public <T> CompletableFuture<T> listenEsRow(String tableName, Object id, long timeout, Supplier<T> supplier) {
        return listenEsRow(tableName, id, timeout)
                .handle((r, t) -> supplier.get());
    }

    public <T> CompletableFuture<T> listenEsRow(String tableName, Object id, long timeout, T result) {
        return listenEsRow(tableName, id, timeout)
                .handle((r, t) -> result);
    }

    public CompletableFuture<ListenEsResponse> listenEsRows(String tableName, Iterable<?> ids, long timeout) {
        Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, ids);
        return listenEsRow(filter, filter.rowCount(), timeout);
    }

    public <T> CompletableFuture<T> listenEsRows(String tableName, Iterable<?> ids, long timeout, BiFunction<ListenEsResponse, Throwable, T> map) {
        return listenEsRows(tableName, ids, timeout)
                .handle(map);
    }

    public <T> CompletableFuture<T> listenEsRows(String tableName, Iterable<?> ids, long timeout, T result) {
        return listenEsRows(tableName, ids, timeout)
                .handle((r, t) -> result);
    }

    public <T> CompletableFuture<T> listenEsRows(String tableName, Iterable<?> ids, long timeout, Supplier<T> supplier) {
        return listenEsRows(tableName, ids, timeout)
                .handle((r, t) -> supplier.get());
    }

    public ScheduledExecutorService getScheduled() {
        return scheduled;
    }

    public int getDumpCount() {
        return dumpCount.get();
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(BiPredicate<Long, EsDmlDTO> rowFilter,
                                                           int rowCount, long timeout) {
        if (dumpCount.get() == 0) {
            return CompletableFuture.completedFuture(new ListenEsResponse(Collections.emptyList(), System.currentTimeMillis()));
        } else {
            CompletableFuture<ListenEsResponse> future = new TimeoutCompletableFuture<>(timeout, scheduled);
            listenEs(new DtsEsRowListener(future, rowFilter, rowCount));
            return future;
        }
    }

    public void listenEs(ListenEs listenEs) {
        synchronized (listenEsList) {
            listenEsList.add(listenEs);
        }
    }

    private static class ClearListener implements Runnable {
        private final LinkedList<ListenEs> listenEsList;

        public ClearListener(LinkedList<ListenEs> listenEsList) {
            this.listenEsList = listenEsList;
        }

        @Override
        public void run() {
            int size = listenEsList.size();
            if (size == 0) {
                return;
            }
            synchronized (listenEsList) {
                listenEsList.removeIf(ListenEs::isDone);
            }
        }
    }

    private static class DtsDumpListener implements ServerInstanceClient.DumpListener {
        private final LinkedList<ListenEs> listenEsList;

        public DtsDumpListener(LinkedList<ListenEs> listenEsList) {
            this.listenEsList = listenEsList;
        }

        @Override
        public void onEvent(Long messageId, Object data) {
            if (data instanceof EsDmlDTO) {
                EsDmlDTO dml = (EsDmlDTO) data;
                if (!listenEsList.isEmpty()) {
                    synchronized (listenEsList) {
                        for (ListenEs listenEs : listenEsList) {
                            if (!listenEs.isDone()) {
                                listenEs.onEvent(messageId, dml);
                            }
                        }
                    }
                }
            }
        }
    }
}
