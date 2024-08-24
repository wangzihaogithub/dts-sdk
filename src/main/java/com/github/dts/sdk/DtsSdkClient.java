package com.github.dts.sdk;

import com.github.dts.sdk.client.DiscoveryService;
import com.github.dts.sdk.client.ServerInstanceClient;
import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.EsDmlDTO;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.Util;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiPredicate;

public class DtsSdkClient {
    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException("DtsSdkListenTimeout");
    private final DtsDumpListener dumpListener = new DtsDumpListener();
    private final ScheduledExecutorService scheduled = Util.newScheduled(1, "DTS-scheduled", true);
    private final ArrayList<ListenEs> listenEsList = new ArrayList<>();

    public DtsSdkClient(DtsSdkConfig config, DiscoveryService discoveryService, ListableBeanFactory beanFactory) {
        discoveryService.registerSdkInstance();
        scheduled.scheduleWithFixedDelay(new ClearListener(), config.getClearDoneInterval(), config.getClearDoneInterval(), TimeUnit.MILLISECONDS);

        ThreadPoolExecutor executor = Util.newFixedThreadPool(0, 1000, 30000, "DTS-dump", true, true);
        try (ReferenceCounted<List<ServerInstanceClient>> ref = discoveryService.getServerListRef()) {
            for (ServerInstanceClient client : ref.get()) {
                executor.execute(() -> client.dump(dumpListener, config.getRequestRetrySleep(), config.getRequestMaxRetry()));
            }
        }
        discoveryService.addServerListener(new DiscoveryService.ServerListener() {
            @Override
            public <E extends ServerInstanceClient> void onChange(DiscoveryService.ServerChangeEvent<E> event) {
                for (E client : event.insertList) {
                    executor.execute(() -> client.dump(dumpListener, config.getRequestRetrySleep(), config.getRequestMaxRetry()));
                }
            }
        });
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(String tableName, Object id) {
        return listenEsRow(Filters.primaryKey(tableName, id), 1, 500);
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(String tableName, Object id, long timeout) {
        return listenEsRow(Filters.primaryKey(tableName, id), 1, timeout);
    }

    public CompletableFuture<ListenEsResponse> listenEsRows(String tableName, Iterable<?> ids, long timeout) {
        Filters.UniquePrimaryKey filter = Filters.primaryKey(tableName, ids);
        return listenEsRow(filter, filter.rowCount(), timeout);
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(BiPredicate<Long, EsDmlDTO> rowFilter,
                                                           int rowCount, long timeout) {
        CompletableFuture<ListenEsResponse> future = new TimeoutCompletableFuture<>(timeout, scheduled);
        synchronized (listenEsList) {
            listenEsList.add(new RowListenEs(future, rowFilter, rowCount, timeout));
        }
        return future;
    }

    public void listenEs(ListenEs listenEs) {
        synchronized (listenEsList) {
            listenEsList.add(listenEs);
        }
    }

    public static class TimeoutCompletableFuture<T> extends CompletableFuture<T> {
        private final ScheduledFuture<?> timeoutScheduleFuture;

        public TimeoutCompletableFuture(long timeout, ScheduledExecutorService scheduled) {
            if (timeout > 0 && timeout < Integer.MAX_VALUE) {
                this.timeoutScheduleFuture = scheduled.schedule(() -> {
                    if (!isDone()) {
                        completeExceptionally(TIMEOUT_EXCEPTION);
                    }
                }, timeout, TimeUnit.MILLISECONDS);
            } else {
                this.timeoutScheduleFuture = null;
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                if (timeoutScheduleFuture != null) {
                    timeoutScheduleFuture.cancel(mayInterruptIfRunning);
                }
            } catch (Exception ignored) {

            }
            return super.cancel(mayInterruptIfRunning);
        }
    }


    private static class RowListenEs implements ListenEs {
        final CompletableFuture<ListenEsResponse> future;
        final BiPredicate<Long, EsDmlDTO> rowFilter;
        final int rowCount;
        final long timeout;
        final List<EsDmlDTO> hitList;
        final long timestamp = System.currentTimeMillis();

        private RowListenEs(CompletableFuture<ListenEsResponse> future, BiPredicate<Long, EsDmlDTO> rowFilter, int rowCount, long timeout) {
            this.future = future;
            this.rowFilter = rowFilter;
            this.rowCount = rowCount;
            this.timeout = timeout;
            this.hitList = new ArrayList<>(Math.max(rowCount, 0));
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public void onEvent(Long messageId, EsDmlDTO dml) {
            if (future.isDone()) {
                return;
            }
            if (rowFilter.test(messageId, dml)) {
                hitList.add(dml);
                if (hitList.size() >= rowCount) {
                    future.complete(new ListenEsResponse(hitList, timestamp));
                }
            }
        }

        @Override
        public String toString() {
            return "HitRowListenEs{" +
                    "done=" + future.isDone() +
                    ", rowTest=" + rowFilter +
                    ", rowCount=" + rowCount +
                    ", timeout=" + timeout +
                    '}';
        }
    }

    private class ClearListener implements Runnable {

        @Override
        public void run() {
            int size = listenEsList.size();
            if (size == 0) {
                return;
            }
            synchronized (listenEsList) {
                boolean b = listenEsList.removeIf(ListenEs::isDone);
                if (b && size > 1000) {
                    listenEsList.trimToSize();
                }
            }
        }
    }

    private class DtsDumpListener implements ServerInstanceClient.DumpListener {

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
