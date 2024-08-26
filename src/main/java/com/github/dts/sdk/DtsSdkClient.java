package com.github.dts.sdk;

import com.github.dts.sdk.client.DiscoveryService;
import com.github.dts.sdk.client.ServerInstanceClient;
import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.EsDmlDTO;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.Util;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

public class DtsSdkClient {
    private final DtsDumpListener dumpListener = new DtsDumpListener();
    private final ScheduledExecutorService scheduled = Util.newScheduled(1, "DTS-scheduled", true);
    private final LinkedList<ListenEs> listenEsList = new LinkedList<>();

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

    public ScheduledExecutorService getScheduled() {
        return scheduled;
    }

    public CompletableFuture<ListenEsResponse> listenEsRow(BiPredicate<Long, EsDmlDTO> rowFilter,
                                                           int rowCount, long timeout) {
        CompletableFuture<ListenEsResponse> future = new TimeoutCompletableFuture<>(timeout, scheduled);
        synchronized (listenEsList) {
            listenEsList.add(new DtsEsRowListener(future, rowFilter, rowCount));
        }
        return future;
    }

    public void listenEs(ListenEs listenEs) {
        synchronized (listenEsList) {
            listenEsList.add(listenEs);
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
                listenEsList.removeIf(ListenEs::isDone);
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
