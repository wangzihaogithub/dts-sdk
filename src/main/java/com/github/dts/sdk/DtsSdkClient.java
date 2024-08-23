package com.github.dts.sdk;

import com.github.dts.sdk.client.DiscoveryService;
import com.github.dts.sdk.client.ServerInstanceClient;
import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.DmlDTO;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.Util;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.BiPredicate;

public class DtsSdkClient {
    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException("DtsSdkListenTimeout");
    private final DtsDumpListener dumpListener = new DtsDumpListener();
    private final ScheduledExecutorService scheduled = Util.newScheduled(1, "DTS-dump-scheduled", true);
    private final List<ListenEs> listenEsList = new ArrayList<>();

    public DtsSdkClient(DtsSdkConfig config, DiscoveryService discoveryService, ListableBeanFactory beanFactory) {
        discoveryService.registerSdkInstance();
        scheduled.scheduleWithFixedDelay(new ClearListener(), 100, 100, TimeUnit.MILLISECONDS);

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

    public CompletableFuture<ListenEsResponse> listenEs(String indexName, Object id) {
        return listenEs(new IndexIdTest(indexName, id), 1, 500);
    }

    public CompletableFuture<ListenEsResponse> listenEs(String indexName, Object id, long timeout) {
        return listenEs(new IndexIdTest(indexName, id), 1, timeout);
    }

    public CompletableFuture<ListenEsResponse> listenEs(BiPredicate<Long, DmlDTO> rowTest,
                                                        int rowCount, long timeout) {
        CompletableFuture<ListenEsResponse> future = new CompletableFuture<>();
        ListenEs listenEs = new ListenEs(future, rowTest, rowCount, timeout);
        listenEsList.add(listenEs);
        scheduled.schedule(() -> {
            if (!future.isDone()) {
                future.completeExceptionally(TIMEOUT_EXCEPTION);
            }
        }, timeout, TimeUnit.MILLISECONDS);
        return future;
    }

    private static class IndexIdTest implements BiPredicate<Long, DmlDTO> {
        private final String indexName;
        private final Object id;

        private IndexIdTest(String indexName, Object id) {
            this.indexName = indexName;
            this.id = id;
        }

        @Override
        public boolean test(Long messageId, DmlDTO dml) {
            if (!dml.getIndexNames().contains(indexName)) {
                return false;
            }
            Object[] ids = dml.getIds();
            if (ids.length != 1) {
                return false;
            }
            String rowIdString = Objects.toString(ids[0], null);
            String idString = Objects.toString(id, null);
            return Objects.equals(rowIdString, idString);
        }

        @Override
        public String toString() {
            return "IndexIdTest{" +
                    "indexName='" + indexName + '\'' +
                    ", id=" + id +
                    '}';
        }
    }

    public static class ListenEsResponse {
        final List<DmlDTO> hitList;

        public ListenEsResponse(List<DmlDTO> hitList) {
            this.hitList = hitList;
        }

        @Override
        public String toString() {
            return "ListenEsResponse{" +
                    "hitList=" + hitList +
                    '}';
        }
    }

    private static class ListenEs {
        final CompletableFuture<ListenEsResponse> future;
        final BiPredicate<Long, DmlDTO> rowTest;
        final int rowCount;
        final long timeout;
        final List<DmlDTO> hitList;

        private ListenEs(CompletableFuture<ListenEsResponse> future, BiPredicate<Long, DmlDTO> rowTest, int rowCount, long timeout) {
            this.future = future;
            this.rowTest = rowTest;
            this.rowCount = rowCount;
            this.timeout = timeout;
            this.hitList = new ArrayList<>(Math.max(rowCount, 0));
        }

        public void onEvent(Long messageId, DmlDTO dml) {
            if (future.isDone()) {
                return;
            }
            if (rowTest.test(messageId, dml)) {
                hitList.add(dml);
                if (hitList.size() >= rowCount) {
                    future.complete(new ListenEsResponse(hitList));
                }
            }
        }

        @Override
        public String toString() {
            return "ListenEs{" +
                    "done=" + future.isDone() +
                    ", rowTest=" + rowTest +
                    ", rowCount=" + rowCount +
                    ", timeout=" + timeout +
                    '}';
        }
    }

    private class ClearListener implements Runnable {

        @Override
        public void run() {
            if (listenEsList.isEmpty()) {
                return;
            }
            synchronized (listenEsList) {
                listenEsList.removeIf(e -> e.future.isDone());
            }
        }
    }

    private class DtsDumpListener implements ServerInstanceClient.DumpListener {

        @Override
        public void onEvent(Long messageId, Object data) {
            if (data instanceof DmlDTO) {
                DmlDTO dml = (DmlDTO) data;
                if (!listenEsList.isEmpty()) {
                    synchronized (listenEsList) {
                        for (ListenEs listenEs : listenEsList) {
                            listenEs.onEvent(messageId, dml);
                        }
                    }
                }
            }
        }
    }
}
