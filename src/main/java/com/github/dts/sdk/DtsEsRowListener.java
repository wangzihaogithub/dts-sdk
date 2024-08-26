package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;

public class DtsEsRowListener implements ListenEs {
    private final CompletableFuture<ListenEsResponse> future;
    private final BiPredicate<Long, EsDmlDTO> rowFilter;
    private final int rowCount;
    private final List<EsDmlDTO> hitList;
    private final long timestamp = System.currentTimeMillis();

    public DtsEsRowListener(CompletableFuture<ListenEsResponse> future, BiPredicate<Long, EsDmlDTO> rowFilter, int rowCount) {
        this.future = future;
        this.rowFilter = rowFilter;
        this.rowCount = rowCount;
        this.hitList = new ArrayList<>(Math.max(rowCount, 0));
    }

    public CompletableFuture<ListenEsResponse> future() {
        return future;
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
                '}';
    }
}