package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;

import java.util.List;

public class ListenEsResponse {
    private final long timestamp = System.currentTimeMillis();
    private final long startTimestamp;
    private final List<EsDmlDTO> hitList;

    public ListenEsResponse(List<EsDmlDTO> hitList, long startTimestamp) {
        this.hitList = hitList;
        this.startTimestamp = startTimestamp;
    }

    public List<EsDmlDTO> getHitList() {
        return hitList;
    }

    public long getCost() {
        return startTimestamp - timestamp;
    }

    @Override
    public String toString() {
        return "ListenEsResponse{" +
                "cost=" + getCost() + "ms" +
                ", hitList=" + hitList +
                '}';
    }
}