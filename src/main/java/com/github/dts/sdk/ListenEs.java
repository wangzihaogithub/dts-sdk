package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;

public interface ListenEs {
    boolean isDone();

    void onEvent(Long messageId, EsDmlDTO dml);
}