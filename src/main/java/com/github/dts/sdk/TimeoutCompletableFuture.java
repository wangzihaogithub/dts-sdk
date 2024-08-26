package com.github.dts.sdk;

import java.util.concurrent.*;

public class TimeoutCompletableFuture<T> extends CompletableFuture<T> {
    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException("DtsSdkListenTimeout");

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