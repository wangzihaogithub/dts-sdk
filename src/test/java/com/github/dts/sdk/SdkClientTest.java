package com.github.dts.sdk;

import com.github.dts.sdk.util.Util;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SdkClientTest {
    public static void main(String[] args) throws InterruptedException {
//        DtsEsRowFutureBuilder builder = DtsEsRowFutureBuilder.builder(null);
        ScheduledThreadPoolExecutor newed = Util.newScheduled(1, () -> "1", System.out::println);
        newed.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("newed = " + newed);
            }
        }, 1, 1, TimeUnit.SECONDS);
        Thread.sleep(1000000000);
    }


}
