package com.github.dts.sdk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);

        DtsSdkClient client = context.getBean(DtsSdkClient.class);

        CompletableFuture<DtsSdkClient.ListenEsResponse> future = client.listenEs(
                "cnwy_job_test_index_alias", 1577353, 100);

        future.whenComplete(new BiConsumer<DtsSdkClient.ListenEsResponse, Throwable>() {
            @Override
            public void accept(DtsSdkClient.ListenEsResponse listenEsResponse, Throwable throwable) {
                System.out.println("listenEsResponse = " + listenEsResponse + "throwable" + throwable);
            }
        });
    }
}
