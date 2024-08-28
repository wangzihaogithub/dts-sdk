package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);

        DtsSdkClient client = context.getBean(DtsSdkClient.class);
        listenEsRow(client);
        builder(client);
        listenEs(client);
    }


    private static void listenEsRow(DtsSdkClient client) {
        client.listenEsRow("cnwy_job_test_index_alias", 1577353, 5000)
                .whenComplete(new BiConsumer<ListenEsResponse, Throwable>() {
                    @Override
                    public void accept(ListenEsResponse listenEsResponse, Throwable throwable) {
                        System.out.println("listenEsResponse = " + listenEsResponse + "throwable" + throwable);
                    }
                });
    }

    private static void builder(DtsSdkClient client) {
        DtsEsRowFutureBuilder builder = DtsEsRowFutureBuilder.builder(client, Integer.MAX_VALUE, "job");
        builder.addPrimaryKey(621619);
        builder.addPrimaryKey(621622);

        CompletableFuture<ListenEsResponse> build = builder.build();
        build.whenComplete(new BiConsumer<ListenEsResponse, Throwable>() {
            @Override
            public void accept(ListenEsResponse listenEsResponse, Throwable throwable) {
                System.out.println("listenEsResponse = " + listenEsResponse);
            }
        });
    }

    private static void listenEs(DtsSdkClient client) {
        client.listenEs(new ListenEs() {
            private boolean done;

            @Override
            public boolean isDone() {
                return done;
            }

            @Override
            public void onEvent(Long messageId, EsDmlDTO dml) {
                if (Objects.equals(11000, dml.getIdInteger())) {
                    done = true;
                }
            }
        });
    }
}
