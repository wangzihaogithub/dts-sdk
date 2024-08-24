package com.github.dts.sdk;

import com.github.dts.sdk.util.EsDmlDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Objects;
import java.util.function.BiConsumer;

@SpringBootApplication
public class Application {
    @Autowired
    private DtsSdkClient dtsSdkClient;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);

        DtsSdkClient client = context.getBean(DtsSdkClient.class);

        client.listenEsRow("cnwy_job_test_index_alias", 1577353, 100).whenComplete(new BiConsumer<ListenEsResponse, Throwable>() {
            @Override
            public void accept(ListenEsResponse listenEsResponse, Throwable throwable) {
                System.out.println("listenEsResponse = " + listenEsResponse + "throwable" + throwable);
            }
        });

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
