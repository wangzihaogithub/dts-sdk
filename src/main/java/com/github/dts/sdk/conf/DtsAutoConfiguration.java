package com.github.dts.sdk.conf;

import com.github.dts.sdk.DtsSdkClient;
import com.github.dts.sdk.client.DiscoveryService;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(DtsSdkConfig.class)
@Configuration
public class DtsAutoConfiguration {
    @Bean
    public DtsSdkClient dtsClient(DtsSdkConfig config, DiscoveryService discoveryService) {
        return new DtsSdkClient(config, discoveryService);
    }

    @Bean
    public DiscoveryService discoveryService(DtsSdkConfig config, ListableBeanFactory beanFactory) {
        return DiscoveryService.newInstance(config.getCluster(), beanFactory);
    }
}
