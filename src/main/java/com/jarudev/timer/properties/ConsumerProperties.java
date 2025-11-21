package com.jarudev.timer.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "timer-app.consumer")
public class ConsumerProperties {

    Long emptyQueueSleepMs;
    Long dbUnavailableSleepMs;
    int batchThreshold;
    int maxBatchSize;
}
