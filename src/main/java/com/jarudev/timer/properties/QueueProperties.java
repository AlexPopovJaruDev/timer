package com.jarudev.timer.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "timer-app.queue")
public class QueueProperties {

    Long maxBufferSize;
}
