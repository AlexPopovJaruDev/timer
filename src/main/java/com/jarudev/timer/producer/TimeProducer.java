package com.jarudev.timer.producer;

import com.jarudev.timer.storage.TimeQueueStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class TimeProducer {

    private final TimeQueueStorage queueStorage;

    @Scheduled(fixedRate = 1000)
    public void publishCurrentTime() {
        LocalDateTime now = LocalDateTime.now();
        queueStorage.offer(now);
        log.debug("Published timestamp to queue: {}", now);
    }
}
