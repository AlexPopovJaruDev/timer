package com.jarudev.timer.consumer;

import com.jarudev.timer.monitor.DbHealthMonitor;
import com.jarudev.timer.properties.ConsumerProperties;
import com.jarudev.timer.service.TimeService;
import com.jarudev.timer.storage.TimeQueueStorage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class TimeConsumer {

    private final TimeQueueStorage queueStorage;
    private final TimeService timeService;
    private final DbHealthMonitor dbHealthMonitor;
    private final ConsumerProperties properties;

    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "time-consumer");
                t.setDaemon(true);
                return t;
            });

    @PostConstruct
    public void start() {
        executor.submit(this::pollLoop);
    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void pollLoop() {
        log.info("TimeConsumer started.");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Если БД уже помечена как недоступная — не трогаем очередь, ждём.
                if (!dbHealthMonitor.isDbAvailable()) {
                    sleepSilently(properties.getDbUnavailableSleepMs());
                    continue;
                }

                int queueSize = queueStorage.size();
                if (queueSize == 0) {
                    sleepSilently(properties.getEmptyQueueSleepMs());
                    continue;
                }

                if (queueSize < properties.getBatchThreshold()) {
                    // Быстрая БД / небольшой лаг: пишем по одной записи
                    List<LocalDateTime> one = queueStorage.drainUpTo(1);
                    if (!one.isEmpty()) {
                        LocalDateTime ts = one.get(0);
                        timeService.writeOne(ts);
                        log.debug("Wrote single timestamp {}", ts);
                    }
                } else {
                    // Очередь заметно растёт — пишем батчами
                    List<LocalDateTime> batch = queueStorage.drainUpTo(properties.getMaxBatchSize());
                    if (!batch.isEmpty()) {
                        timeService.writeBatch(batch);
                        log.debug("Wrote batch of {} timestamps", batch.size());
                    }
                }

            } catch (Exception ex) {
                // ВАЖНО: цикл НЕ должен умирать из-за ошибки.
                log.error("Error in TimeConsumer iteration", ex);

                // Если это проблема соединения — TimeService уже вернул записи в очередь и пометил БД как UNAVAILABLE.
                // Здесь просто чуть подождём, чтобы не крутить цикл впустую.
                if (timeService.isConnectionProblem(ex) || !dbHealthMonitor.isDbAvailable()) {
                    sleepSilently(properties.getDbUnavailableSleepMs());
                }
                // Любая другая ошибка: просто логируем и идём дальше.
            }
        }
        log.info("TimeConsumer stopped (thread interrupted).");
    }

    private void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
