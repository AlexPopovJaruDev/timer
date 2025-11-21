package com.jarudev.timer.monitor;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class DbHealthMonitor {

    private final JdbcTemplate jdbcTemplate;

    private final AtomicBoolean dbAvailable = new AtomicBoolean(true);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "db-health-monitor");
                t.setDaemon(true);
                return t;
            });

    private volatile ScheduledFuture<?> healthTask;

    public DbHealthMonitor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean isDbAvailable() {
        return dbAvailable.get();
    }

    public void markDbAsUnavailable() {
        if (dbAvailable.compareAndSet(true, false)) {
            log.warn("Marking DB as UNAVAILABLE. Starting health-check every 5 seconds.");
            startHealthCheck();
        }
    }

    private void markDbAsAvailable() {
        if (dbAvailable.compareAndSet(false, true)) {
            log.info("DB is AVAILABLE again. Stopping health-check.");
            stopHealthCheck();
        }
    }

    private void startHealthCheck() {
        if (healthTask != null && !healthTask.isCancelled()) {
            return;
        }
        healthTask = scheduler.scheduleAtFixedRate(
                this::checkDb,
                0,
                5,
                TimeUnit.SECONDS
        );
    }

    private void stopHealthCheck() {
        ScheduledFuture<?> localTask = this.healthTask;
        if (Objects.nonNull(localTask)) {
            localTask.cancel(false);
            this.healthTask = null;
        }
    }

    private void checkDb() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            markDbAsAvailable();
        } catch (Exception ex) {
            log.warn("DB health-check failed: {}", ex.getMessage());
            // оставляем флаг false
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
