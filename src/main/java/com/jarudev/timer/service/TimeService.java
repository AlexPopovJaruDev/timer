package com.jarudev.timer.service;

import com.jarudev.timer.monitor.DbHealthMonitor;
import com.jarudev.timer.repository.TimeRepository;
import com.jarudev.timer.storage.TimeQueueStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TimeService {

    private final TimeRepository repository;
    private final TimeQueueStorage queueStorage;
    private final DbHealthMonitor dbHealthMonitor;

    public void writeOne(LocalDateTime timestamp) {
        try {
            repository.saveOne(timestamp);
        } catch (RuntimeException ex) {
            handleWriteFailure(List.of(timestamp), ex);
        }
    }

    public void writeBatch(List<LocalDateTime> timestamps) {
        if (timestamps.isEmpty()) {
            return;
        }
        try {
            repository.saveBatch(timestamps);
        } catch (RuntimeException ex) {
            handleWriteFailure(timestamps, ex);
        }
    }

    public List<LocalDateTime> findAll() {
        try {
            return repository.findAll();
        } catch (DataAccessException ex) {
            if (isConnectionProblem(ex)) {
                dbHealthMonitor.markDbAsUnavailable();
            }
            throw ex;
        }
    }

    private void handleWriteFailure(List<LocalDateTime> timestamps, RuntimeException ex) {
        if (isConnectionProblem(ex)) {
            log.error("DB connection problem during write, re-queueing {} items", timestamps.size(), ex);
            queueStorage.returnToHead(timestamps);
            dbHealthMonitor.markDbAsUnavailable();
        } else {
            log.error("Unexpected DB error during write", ex);
            // В проде можно сделать специфичнее, но здесь честно пробрасываем
            throw ex;
        }
    }

    public boolean isConnectionProblem(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if (sqlState != null && sqlState.startsWith("08")) {
                    // 08xxx — классы SQLState для connection errors
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }
}
