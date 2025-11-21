package com.jarudev.timer.exception;

import com.jarudev.timer.dto.ErrorResponse;
import com.jarudev.timer.monitor.DbHealthMonitor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.sql.SQLTransientConnectionException;

@Slf4j
@RestControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final DbHealthMonitor dbHealthMonitor;

    @ExceptionHandler({
            CannotGetJdbcConnectionException.class,
            SQLTransientConnectionException.class
    })
    public ResponseEntity<ErrorResponse> handleConnectionProblems(Exception ex) {
        log.warn("Database connection problem", ex);
        dbHealthMonitor.markDbAsUnavailable();

        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(
                        "DB_UNAVAILABLE",
                        "Database is temporarily unavailable. Please try again later."
                ));
    }

    // 2) Слишком долгий запрос (statement/query timeout)
    @ExceptionHandler(org.springframework.dao.QueryTimeoutException.class)
    public ResponseEntity<ErrorResponse> handleQueryTimeout(org.springframework.dao.QueryTimeoutException ex) {
        log.warn("Query timeout", ex);
        return ResponseEntity
                .status(HttpStatus.GATEWAY_TIMEOUT) // или 503, как захочешь
                .body(new ErrorResponse(
                        "DB_QUERY_TIMEOUT",
                        "Database took too long to respond."
                ));
    }

    // 3) Прочие DB-ошибки (NOT connection, NOT timeout)
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ErrorResponse> handleDataAccess(DataAccessException ex) {
        log.error("Database error", ex);
        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(
                        "DB_ERROR",
                        "Database error occurred. Please try again later."
                ));
    }

    // 4) Остальное - как и раньше
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex) {
        log.error("Unexpected exception", ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(
                        "INTERNAL_ERROR",
                        "Unexpected error occurred."
                ));
    }
}
