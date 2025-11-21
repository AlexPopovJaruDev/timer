package com.jarudev.timer.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class TimeRepository {

    private static final String INSERT_SQL =
            "INSERT INTO time_entry (time) VALUES (?)";

    private static final String SELECT_ALL_SQL =
            "SELECT time FROM time_entry";

    private final JdbcTemplate jdbcTemplate;

    public void saveOne(LocalDateTime timestamp) {
        jdbcTemplate.update(INSERT_SQL, Timestamp.valueOf(timestamp));
    }

    public void saveBatch(List<LocalDateTime> timestamps) {
        jdbcTemplate.batchUpdate(
                INSERT_SQL,
                timestamps,
                timestamps.size(),
                (ps, ts) -> ps.setTimestamp(1, Timestamp.valueOf(ts))
        );
    }

    public List<LocalDateTime> findAll() {
        // Без ORDER BY — выполняем ТЗ
        return jdbcTemplate.query(
                SELECT_ALL_SQL,
                (rs, rowNum) -> rs.getTimestamp("time").toLocalDateTime()
        );
    }
}
