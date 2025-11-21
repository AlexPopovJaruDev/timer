--liquibase formatted sql

--changeset jarudev:create-time-entry-table
CREATE TABLE IF NOT EXISTS time_entry
(
    time TIMESTAMP PRIMARY KEY
);