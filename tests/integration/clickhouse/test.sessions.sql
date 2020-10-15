CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.sessions (
    time DateTime,
    id Int32,
    name String
)
ENGINE = MergeTree()
PARTITION BY toYYYMMDD(time)
ORDER BY time;


INSERT INTO test.sessions
SELECT now() + number * 3600 AS time, number AS id, toString(generateUUIDv4()) AS name
FROM numbers(2048);