CREATE TABLE test_table (
    content         TEXT,
    logdate         date not null
) PARTITION BY RANGE (logdate);