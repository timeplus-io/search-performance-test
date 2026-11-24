-- Timeplus Enterprise: log stream with an inverted (text) index on message.
-- Default (append) stream engine, so the test also covers the streaming storage path.
-- split_by_non_alpha splits on ASCII punctuation/space only, so a run of Chinese characters
-- between separators (e.g. '交易失败' in '交易失败: ...') is one token. In current builds the
-- query-side index support (has_token, has_*_tokens, LIKE) only fires for this tokenizer.
CREATE STREAM IF NOT EXISTS app_logs
(
    log_time  datetime64(3, 'UTC'),
    level     low_cardinality(string),
    service   low_cardinality(string),
    host      low_cardinality(string),
    trace_id  string,
    message   string,
    INDEX idx_msg   message  TYPE text(tokenizer = 'split_by_non_alpha'),
    INDEX idx_trace trace_id TYPE bloom_filter
)
PARTITION BY to_YYYYMMDD(log_time)
ORDER BY (service, level, log_time)
SETTINGS event_time_column = 'log_time'
