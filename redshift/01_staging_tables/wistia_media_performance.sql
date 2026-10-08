CREATE TABLE IF NOT EXISTS staging.wistia_media_performance (
    media_id            VARCHAR(100),
    media_numeric_id    BIGINT,
    media_name          VARCHAR(500),
    duration_seconds    DOUBLE PRECISION,
    status              VARCHAR(100),
    media_type          VARCHAR(100),
    folder_name         VARCHAR(500),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    engagement          DOUBLE PRECISION,
    hours_watched       DOUBLE PRECISION,
    load_count          BIGINT,
    play_count          BIGINT,
    play_rate           DOUBLE PRECISION,
    total_visitors      BIGINT,
    load_timestamp      TIMESTAMP
);