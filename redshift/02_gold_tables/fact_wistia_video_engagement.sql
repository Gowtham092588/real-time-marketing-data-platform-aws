CREATE TABLE IF NOT EXISTS gold.fact_wistia_video_engagement
(
    engagement_key      BIGINT IDENTITY(1,1) NOT NULL,

    event_key           VARCHAR(255) NOT NULL,

    visitor_key         BIGINT,
    media_key           BIGINT,
    date_key            INT,

    event_timestamp     TIMESTAMP,

    play_count          BIGINT,
    load_count          BIGINT,

    engagement_percent  DOUBLE PRECISION,

    PRIMARY KEY (engagement_key)
);