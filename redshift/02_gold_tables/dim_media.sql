CREATE TABLE IF NOT EXISTS gold.dim_media
(
    media_key   BIGINT IDENTITY(1,1) NOT NULL,
    media_id    VARCHAR(100) NOT NULL,
    title       VARCHAR(500),
    created_at  TIMESTAMP,

    PRIMARY KEY (media_key)
);