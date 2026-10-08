CREATE TABLE IF NOT EXISTS gold.dim_channel
(
    channel_key   BIGINT IDENTITY(1,1) NOT NULL,
    channel_name  VARCHAR(100) NOT NULL,

    PRIMARY KEY (channel_key)
);

ALTER TABLE gold.dim_channel
ADD COLUMN campaign_name VARCHAR(150);

ALTER TABLE gold.dim_channel
ADD COLUMN event_type_uri VARCHAR(255);