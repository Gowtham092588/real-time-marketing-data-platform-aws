CREATE TABLE IF NOT EXISTS gold.fact_marketing_spend
(
    spend_key     BIGINT IDENTITY(1,1) NOT NULL,
    date_key      INT NOT NULL,
    channel_key   BIGINT NOT NULL,
    spend_amount  DECIMAL(18,2) NOT NULL,

    PRIMARY KEY (spend_key)
);