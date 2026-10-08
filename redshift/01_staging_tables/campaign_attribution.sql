CREATE TABLE IF NOT EXISTS staging.campaign_attribution (
    report_date DATE,
    channel VARCHAR(100),
    utm_source VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_medium VARCHAR(255),
    total_bookings BIGINT,
    load_timestamp TIMESTAMP
);
