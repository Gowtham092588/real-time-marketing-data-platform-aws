CREATE TABLE IF NOT EXISTS gold.channel_performance (
    report_date DATE,
    channel VARCHAR(100),
    total_bookings BIGINT,
    total_spend DECIMAL(12,2),
    cost_per_booking DECIMAL(12,2),
    load_timestamp TIMESTAMP
);