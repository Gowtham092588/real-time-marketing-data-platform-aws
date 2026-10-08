CREATE TABLE IF NOT EXISTS gold.booking_time_analysis (
    report_date DATE,
    day_of_week VARCHAR(20),
    booking_hour INTEGER,
    channel VARCHAR(100),
    total_bookings BIGINT,
    load_timestamp TIMESTAMP
);