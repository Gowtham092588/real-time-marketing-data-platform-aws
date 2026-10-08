CREATE TABLE IF NOT EXISTS staging.employee_meeting_load (
    employee_id VARCHAR(255),
    employee_name VARCHAR(255),
    employee_email VARCHAR(255),
    week_start_date DATE,
    total_meetings BIGINT,
    avg_meetings_per_week DECIMAL(10,2),
    load_timestamp TIMESTAMP
);