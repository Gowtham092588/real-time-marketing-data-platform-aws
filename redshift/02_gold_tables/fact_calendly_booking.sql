CREATE TABLE IF NOT EXISTS gold.fact_calendly_booking
(
    booking_key               BIGINT IDENTITY(1,1) NOT NULL,
    booking_id                VARCHAR(255) NOT NULL,
    scheduled_event_id        VARCHAR(255),

    lead_key                  BIGINT,
    date_key                  INT,
    channel_key               BIGINT,
    employee_key              BIGINT,

    event_type_id             VARCHAR(255),
    event_name                VARCHAR(500),

    start_time                TIMESTAMP,
    end_time                  TIMESTAMP,

    meeting_duration_minutes  INT,

    status                    VARCHAR(100),

    is_rescheduled            BOOLEAN,
    is_cancelled              BOOLEAN,

    PRIMARY KEY (booking_key)
);