CREATE TABLE IF NOT EXISTS gold.calendly_employee_meetings_detail
(
    scheduled_event_id  VARCHAR(255) NOT NULL,
    employee_id         VARCHAR(255) NOT NULL,

    employee_name       VARCHAR(255),
    employee_email      VARCHAR(255),

    meeting_start_time  TIMESTAMP,
    event_updated_at    TIMESTAMP
);