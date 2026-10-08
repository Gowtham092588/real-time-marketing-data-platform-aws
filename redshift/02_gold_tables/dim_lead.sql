CREATE TABLE gold.dim_lead
(
    lead_key        BIGINT IDENTITY(1,1) NOT NULL,
    lead_id         VARCHAR(100) NOT NULL,
    display_name    VARCHAR(200),
    lead_email      VARCHAR(255),

    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date   TIMESTAMP,
    is_current           BOOLEAN NOT NULL,

    PRIMARY KEY (lead_key)
);