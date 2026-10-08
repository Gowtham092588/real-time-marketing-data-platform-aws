CREATE TABLE IF NOT EXISTS gold.fact_crm_lead
(
    crm_lead_key      BIGINT IDENTITY(1,1) NOT NULL,
    lead_key          BIGINT NOT NULL,
    created_date_key  INT NOT NULL,
    owner_key         BIGINT,
    status_label      VARCHAR(100),
    funnel            VARCHAR(100),

    PRIMARY KEY (crm_lead_key)
);