CREATE TABLE IF NOT EXISTS staging.crm_leads
(
    lead_id        VARCHAR(100),
    display_name   VARCHAR(255),
    lead_email     VARCHAR(255),
    status_label   VARCHAR(100),
    lead_owner     VARCHAR(255),
    funnel         VARCHAR(500),
    date_created   TIMESTAMP
);