CREATE OR REPLACE PROCEDURE gold.sp_load_dim_lead()
AS $$
DECLARE
    v_load_ts TIMESTAMP;

BEGIN

    v_load_ts := GETDATE();

    DROP TABLE IF EXISTS changed_leads;

    CREATE TEMP TABLE changed_leads AS
    SELECT
        s.lead_id,
        s.display_name,
        s.lead_email
    FROM gold.crm_leads s
    JOIN gold.dim_lead d
        ON s.lead_id = d.lead_id
       AND d.is_current = TRUE
    WHERE
           COALESCE(s.display_name, '') <> COALESCE(d.display_name, '')
        OR COALESCE(s.lead_email, '') <> COALESCE(d.lead_email, '');


    -- Expire previous version
    UPDATE gold.dim_lead
    SET
        effective_end_date = v_load_ts,
        is_current = FALSE
    FROM changed_leads c
    WHERE gold.dim_lead.lead_id = c.lead_id
      AND gold.dim_lead.is_current = TRUE;


    -- Insert new version for changed leads
    INSERT INTO gold.dim_lead
    (
        lead_id,
        display_name,
        lead_email,
        effective_start_date,
        effective_end_date,
        is_current
    )
    SELECT
        lead_id,
        display_name,
        lead_email,
        v_load_ts,
        NULL,
        TRUE
    FROM changed_leads;


    -- Insert completely new leads
    INSERT INTO gold.dim_lead
    (
        lead_id,
        display_name,
        lead_email,
        effective_start_date,
        effective_end_date,
        is_current
    )
    SELECT
        s.lead_id,
        s.display_name,
        s.lead_email,
        v_load_ts,
        NULL,
        TRUE
    FROM gold.crm_leads s
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM gold.dim_lead d
        WHERE d.lead_id = s.lead_id
    );

END;
$$
LANGUAGE plpgsql;