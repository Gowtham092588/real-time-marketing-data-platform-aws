CREATE OR REPLACE PROCEDURE gold.sp_load_fact_crm_lead()
AS $$
BEGIN

    MERGE INTO gold.fact_crm_lead
    USING
    (
        SELECT
            dl.lead_key,
            dd.date_key AS created_date_key,
            dm.owner_key,
            c.status_label,
            c.funnel

        FROM gold.crm_leads c

        JOIN gold.dim_lead dl
            ON c.lead_id = dl.lead_id
           AND dl.is_current = TRUE

        JOIN gold.dim_date dd
            ON CAST(c.date_created AS DATE) = dd.full_date

        LEFT JOIN gold.dim_owner dm
            ON TRIM(c.lead_owner) = dm.lead_owner

    ) AS source

    ON gold.fact_crm_lead.lead_key = source.lead_key

    WHEN MATCHED THEN
    UPDATE SET
        created_date_key = source.created_date_key,
        owner_key        = source.owner_key,
        status_label     = source.status_label,
        funnel           = source.funnel

    WHEN NOT MATCHED THEN
    INSERT
    (
        lead_key,
        created_date_key,
        owner_key,
        status_label,
        funnel
    )
    VALUES
    (
        source.lead_key,
        source.created_date_key,
        source.owner_key,
        source.status_label,
        source.funnel
    );

END;
$$
LANGUAGE plpgsql;