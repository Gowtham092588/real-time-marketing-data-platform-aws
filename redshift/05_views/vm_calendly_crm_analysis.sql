CREATE OR REPLACE VIEW gold.vm_calendly_crm_analysis AS
    SELECT
            f.booking_id,
            d.full_date AS booking_date,
            c.channel_name AS channel,
            f.event_name,
            f.start_time,
            l.lead_id,
            l.display_name,
            l.lead_email,
            crm.status_label,
            crm.lead_owner,
            crm.funnel
        FROM gold.fact_calendly_booking f
        JOIN gold.dim_date d
            ON f.date_key = d.date_key
        LEFT JOIN gold.dim_channel c
            ON f.channel_key = c.channel_key
        JOIN gold.dim_lead l
            ON f.lead_key = l.lead_key
        JOIN gold.crm_leads crm
            ON l.lead_id = crm.lead_id
        WHERE f.lead_key IS NOT NULL;