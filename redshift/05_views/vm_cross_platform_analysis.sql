CREATE OR REPLACE VIEW gold.vm_cross_platform_analysis AS
WITH calendly_daily AS
        (
            SELECT
                d.full_date AS report_date,
                COUNT(DISTINCT f.booking_id) AS total_bookings
            FROM gold.fact_calendly_booking f
            JOIN gold.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                d.full_date
        ),
        crm_daily AS
        (
            SELECT
                CAST(date_created AS DATE) AS report_date,
                COUNT(DISTINCT lead_id) AS new_crm_leads
            FROM gold.crm_leads
            WHERE date_created IS NOT NULL
            GROUP BY
                CAST(date_created AS DATE)
        ),
        wistia_daily AS
        (
            SELECT
                d.full_date AS report_date,
                COUNT(
                    DISTINCT f.event_key
                ) AS wistia_engagement_events,
                COUNT(
                    DISTINCT f.visitor_key
                ) AS wistia_visitors
            FROM gold.fact_wistia_video_engagement f
            JOIN gold.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                d.full_date
        ),
        spend_daily AS
        (
            SELECT
                report_date,
                SUM(total_spend) AS total_spend
            FROM gold.channel_performance
            GROUP BY
                report_date
        ),
        all_dates AS
        (
            SELECT report_date
            FROM calendly_daily
            UNION
            SELECT report_date
            FROM crm_daily
            UNION
            SELECT report_date
            FROM wistia_daily
            UNION
            SELECT report_date
            FROM spend_daily
        )

        SELECT
            a.report_date,
            COALESCE(s.total_spend, 0) AS total_spend,
            COALESCE(c.total_bookings,0) AS total_bookings,
            COALESCE(
                crm.new_crm_leads,
                0
            ) AS new_crm_leads,

            COALESCE(
                w.wistia_engagement_events,
                0
            ) AS wistia_engagement_events,

            COALESCE(
                w.wistia_visitors,
                0
            ) AS wistia_visitors

        FROM all_dates a

        LEFT JOIN spend_daily s
            ON a.report_date = s.report_date

        LEFT JOIN calendly_daily c
            ON a.report_date = c.report_date

        LEFT JOIN crm_daily crm
            ON a.report_date = crm.report_date

        LEFT JOIN wistia_daily w
            ON a.report_date = w.report_date

        ORDER BY
            a.report_date;