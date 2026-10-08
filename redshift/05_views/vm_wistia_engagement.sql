CREATE OR REPLACE VIEW gold.vm_wistia_engagement AS
 SELECT
            f.event_key,

            d.full_date AS engagement_date,

            f.visitor_key,

            m.media_id,
            m.title AS media_name,

            f.event_timestamp,

            f.play_count,
            f.load_count,
            f.engagement_percent

        FROM gold.fact_wistia_video_engagement f

        JOIN gold.dim_date d
            ON f.date_key = d.date_key

        LEFT JOIN gold.dim_media m
            ON f.media_key = m.media_key

        ORDER BY
            f.event_timestamp;
