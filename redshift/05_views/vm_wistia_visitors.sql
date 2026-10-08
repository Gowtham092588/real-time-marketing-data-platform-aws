CREATE OR REPLACE VIEW gold.vm_wistia_visitors AS 
SELECT
            v.visitor_key,
            v.visitor_id,
            v.email,

            COUNT(DISTINCT f.event_key) AS engagement_events,

            COUNT(DISTINCT f.media_key) AS videos_engaged,

            SUM(f.play_count) AS total_plays,

            AVG(f.engagement_percent) AS avg_engagement_percent

        FROM gold.dim_visitor v

        LEFT JOIN gold.fact_wistia_video_engagement f
            ON v.visitor_key = f.visitor_key

        GROUP BY
            v.visitor_key,
            v.visitor_id,
            v.email

        ORDER BY
            engagement_events DESC;