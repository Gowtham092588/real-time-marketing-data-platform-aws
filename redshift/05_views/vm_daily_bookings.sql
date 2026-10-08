CREATE OR REPLACE VIEW gold.vw_daily_bookings AS
 SELECT
            d.full_date AS booking_date,
            c.channel_name AS source,
            COUNT(DISTINCT f.booking_id) AS total_bookings

        FROM gold.fact_calendly_booking f

        JOIN gold.dim_date d
            ON f.date_key = d.date_key

        LEFT JOIN gold.dim_channel c
            ON f.channel_key = c.channel_key

        GROUP BY
            d.full_date,
            c.channel_name

        ORDER BY
            d.full_date,
            c.channel_name;