CREATE OR REPLACE PROCEDURE gold.sp_load_fact_wistia_video_engagement()
AS $$
BEGIN

    MERGE INTO gold.fact_wistia_video_engagement

    USING
    (
        SELECT
            w.event_key,

            dv.visitor_key,

            dm.media_key,

            dd.date_key,

            w.event_timestamp,

            w.visitor_play_count AS play_count,

            w.visitor_load_count AS load_count,

            w.percent_viewed_pct AS engagement_percent

        FROM gold.wistia_visitor_engagement w

        LEFT JOIN gold.dim_visitor dv
            ON w.visitor_key = dv.visitor_id

        LEFT JOIN gold.dim_media dm
            ON w.media_id = dm.media_id

        JOIN gold.dim_date dd
            ON CAST(w.event_timestamp AS DATE)
               = dd.full_date

        WHERE w.event_key IS NOT NULL

    ) AS source

    ON gold.fact_wistia_video_engagement.event_key =
       source.event_key
    AND gold.fact_wistia_video_engagement.visitor_key =
        source.visitor_key

    WHEN MATCHED THEN
    UPDATE SET

        visitor_key =
            source.visitor_key,

        media_key =
            source.media_key,

        date_key =
            source.date_key,

        event_timestamp =
            source.event_timestamp,

        play_count =
            source.play_count,

        load_count =
            source.load_count,

        engagement_percent =
            source.engagement_percent

    WHEN NOT MATCHED THEN

    INSERT
    (
        event_key,
        visitor_key,
        media_key,
        date_key,
        event_timestamp,
        play_count,
        load_count,
        engagement_percent
    )

    VALUES
    (
        source.event_key,
        source.visitor_key,
        source.media_key,
        source.date_key,
        source.event_timestamp,
        source.play_count,
        source.load_count,
        source.engagement_percent
    );

END;
$$
LANGUAGE plpgsql;