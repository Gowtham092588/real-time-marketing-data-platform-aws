CREATE OR REPLACE PROCEDURE gold.sp_load_dim_channel()
AS $$
BEGIN

    MERGE INTO gold.dim_channel
    USING
    (
        SELECT DISTINCT
            channel AS channel_name
        FROM gold.channel_performance
        WHERE channel IS NOT NULL
    ) AS source
    ON gold.dim_channel.channel_name = source.channel_name

    WHEN NOT MATCHED THEN
    INSERT
    (
        channel_name,
        campaign_name,
        event_type_uri
    )
    VALUES
    (
        source.channel_name,
        NULL,
        NULL
    );

END;
$$
LANGUAGE plpgsql;