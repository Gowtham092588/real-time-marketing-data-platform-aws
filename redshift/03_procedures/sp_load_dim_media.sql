CREATE OR REPLACE PROCEDURE gold.sp_load_dim_media()
AS $$
BEGIN

    MERGE INTO gold.dim_media
    USING
    (
        SELECT DISTINCT
            media_id,
            media_name AS title,
            created_at
        FROM gold.wistia_media_performance
        WHERE media_id IS NOT NULL
    ) AS source

    ON gold.dim_media.media_id = source.media_id

    WHEN MATCHED THEN
    UPDATE SET
        title      = source.title,
        created_at = source.created_at

    WHEN NOT MATCHED THEN
    INSERT
    (
        media_id,
        title,
        created_at
    )
    VALUES
    (
        source.media_id,
        source.title,
        source.created_at
    );

END;
$$
LANGUAGE plpgsql; 