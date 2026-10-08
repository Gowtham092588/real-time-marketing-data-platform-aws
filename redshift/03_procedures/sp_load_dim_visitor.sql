CREATE OR REPLACE PROCEDURE gold.sp_load_dim_visitor()
AS $$
BEGIN

    MERGE INTO gold.dim_visitor
    USING
    (
        SELECT
            visitor_key AS visitor_id,
            MAX(visitor_email) AS email
        FROM gold.wistia_visitor_engagement
        WHERE visitor_key IS NOT NULL
        GROUP BY visitor_key
    ) AS source

    ON gold.dim_visitor.visitor_id = source.visitor_id

    WHEN MATCHED THEN
    UPDATE SET
        email = source.email

    WHEN NOT MATCHED THEN
    INSERT
    (
        visitor_id,
        email,
        ip_address
    )
    VALUES
    (
        source.visitor_id,
        source.email,
        NULL
    );

END;
$$
LANGUAGE plpgsql; 
