CREATE OR REPLACE PROCEDURE gold.sp_load_fact_marketing_spend()
AS $$
BEGIN

    MERGE INTO gold.fact_marketing_spend
    USING
    (
        SELECT
            dd.date_key,
            dc.channel_key,
            cp.total_spend AS spend_amount

        FROM gold.channel_performance cp

        JOIN gold.dim_date dd
            ON cp.report_date = dd.full_date

        JOIN gold.dim_channel dc
            ON cp.channel = dc.channel_name

    ) AS source

    ON gold.fact_marketing_spend.date_key =
       source.date_key

    AND gold.fact_marketing_spend.channel_key =
        source.channel_key

    WHEN MATCHED THEN
    UPDATE SET
        spend_amount = source.spend_amount

    WHEN NOT MATCHED THEN
    INSERT
    (
        date_key,
        channel_key,
        spend_amount
    )
    VALUES
    (
        source.date_key,
        source.channel_key,
        source.spend_amount
    );

END;
$$
LANGUAGE plpgsql;