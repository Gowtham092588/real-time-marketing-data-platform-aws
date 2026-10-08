CREATE OR REPLACE PROCEDURE gold.sp_load_dim_owner()
AS $$
BEGIN

    INSERT INTO gold.dim_owner
    (
        lead_owner
    )

    SELECT DISTINCT
        TRIM(c.lead_owner)

    FROM gold.crm_leads c

    WHERE c.lead_owner IS NOT NULL
      AND TRIM(c.lead_owner) <> ''

      AND NOT EXISTS
      (
          SELECT 1
          FROM gold.dim_owner d
          WHERE d.lead_owner = TRIM(c.lead_owner)
      );

END;
$$
LANGUAGE plpgsql;