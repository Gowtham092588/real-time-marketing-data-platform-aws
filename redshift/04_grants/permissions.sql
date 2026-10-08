GRANT USAGE ON SCHEMA staging TO crm_glue_user;

GRANT CREATE ON SCHEMA staging TO crm_glue_user;

GRANT SELECT, INSERT, UPDATE, DELETE
ON staging.crm_leads
TO crm_glue_user;

GRANT USAGE ON SCHEMA gold TO crm_glue_user;
GRANT CREATE ON SCHEMA gold TO crm_glue_user;

GRANT SELECT, INSERT, UPDATE, DELETE
ON gold.crm_leads
TO crm_glue_user;

GRANT EXECUTE
ON PROCEDURE gold.sp_load_dim_lead()
TO crm_glue_user;

GRANT USAGE ON SCHEMA gold TO crm_glue_user;

GRANT SELECT ON TABLE gold.crm_leads
TO crm_glue_user;

GRANT SELECT, INSERT, UPDATE ON TABLE gold.calendly_employee_meetings_detail
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_lead()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_channel()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_employee()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_media()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_owner()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_dim_visitor()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_fact_calendly_booking()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_fact_marketing_spend()
TO crm_glue_user;

GRANT EXECUTE ON PROCEDURE gold.sp_load_fact_crm_lead()
TO crm_glue_user;


GRANT EXECUTE ON PROCEDURE gold.sp_load_fact_wistia_video_engagement()
TO crm_glue_user;