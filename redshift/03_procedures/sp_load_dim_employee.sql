CREATE OR REPLACE PROCEDURE gold.sp_load_dim_employee()
AS $$
BEGIN

    MERGE INTO gold.dim_employee
    USING
    (
        SELECT DISTINCT
            employee_id,
            employee_name,
            employee_email
        FROM gold.employee_meeting_load
        WHERE employee_id IS NOT NULL
    ) AS source

    ON gold.dim_employee.employee_id = source.employee_id

    WHEN MATCHED THEN
    UPDATE SET
        employee_name  = source.employee_name,
        employee_email = source.employee_email

    WHEN NOT MATCHED THEN
    INSERT
    (
        employee_id,
        employee_name,
        employee_email
    )
    VALUES
    (
        source.employee_id,
        source.employee_name,
        source.employee_email
    );

END;
$$
LANGUAGE plpgsql;