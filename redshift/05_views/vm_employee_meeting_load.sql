CREATE OR REPLACE VIEW gold.vm_employee_meeting_load AS
SELECT
            e.employee_id,
            e.employee_name,

            DATE_TRUNC(
                'week',
                f.start_time
            ) AS week_start_date,

            COUNT(
                DISTINCT f.booking_id
            ) AS total_meetings

        FROM gold.fact_calendly_booking f

        JOIN gold.dim_employee e
            ON f.employee_key = e.employee_key

        WHERE f.employee_key IS NOT NULL

        GROUP BY
            e.employee_id,
            e.employee_name,
            DATE_TRUNC(
                'week',
                f.start_time
            )

        ORDER BY
            week_start_date,
            employee_name;