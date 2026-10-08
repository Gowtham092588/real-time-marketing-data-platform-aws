CREATE OR REPLACE PROCEDURE gold.sp_load_fact_calendly_booking()
AS $$
BEGIN

    MERGE INTO gold.fact_calendly_booking

    USING
    (
        WITH lead_lookup AS
        (
            SELECT
                lead_key,
                lead_id,
                lead_email
            FROM
            (
                SELECT
                    dl.lead_key,
                    dl.lead_id,
                    dl.lead_email,

                    ROW_NUMBER() OVER
                    (
                        PARTITION BY LOWER(TRIM(dl.lead_email))
                        ORDER BY c.date_created DESC,
                                 dl.lead_key DESC
                    ) AS rn

                FROM gold.dim_lead dl

                JOIN gold.crm_leads c
                    ON dl.lead_id = c.lead_id

                WHERE dl.is_current = TRUE
                  AND dl.lead_email IS NOT NULL
            ) x

            WHERE rn = 1
        )

        SELECT
            b.invitee_id AS booking_id,

            b.scheduled_event_id,

            dl.lead_key,

            dd.date_key,

            dc.channel_key,

            de.employee_key,

            b.event_type_id,

            b.event_name,

            b.event_start_time AS start_time,

            b.event_end_time AS end_time,

            DATEDIFF(
                minute,
                b.event_start_time,
                b.event_end_time
            ) AS meeting_duration_minutes,

            b.event_status AS status,

            COALESCE(
                b.rescheduled,
                FALSE
            ) AS is_rescheduled,

            CASE
                WHEN LOWER(
                    COALESCE(b.event_status, '')
                ) IN ('canceled', 'cancelled')

                OR LOWER(
                    COALESCE(b.invitee_status, '')
                ) IN ('canceled', 'cancelled')

                THEN TRUE
                ELSE FALSE
            END AS is_cancelled

        FROM gold.calendly_bookings_detail b

        LEFT JOIN lead_lookup dl
            ON LOWER(TRIM(b.invitee_email))
               = LOWER(TRIM(dl.lead_email))

        JOIN gold.dim_date dd
            ON CAST(b.event_start_time AS DATE)
               = dd.full_date

        LEFT JOIN gold.dim_channel dc
            ON b.marketing_channel
               = dc.channel_name

        LEFT JOIN gold.calendly_employee_meetings_detail em
            ON b.scheduled_event_id
               = em.scheduled_event_id

        LEFT JOIN gold.dim_employee de
            ON em.employee_id
               = de.employee_id

        WHERE b.invitee_id IS NOT NULL

    ) AS source

    ON gold.fact_calendly_booking.booking_id =
       source.booking_id

    WHEN MATCHED THEN
    UPDATE SET

        scheduled_event_id =
            source.scheduled_event_id,

        lead_key =
            source.lead_key,

        date_key =
            source.date_key,

        channel_key =
            source.channel_key,

        employee_key =
            source.employee_key,

        event_type_id =
            source.event_type_id,

        event_name =
            source.event_name,

        start_time =
            source.start_time,

        end_time =
            source.end_time,

        meeting_duration_minutes =
            source.meeting_duration_minutes,

        status =
            source.status,

        is_rescheduled =
            source.is_rescheduled,

        is_cancelled =
            source.is_cancelled

    WHEN NOT MATCHED THEN

    INSERT
    (
        booking_id,
        scheduled_event_id,
        lead_key,
        date_key,
        channel_key,
        employee_key,
        event_type_id,
        event_name,
        start_time,
        end_time,
        meeting_duration_minutes,
        status,
        is_rescheduled,
        is_cancelled
    )

    VALUES
    (
        source.booking_id,
        source.scheduled_event_id,
        source.lead_key,
        source.date_key,
        source.channel_key,
        source.employee_key,
        source.event_type_id,
        source.event_name,
        source.start_time,
        source.end_time,
        source.meeting_duration_minutes,
        source.status,
        source.is_rescheduled,
        source.is_cancelled
    );

END;
$$
LANGUAGE plpgsql;
