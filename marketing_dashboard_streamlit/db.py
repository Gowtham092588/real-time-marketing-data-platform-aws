import streamlit as st
import redshift_connector
import pandas as pd


@st.cache_data(ttl=300)
def run_query(query):

    conn = None
    cursor = None

    try:

        conn = redshift_connector.connect(
            host=st.secrets["redshift"]["host"],
            port=st.secrets["redshift"]["port"],
            database=st.secrets["redshift"]["database"],
            user=st.secrets["redshift"]["user"],
            password=st.secrets["redshift"]["password"]
        )

        cursor = conn.cursor()

        cursor.execute(query)

        columns = [
            column[0]
            for column in cursor.description
        ]

        rows = cursor.fetchall()

        return pd.DataFrame(
            rows,
            columns=columns
        )

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()


def get_daily_bookings():

    query = """
        SELECT
            d.full_date AS booking_date,
            c.channel_name AS source,
            COUNT(DISTINCT f.booking_id) AS total_bookings

        FROM gold.fact_calendly_booking f

        JOIN gold.dim_date d
            ON f.date_key = d.date_key

        LEFT JOIN gold.dim_channel c
            ON f.channel_key = c.channel_key

        GROUP BY
            d.full_date,
            c.channel_name

        ORDER BY
            d.full_date,
            c.channel_name;
    """

    return run_query(query)


def get_channel_performance():

    query = """
        SELECT
            report_date,
            channel,
            total_bookings,
            total_spend,
            cost_per_booking

        FROM gold.channel_performance

        ORDER BY
            report_date,
            channel;
    """

    return run_query(query)


def get_booking_time_analysis():

    query = """
        SELECT
            EXTRACT(DOW FROM f.start_time) AS day_number,

            CASE EXTRACT(DOW FROM f.start_time)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS day_of_week,

            EXTRACT(HOUR FROM f.start_time) AS booking_hour,

            COUNT(DISTINCT f.booking_id) AS total_bookings

        FROM gold.fact_calendly_booking f

        GROUP BY
            EXTRACT(DOW FROM f.start_time),

            CASE EXTRACT(DOW FROM f.start_time)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END,

            EXTRACT(HOUR FROM f.start_time)

        ORDER BY
            day_number,
            booking_hour;
    """

    return run_query(query)


def get_employee_meeting_load():

    query = """
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
    """
    return run_query(query)


def get_calendly_crm_analysis():

    query = """
        SELECT
            f.booking_id,
            d.full_date AS booking_date,
            c.channel_name AS channel,
            f.event_name,
            f.start_time,
            l.lead_id,
            l.display_name,
            l.lead_email,
            crm.status_label,
            crm.lead_owner,
            crm.funnel
        FROM gold.fact_calendly_booking f
        JOIN gold.dim_date d
            ON f.date_key = d.date_key
        LEFT JOIN gold.dim_channel c
            ON f.channel_key = c.channel_key
        JOIN gold.dim_lead l
            ON f.lead_key = l.lead_key
        JOIN gold.crm_leads crm
            ON l.lead_id = crm.lead_id
        WHERE f.lead_key IS NOT NULL;
    """

    return run_query(query)


def get_wistia_engagement():

    query = """
        SELECT
            f.event_key,

            d.full_date AS engagement_date,

            f.visitor_key,

            m.media_id,
            m.title AS media_name,

            f.event_timestamp,

            f.play_count,
            f.load_count,
            f.engagement_percent

        FROM gold.fact_wistia_video_engagement f

        JOIN gold.dim_date d
            ON f.date_key = d.date_key

        LEFT JOIN gold.dim_media m
            ON f.media_key = m.media_key

        ORDER BY
            f.event_timestamp;
    """

    return run_query(query)


def get_wistia_visitors():

    query = """
        SELECT
            v.visitor_key,
            v.visitor_id,
            v.email,

            COUNT(DISTINCT f.event_key) AS engagement_events,

            COUNT(DISTINCT f.media_key) AS videos_engaged,

            SUM(f.play_count) AS total_plays,

            AVG(f.engagement_percent) AS avg_engagement_percent

        FROM gold.dim_visitor v

        LEFT JOIN gold.fact_wistia_video_engagement f
            ON v.visitor_key = f.visitor_key

        GROUP BY
            v.visitor_key,
            v.visitor_id,
            v.email

        ORDER BY
            engagement_events DESC;
    """

    return run_query(query)

def get_cross_platform_analysis():

    query = """
        WITH calendly_daily AS
        (
            SELECT
                d.full_date AS report_date,
                COUNT(DISTINCT f.booking_id) AS total_bookings
            FROM gold.fact_calendly_booking f
            JOIN gold.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                d.full_date
        ),
        crm_daily AS
        (
            SELECT
                CAST(date_created AS DATE) AS report_date,
                COUNT(DISTINCT lead_id) AS new_crm_leads
            FROM gold.crm_leads
            WHERE date_created IS NOT NULL
            GROUP BY
                CAST(date_created AS DATE)
        ),
        wistia_daily AS
        (
            SELECT
                d.full_date AS report_date,
                COUNT(
                    DISTINCT f.event_key
                ) AS wistia_engagement_events,
                COUNT(
                    DISTINCT f.visitor_key
                ) AS wistia_visitors
            FROM gold.fact_wistia_video_engagement f
            JOIN gold.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                d.full_date
        ),
        spend_daily AS
        (
            SELECT
                report_date,
                SUM(total_spend) AS total_spend
            FROM gold.channel_performance
            GROUP BY
                report_date
        ),
        all_dates AS
        (
            SELECT report_date
            FROM calendly_daily
            UNION
            SELECT report_date
            FROM crm_daily
            UNION
            SELECT report_date
            FROM wistia_daily
            UNION
            SELECT report_date
            FROM spend_daily
        )

        SELECT
            a.report_date,
            COALESCE(s.total_spend, 0) AS total_spend,
            COALESCE(c.total_bookings,0) AS total_bookings,
            COALESCE(
                crm.new_crm_leads,
                0
            ) AS new_crm_leads,

            COALESCE(
                w.wistia_engagement_events,
                0
            ) AS wistia_engagement_events,

            COALESCE(
                w.wistia_visitors,
                0
            ) AS wistia_visitors

        FROM all_dates a

        LEFT JOIN spend_daily s
            ON a.report_date = s.report_date

        LEFT JOIN calendly_daily c
            ON a.report_date = c.report_date

        LEFT JOIN crm_daily crm
            ON a.report_date = crm.report_date

        LEFT JOIN wistia_daily w
            ON a.report_date = w.report_date

        ORDER BY
            a.report_date;
    """

    return run_query(query)
