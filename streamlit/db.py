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
        SELECT *
        FROM gold.vw_daily_bookings
        ORDER BY booking_date, source;
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
         SELECT *
        FROM gold.vw_employee_meeting_load
        ORDER BY week_start_date, employee_name;
    """
    return run_query(query)


def get_calendly_crm_analysis():

    query = """
        SELECT *
        FROM gold.vw_calendly_crm_analysis;
    """

    return run_query(query)


def get_wistia_engagement():

    query = """
        SELECT *
        FROM gold.vw_wistia_engagement
        ORDER BY event_timestamp;
    """

    return run_query(query)


def get_wistia_visitors():

    query = """
        SELECT *
        FROM gold.vw_wistia_visitors
        ORDER BY engagement_events DESC;
    """

    return run_query(query)


def get_cross_platform_analysis():

    query = """
        SELECT *
        FROM gold.vw_cross_platform_analysis
        ORDER BY report_date;
    """

    return run_query(query)
