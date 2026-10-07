import streamlit as st
import plotly.express as px

from db import (
    get_daily_bookings,
    get_channel_performance,
    get_booking_time_analysis,
    get_employee_meeting_load,
    get_calendly_crm_analysis,
    get_wistia_engagement,
    get_wistia_visitors,
    get_cross_platform_analysis
)


# ============================================================
# PAGE SETUP
# ============================================================

st.set_page_config(
    page_title="Marketing Analytics Dashboard",
    layout="wide"
)

st.title("Marketing Analytics Dashboard")
st.caption("CRM | Calendly | Wistia")


# ============================================================
# NAVIGATION
# ============================================================

page = st.sidebar.radio(
    "Select Analysis",
    [
        "Calendly Analysis",
        "Lead Analysis",
        "Wistia Analysis",
        "Cross-Platform Summary"
    ]
)


# ============================================================
# 1. CALENDLY ANALYSIS
# ============================================================

if page == "Calendly Analysis":

    st.header("Calendly Analysis")

    daily_df = get_daily_bookings()
    channel_df = get_channel_performance()
    time_df = get_booking_time_analysis()
    employee_df = get_employee_meeting_load()

    # --------------------------------------------------------
    # COST PER BOOKING
    # --------------------------------------------------------

    st.subheader("Cost Per Booking by Channel")

    total_bookings = daily_df["total_bookings"].sum()
    total_spend = channel_df["total_spend"].sum()

    paid_df = channel_df[
        channel_df["total_bookings"] > 0
    ].copy()

    paid_bookings = paid_df["total_bookings"].sum()
    paid_spend = paid_df["total_spend"].sum()

    average_cpb = (
        paid_spend / paid_bookings
        if paid_bookings > 0
        else 0
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Bookings",
        f"{int(total_bookings):,}"
    )

    col2.metric(
        "Total Spend",
        f"${total_spend:,.2f}"
    )

    col3.metric(
        "Average Cost Per Booking",
        f"${average_cpb:,.2f}"
    )

    channel_summary = (
        paid_df
        .groupby(
            "channel",
            as_index=False
        )
        .agg(
            total_bookings=("total_bookings", "sum"),
            total_spend=("total_spend", "sum")
        )
    )

    channel_summary["cost_per_booking"] = (
        channel_summary["total_spend"]
        / channel_summary["total_bookings"]
    )

    cpb_chart = px.bar(
        channel_summary,
        x="channel",
        y="cost_per_booking",
        labels={
            "channel": "Channel",
            "cost_per_booking": "Cost Per Booking"
        }
    )

    st.plotly_chart(
        cpb_chart,
        use_container_width=True
    )

    st.dataframe(
        channel_summary,
        use_container_width=True
    )

    # --------------------------------------------------------
    # DAILY BOOKINGS
    # --------------------------------------------------------

    st.subheader("Daily Calls Booked by Source")

    booking_chart = px.line(
        daily_df,
        x="booking_date",
        y="total_bookings",
        color="source",
        markers=True,
        labels={
            "booking_date": "Date",
            "total_bookings": "Bookings",
            "source": "Source"
        }
    )

    st.plotly_chart(
        booking_chart,
        use_container_width=True
    )

    # --------------------------------------------------------
    # CHANNEL ATTRIBUTION
    # --------------------------------------------------------

    st.subheader("Channel Attribution")

    channel_volume = (
        channel_summary
        .sort_values(
            "total_bookings",
            ascending=False
        )
    )

    channel_chart = px.bar(
        channel_volume,
        x="channel",
        y="total_bookings",
        labels={
            "channel": "Channel",
            "total_bookings": "Bookings"
        }
    )

    st.plotly_chart(
        channel_chart,
        use_container_width=True
    )

    # --------------------------------------------------------
    # BOOKING TIME ANALYSIS
    # --------------------------------------------------------

    st.subheader(
        "Booking Volume by Time Slot and Day of Week"
    )

    heatmap_df = (
        time_df
        .pivot(
            index="day_of_week",
            columns="booking_hour",
            values="total_bookings"
        )
        .fillna(0)
    )

    day_order = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday"
    ]

    heatmap_df = heatmap_df.reindex(day_order)

    heatmap = px.imshow(
        heatmap_df,
        labels={
            "x": "Hour",
            "y": "Day",
            "color": "Bookings"
        },
        aspect="auto"
    )

    st.plotly_chart(
        heatmap,
        use_container_width=True
    )

    hourly_df = (
        time_df
        .groupby(
            "booking_hour",
            as_index=False
        )["total_bookings"]
        .sum()
    )

    hourly_chart = px.bar(
        hourly_df,
        x="booking_hour",
        y="total_bookings",
        labels={
            "booking_hour": "Hour",
            "total_bookings": "Bookings"
        }
    )

    st.plotly_chart(
        hourly_chart,
        use_container_width=True
    )

    # --------------------------------------------------------
    # EMPLOYEE MEETING LOAD
    # --------------------------------------------------------

    st.subheader("Employee Meeting Load")

    total_meetings = employee_df["total_meetings"].sum()
    max_meetings = employee_df["total_meetings"].max()
    min_meetings = employee_df["total_meetings"].min()

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Meetings",
        f"{int(total_meetings):,}"
    )

    col2.metric(
        "Max Meetings per Week",
        f"{int(max_meetings):,}"
    )

    col3.metric(
        "Min Meetings per Week",
        f"{int(min_meetings):,}"
    )

    employee_avg = (
        employee_df
        .groupby(
            [
                "employee_id",
                "employee_name"
            ],
            as_index=False
        )
        .agg(
            total_meetings=("total_meetings", "sum"),
            weeks=("week_start_date", "nunique")
        )
    )

    employee_avg["avg_meetings_per_week"] = (
        employee_avg["total_meetings"]
        / employee_avg["weeks"]
    )

    employee_chart = px.bar(
        employee_avg,
        x="employee_name",
        y="avg_meetings_per_week",
        labels={
            "employee_name": "Employee",
            "avg_meetings_per_week":
                "Average Meetings per Week"
        }
    )

    st.plotly_chart(
        employee_chart,
        use_container_width=True
    )


# ============================================================
# 2. LEAD ANALYSIS
# ============================================================

elif page == "Lead Analysis":

    st.header("Calendly to CRM Lead Analysis")

    daily_df = get_daily_bookings()
    crm_df = get_calendly_crm_analysis()

    total_bookings = (
        daily_df["total_bookings"]
        .sum()
    )

    matched_bookings = (
        crm_df["booking_id"]
        .nunique()
    )

    match_rate = (
        matched_bookings
        / total_bookings
        * 100
        if total_bookings > 0
        else 0
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Bookings",
        f"{int(total_bookings):,}"
    )

    col2.metric(
        "CRM-Matched Bookings",
        f"{matched_bookings:,}"
    )

    col3.metric(
        "CRM Match Rate",
        f"{match_rate:.2f}%"
    )

    # --------------------------------------------------------
    # MATCHED BOOKINGS BY CHANNEL
    # --------------------------------------------------------

    st.subheader(
        "CRM-Matched Bookings by Channel"
    )

    channel_df = (
        crm_df
        .groupby(
            "channel",
            as_index=False
        )
        .agg(
            matched_bookings=(
                "booking_id",
                "nunique"
            )
        )
    )

    channel_chart = px.bar(
        channel_df,
        x="channel",
        y="matched_bookings",
        labels={
            "channel": "Channel",
            "matched_bookings":
                "CRM-Matched Bookings"
        }
    )

    st.plotly_chart(
        channel_chart,
        use_container_width=True
    )

    # --------------------------------------------------------
    # CRM FUNNEL
    # --------------------------------------------------------

    st.subheader("CRM Funnel")

    funnel_df = (
        crm_df
        .groupby(
            "funnel",
            as_index=False
        )
        .agg(
            matched_bookings=(
                "booking_id",
                "nunique"
            )
        )
    )

    funnel_chart = px.bar(
        funnel_df,
        x="funnel",
        y="matched_bookings",
        labels={
            "funnel": "Funnel",
            "matched_bookings":
                "Matched Bookings"
        }
    )

    st.plotly_chart(
        funnel_chart,
        use_container_width=True
    )


# ============================================================
# 3. WISTIA ANALYSIS
# ============================================================

elif page == "Wistia Analysis":

    st.header("Wistia Video & Visitor Analysis")

    wistia_df = get_wistia_engagement()
    visitor_df = get_wistia_visitors()

    unique_visitors = (
        wistia_df["visitor_key"]
        .nunique()
    )

    total_plays = (
        wistia_df["play_count"]
        .fillna(0)
        .sum()
    )

    average_engagement = (
        wistia_df["engagement_percent"]
        .mean()
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Unique Visitors",
        f"{unique_visitors:,}"
    )

    col2.metric(
        "Total Plays",
        f"{int(total_plays):,}"
    )

    col3.metric(
        "Average Engagement",
        f"{average_engagement:.2f}%"
    )

    # --------------------------------------------------------
    # VIDEO PERFORMANCE
    # --------------------------------------------------------

    st.subheader("Video Performance")

    video_df = (
        wistia_df
        .groupby(
            [
                "media_id",
                "media_name"
            ],
            as_index=False
        )
        .agg(
            unique_visitors=(
                "visitor_key",
                "nunique"
            ),
            total_plays=(
                "play_count",
                "sum"
            ),
            average_engagement=(
                "engagement_percent",
                "mean"
            )
        )
    )

    video_chart = px.bar(
        video_df,
        x="media_name",
        y="average_engagement",
        labels={
            "media_name": "Video",
            "average_engagement":
                "Average Engagement (%)"
        }
    )

    st.plotly_chart(
        video_chart,
        use_container_width=True
    )

    st.dataframe(
        video_df,
        use_container_width=True
    )

    # --------------------------------------------------------
    # TOP VISITORS
    # --------------------------------------------------------

    st.subheader("Top Engaged Visitors")

    top_visitors = (
        visitor_df
        .sort_values(
            "engagement_events",
            ascending=False
        )
        .head(10)
    )

    visitor_chart = px.bar(
        top_visitors,
        x="visitor_id",
        y="engagement_events",
        labels={
            "visitor_id": "Visitor",
            "engagement_events":
                "Engagement Events"
        }
    )

    st.plotly_chart(
        visitor_chart,
        use_container_width=True
    )


# ============================================================
# 4. CROSS-PLATFORM SUMMARY
# ============================================================

elif page == "Cross-Platform Summary":

    st.header("Cross-Platform Summary")

    st.write(
        "Summary of marketing spend, bookings, "
        "CRM leads and Wistia engagement."
    )

    cross_df = get_cross_platform_analysis()

    # --------------------------------------------------------
    # SUMMARY METRICS
    # --------------------------------------------------------

    total_spend = (
        cross_df["total_spend"]
        .sum()
    )

    total_bookings = (
        cross_df["total_bookings"]
        .sum()
    )

    total_leads = (
        cross_df["new_crm_leads"]
        .sum()
    )

    wistia_events = (
        cross_df[
            "wistia_engagement_events"
        ]
        .sum()
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Spend",
        f"${total_spend:,.2f}"
    )

    col2.metric(
        "Total Bookings",
        f"{int(total_bookings):,}"
    )

    col3.metric(
        "New CRM Leads",
        f"{int(total_leads):,}"
    )

    col4.metric(
        "Wistia Engagement",
        f"{int(wistia_events):,}"
    )

    # --------------------------------------------------------
    # MARKETING SPEND TREND
    # --------------------------------------------------------

    st.subheader("Marketing Spend Trend")

    spend_chart = px.line(
        cross_df,
        x="report_date",
        y="total_spend",
        markers=True,
        labels={
            "report_date": "Date",
            "total_spend": "Spend"
        }
    )

    st.plotly_chart(
        spend_chart,
        use_container_width=True
    )

    # --------------------------------------------------------
    # BOOKINGS / LEADS / WISTIA TREND
    # --------------------------------------------------------

    st.subheader(
        "Bookings, CRM Leads and Wistia Activity"
    )

    trend_df = (
        cross_df[
            [
                "report_date",
                "total_bookings",
                "new_crm_leads",
                "wistia_engagement_events"
            ]
        ]
        .melt(
            id_vars="report_date",
            var_name="metric",
            value_name="count"
        )
    )

    trend_chart = px.line(
        trend_df,
        x="report_date",
        y="count",
        color="metric",
        markers=True,
        labels={
            "report_date": "Date",
            "count": "Count",
            "metric": "Metric"
        }
    )

    st.plotly_chart(
        trend_chart,
        use_container_width=True
    )
