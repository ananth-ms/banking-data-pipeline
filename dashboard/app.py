import streamlit as st
import pandas as pd
import plotly.express as px
import time

from db import run_query


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Banking Fraud Dashboard",
    page_icon="🏦",
    layout="wide"
)


# ============================================================
# TITLE
# ============================================================

st.title("🏦 Banking Fraud Detection Dashboard")

st.caption(
    "Developed by Ananth M. | •  Python • RabbitMQ • HDFS • PySpark • MySQL • Streamlit"
)


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.header("Dashboard Filters")

# ------------------------------------------------------------
# Get available customers
# ------------------------------------------------------------

customers_df = run_query("""
    SELECT DISTINCT customer_id
    FROM fraud_alerts
    ORDER BY customer_id
""")

customers = ["All"] + customers_df["customer_id"].tolist()


selected_customer = st.sidebar.selectbox(
    "Customer",
    customers
)


# ------------------------------------------------------------
# Get fraud rules
# ------------------------------------------------------------

rules_df = run_query("""
    SELECT DISTINCT fraud_rule
    FROM fraud_alerts
    ORDER BY fraud_rule
""")

rules = ["All"] + rules_df["fraud_rule"].tolist()


selected_rule = st.sidebar.selectbox(
    "Fraud Rule",
    rules
)


# ------------------------------------------------------------
# Date filter
# ------------------------------------------------------------

date_df = run_query("""
    SELECT
        MIN(DATE(transaction_timestamp)) AS min_date,
        MAX(DATE(transaction_timestamp)) AS max_date
    FROM fraud_alerts
""")

min_date = pd.to_datetime(date_df.iloc[0]["min_date"]).date()
max_date = pd.to_datetime(date_df.iloc[0]["max_date"]).date()


selected_dates = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)


# ============================================================
# BUILD FILTER CONDITIONS
# ============================================================

conditions = []


if selected_customer != "All":
    conditions.append(
        f"customer_id = '{selected_customer}'"
    )


if selected_rule != "All":
    conditions.append(
        f"fraud_rule = '{selected_rule}'"
    )


if len(selected_dates) == 2:

    start_date = selected_dates[0]
    end_date = selected_dates[1]

    conditions.append(
        f"DATE(transaction_timestamp) "
        f"BETWEEN '{start_date}' AND '{end_date}'"
    )


where_clause = ""

if conditions:
    where_clause = "WHERE " + " AND ".join(conditions)


# ============================================================
# KPI DATA
# ============================================================

transaction_count = run_query("""
    SELECT COUNT(*) AS total
    FROM transactions
""").iloc[0]["total"]


successful_transactions = run_query("""
    SELECT COUNT(*) AS total
    FROM transactions
    WHERE status = 'SUCCESS'
""").iloc[0]["total"]


total_transaction_amount = run_query("""
    SELECT COALESCE(SUM(amount), 0) AS total
    FROM transactions
""").iloc[0]["total"]


fraud_query = f"""
    SELECT
        COUNT(*) AS total_alerts,
        COALESCE(SUM(amount), 0) AS total_amount,
        COUNT(DISTINCT customer_id) AS customers
    FROM fraud_alerts
    {where_clause}
"""

fraud_kpi = run_query(fraud_query).iloc[0]

fraud_count = fraud_kpi["total_alerts"]
fraud_amount = fraud_kpi["total_amount"]
fraud_customers = fraud_kpi["customers"]


# ============================================================
# KPI CARDS
# ============================================================

col1, col2, col3, col4, col5 = st.columns(5)


col1.metric(
    "Total Transactions",
    f"{transaction_count:,}"
)


col2.metric(
    "Successful Transactions",
    f"{successful_transactions:,}"
)


col3.metric(
    "Transaction Amount",
    f"₹{total_transaction_amount:,.0f}"
)


col4.metric(
    "Fraud Alerts",
    f"{fraud_count:,}"
)


col5.metric(
    "Fraud Amount",
    f"₹{fraud_amount:,.0f}"
)


st.divider()


# ============================================================
# FRAUD RULE ANALYSIS
# ============================================================

st.subheader("🚨 Fraud Rule Analysis")


rule_df = run_query(f"""
    SELECT
        fraud_rule,
        COUNT(*) AS alert_count,
        COALESCE(SUM(amount), 0) AS total_amount
    FROM fraud_alerts
    {where_clause}
    GROUP BY fraud_rule
    ORDER BY alert_count DESC
""")


col1, col2 = st.columns(2)


with col1:

    fig = px.bar(
        rule_df,
        x="fraud_rule",
        y="alert_count",
        text="alert_count",
        title="Fraud Alerts by Rule"
    )

    fig.update_layout(
        xaxis_title="Fraud Rule",
        yaxis_title="Alert Count"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


with col2:

    fig = px.pie(
        rule_df,
        names="fraud_rule",
        values="alert_count",
        hole=0.4,
        title="Fraud Rule Distribution"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ============================================================
# CUSTOMER ANALYSIS
# ============================================================

st.subheader("👤 Fraud Alerts by Customer")


customer_query = f"""
    SELECT
        customer_id,
        COUNT(*) AS alert_count,
        COALESCE(SUM(amount), 0) AS fraud_amount
    FROM fraud_alerts
    {where_clause}
    GROUP BY customer_id
    ORDER BY alert_count DESC
"""


customer_df = run_query(customer_query)


fig = px.bar(
    customer_df,
    x="customer_id",
    y="alert_count",
    text="alert_count",
    title="Fraud Alerts by Customer"
)


st.plotly_chart(
    fig,
    use_container_width=True
)


# ============================================================
# MERCHANT + TRANSACTION TYPE
# ============================================================

col1, col2 = st.columns(2)


with col1:

    merchant_df = run_query(f"""
        SELECT
            merchant,
            COUNT(*) AS alert_count
        FROM fraud_alerts
        {where_clause}
        GROUP BY merchant
        ORDER BY alert_count DESC
    """)


    fig = px.bar(
        merchant_df,
        x="merchant",
        y="alert_count",
        text="alert_count",
        title="Fraud Alerts by Merchant"
    )


    st.plotly_chart(
        fig,
        use_container_width=True
    )


with col2:

    type_df = run_query(f"""
        SELECT
            transaction_type,
            COUNT(*) AS alert_count
        FROM fraud_alerts
        {where_clause}
        GROUP BY transaction_type
        ORDER BY alert_count DESC
    """)


    fig = px.pie(
        type_df,
        names="transaction_type",
        values="alert_count",
        hole=0.4,
        title="Fraud Alerts by Transaction Type"
    )


    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ============================================================
# LOCATION
# ============================================================

st.subheader("📍 Fraud Alerts by Location")


location_df = run_query(f"""
    SELECT
        location,
        COUNT(*) AS alert_count
    FROM fraud_alerts
    {where_clause}
    GROUP BY location
    ORDER BY alert_count DESC
""")


fig = px.bar(
    location_df,
    x="location",
    y="alert_count",
    text="alert_count",
    title="Fraud Alerts by Location"
)


st.plotly_chart(
    fig,
    use_container_width=True
)


# ============================================================
# FRAUD TREND
# ============================================================

st.subheader("📈 Fraud Trend")


trend_df = run_query(f"""
    SELECT
        DATE(transaction_timestamp) AS fraud_date,
        COUNT(*) AS fraud_alert_count
    FROM fraud_alerts
    {where_clause}
    GROUP BY DATE(transaction_timestamp)
    ORDER BY fraud_date
""")


fig = px.line(
    trend_df,
    x="fraud_date",
    y="fraud_alert_count",
    markers=True,
    title="Daily Fraud Alerts"
)


st.plotly_chart(
    fig,
    use_container_width=True
)


# ============================================================
# RECENT ALERTS
# ============================================================

st.subheader("🔎 Recent Fraud Alerts")


recent_df = run_query(f"""
    SELECT
        fraud_alert_id,
        transaction_id,
        customer_id,
        amount,
        transaction_type,
        merchant,
        location,
        fraud_rule,
        fraud_reason,
        fraud_status,
        created_at
    FROM fraud_alerts
    {where_clause}
    ORDER BY created_at DESC
    LIMIT 50
""")


st.dataframe(
    recent_df,
    use_container_width=True,
    hide_index=True
)

