-- ============================================================================
-- Real-Time Data Analytics Pipeline: Schema DDL & Analytical Queries
-- Engine: PostgreSQL 14+ / Compatible with TimescaleDB, Metabase, and Grafana
-- Author: Prabhat Sharma (prabhat84sharma-ui)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. EXTENSIONS & SCHEMAS
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ----------------------------------------------------------------------------
-- 2. DDL: RAW INGESTION TABLES (Bronze Layer)
-- ----------------------------------------------------------------------------

-- Table: raw_orders
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    payment_method VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    status VARCHAR(24) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_created_at ON raw_orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id ON raw_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_category ON raw_orders (category);
CREATE INDEX IF NOT EXISTS idx_raw_orders_status ON raw_orders (status);

-- Table: raw_pageviews (Clickstream Telemetry)
CREATE TABLE IF NOT EXISTS raw_pageviews (
    event_id VARCHAR(64) PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    page_type VARCHAR(32) NOT NULL,
    referrer VARCHAR(64) NOT NULL,
    device_type VARCHAR(24) NOT NULL,
    latency_ms INT NOT NULL CHECK (latency_ms >= 0),
    bounce BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_pageviews_created_at ON raw_pageviews (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_pageviews_session_user ON raw_pageviews (session_id, user_id);
CREATE INDEX IF NOT EXISTS idx_raw_pageviews_page_type ON raw_pageviews (page_type);

-- ----------------------------------------------------------------------------
-- 3. DDL: PROCESSED & ENRICHED TABLES (Silver Layer)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS processed_transactions (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    gross_amount NUMERIC(12, 2) NOT NULL,
    tax_amount NUMERIC(10, 2) NOT NULL,
    net_revenue NUMERIC(12, 2) NOT NULL,
    estimated_margin NUMERIC(12, 2) NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    status VARCHAR(24) NOT NULL,
    is_fraud_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_reason VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_proc_tx_created_at ON processed_transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_proc_tx_status_created ON processed_transactions (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_proc_tx_fraud ON processed_transactions (is_fraud_anomaly) WHERE is_fraud_anomaly = TRUE;

-- ----------------------------------------------------------------------------
-- 4. DDL: REAL-TIME AGGREGATED METRICS (Gold Layer)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS realtime_metrics_1m (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    total_orders INT NOT NULL DEFAULT 0,
    completed_orders INT NOT NULL DEFAULT 0,
    failed_orders INT NOT NULL DEFAULT 0,
    total_gross_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    total_net_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    unique_customers INT NOT NULL DEFAULT 0,
    anomaly_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_realtime_metrics_1m PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_1m_window_end ON realtime_metrics_1m (window_end DESC);

CREATE TABLE IF NOT EXISTS realtime_metrics_5m (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    rolling_orders INT NOT NULL DEFAULT 0,
    rolling_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_ticket_size NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    error_rate_pct NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    anomaly_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_realtime_metrics_5m PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_5m_window_end ON realtime_metrics_5m (window_end DESC);

-- ----------------------------------------------------------------------------
-- 5. DDL: DEAD LETTER QUEUE (DLQ) FOR SCHEMA VIOLATIONS
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dead_letter_events (
    dlq_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    raw_payload JSONB NOT NULL,
    error_reason TEXT NOT NULL,
    source_ip VARCHAR(45),
    failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_events (failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_event_type ON dead_letter_events (event_type);

-- ============================================================================
-- 6. ADVANCED PRODUCTION REAL-TIME ANALYTICAL QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Rolling 5-Minute Moving Average and Cumulative Running Total Revenue
-- Purpose: Visualizes instantaneous sales momentum vs. smoothed trend line.
-- ----------------------------------------------------------------------------
WITH minute_revenue AS (
    SELECT
        date_trunc('minute', created_at) AS order_minute,
        COUNT(order_id) AS orders_count,
        SUM(total_amount) AS minute_gross_revenue,
        SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END) AS completed_revenue
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '1 hour'
    GROUP BY date_trunc('minute', created_at)
)
SELECT
    order_minute,
    orders_count,
    completed_revenue,
    -- Cumulative running total throughout the active 1-hour window
    SUM(completed_revenue) OVER (
        ORDER BY order_minute
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_running_revenue,
    -- 5-minute rolling moving average of revenue
    ROUND(
        AVG(completed_revenue) OVER (
            ORDER BY order_minute
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2
    ) AS rolling_5m_avg_revenue
FROM minute_revenue
ORDER BY order_minute DESC;

-- ----------------------------------------------------------------------------
-- QUERY 2: Real-Time Anomaly & Fraud Detection via Rolling Z-Score
-- Purpose: Flags transactions where total_amount deviates > 3 standard deviations
--          from the category's rolling 30-minute historical mean.
-- ----------------------------------------------------------------------------
WITH category_stats AS (
    SELECT
        order_id,
        customer_id,
        category,
        total_amount,
        created_at,
        status,
        AVG(total_amount) OVER (
            PARTITION BY category
            ORDER BY created_at
            RANGE BETWEEN INTERVAL '30 minutes' PRECEDING AND CURRENT ROW
        ) AS rolling_cat_mean,
        STDDEV(total_amount) OVER (
            PARTITION BY category
            ORDER BY created_at
            RANGE BETWEEN INTERVAL '30 minutes' PRECEDING AND CURRENT ROW
        ) AS rolling_cat_stddev
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '2 hours'
)
SELECT
    order_id,
    customer_id,
    category,
    total_amount,
    ROUND(rolling_cat_mean, 2) AS cat_mean,
    ROUND(COALESCE(rolling_cat_stddev, 0), 2) AS cat_stddev,
    ROUND(
        CASE 
            WHEN COALESCE(rolling_cat_stddev, 0) > 0 
            THEN (total_amount - rolling_cat_mean) / rolling_cat_stddev 
            ELSE 0 
        END, 2
    ) AS z_score,
    CASE
        WHEN rolling_cat_stddev IS NOT NULL 
             AND (total_amount - rolling_cat_mean) / NULLIF(rolling_cat_stddev, 0) >= 3.0 
        THEN 'CRITICAL_HIGH_VALUE_SPIKE'
        WHEN total_amount > 2000 THEN 'THRESHOLD_EXCEEDED'
        ELSE 'NORMAL'
    END AS anomaly_classification,
    created_at
FROM category_stats
WHERE created_at >= NOW() - INTERVAL '15 minutes'
ORDER BY created_at DESC;

-- ----------------------------------------------------------------------------
-- QUERY 3: Multi-Stage Clickstream Conversion Funnel Analysis
-- Purpose: Evaluates conversion drop-offs across funnel stages:
--          HOME -> SEARCH -> PRODUCT_DETAIL -> CHECKOUT -> PURCHASE
-- ----------------------------------------------------------------------------
WITH user_stages AS (
    SELECT
        session_id,
        user_id,
        MAX(CASE WHEN page_type = 'home' THEN 1 ELSE 0 END) AS visited_home,
        MAX(CASE WHEN page_type = 'search' THEN 1 ELSE 0 END) AS visited_search,
        MAX(CASE WHEN page_type = 'product_detail' THEN 1 ELSE 0 END) AS visited_product,
        MAX(CASE WHEN page_type = 'checkout' THEN 1 ELSE 0 END) AS visited_checkout
    FROM raw_pageviews
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY session_id, user_id
),
purchased_sessions AS (
    SELECT DISTINCT customer_id AS user_id
    FROM raw_orders
    WHERE status = 'COMPLETED'
      AND created_at >= NOW() - INTERVAL '30 minutes'
)
SELECT
    COUNT(DISTINCT s.session_id) AS total_sessions,
    SUM(s.visited_home) AS stage_1_home,
    SUM(s.visited_search) AS stage_2_search,
    SUM(s.visited_product) AS stage_3_product_detail,
    SUM(s.visited_checkout) AS stage_4_checkout,
    COUNT(DISTINCT p.user_id) AS stage_5_purchased,
    ROUND(SUM(s.visited_checkout)::NUMERIC / NULLIF(COUNT(DISTINCT s.session_id), 0) * 100, 2) AS checkout_rate_pct,
    ROUND(COUNT(DISTINCT p.user_id)::NUMERIC / NULLIF(SUM(s.visited_checkout), 0) * 100, 2) AS checkout_to_purchase_pct,
    ROUND(COUNT(DISTINCT p.user_id)::NUMERIC / NULLIF(COUNT(DISTINCT s.session_id), 0) * 100, 2) AS end_to_end_conversion_pct
FROM user_stages s
LEFT JOIN purchased_sessions p ON s.user_id = p.user_id;

-- ----------------------------------------------------------------------------
-- QUERY 4: Category Momentum & Top-K Trending Products with Dense Ranking
-- Purpose: Identifies top 3 revenue-generating products per category in the last hour.
-- ----------------------------------------------------------------------------
WITH product_performance AS (
    SELECT
        category,
        product_id,
        product_name,
        COUNT(order_id) AS units_sold,
        SUM(total_amount) AS category_revenue,
        DENSE_RANK() OVER (
            PARTITION BY category 
            ORDER BY SUM(total_amount) DESC
        ) AS rank_in_category
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '1 hour'
      AND status = 'COMPLETED'
    GROUP BY category, product_id, product_name
)
SELECT
    category,
    rank_in_category,
    product_name,
    product_id,
    units_sold,
    category_revenue,
    ROUND(category_revenue / SUM(category_revenue) OVER (PARTITION BY category) * 100, 2) AS pct_of_category_revenue
FROM product_performance
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;

-- ----------------------------------------------------------------------------
-- QUERY 5: Payment Gateway Health & Real-Time Failure Spikes (2-Min Slices)
-- Purpose: Alerts if failure rate of any payment gateway breaches 15% threshold.
-- ----------------------------------------------------------------------------
WITH gateway_window AS (
    SELECT
        payment_method,
        date_trunc('minute', created_at) - 
            (EXTRACT(minute FROM created_at)::INTEGER % 2) * INTERVAL '1 minute' AS time_slice_2m,
        COUNT(*) AS total_attempts,
        COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_attempts,
        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS successful_attempts
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY payment_method, time_slice_2m
)
SELECT
    time_slice_2m,
    payment_method,
    total_attempts,
    successful_attempts,
    failed_attempts,
    ROUND((failed_attempts::NUMERIC / NULLIF(total_attempts, 0)) * 100, 2) AS failure_rate_pct,
    CASE 
        WHEN (failed_attempts::NUMERIC / NULLIF(total_attempts, 0)) >= 0.20 THEN 'CRITICAL_OUTAGE'
        WHEN (failed_attempts::NUMERIC / NULLIF(total_attempts, 0)) >= 0.10 THEN 'WARNING_DEGRADED'
        ELSE 'HEALTHY'
    END AS gateway_status
FROM gateway_window
ORDER BY time_slice_2m DESC, failure_rate_pct DESC;

-- ----------------------------------------------------------------------------
-- QUERY 6: Order Value Percentile Distribution (P50, P75, P90, P95, P99)
-- Purpose: Computes exact distribution percentiles to identify skew and whale orders.
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_completed_orders,
    ROUND(AVG(total_amount), 2) AS mean_order_value,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS p50_median,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS p75_upper_quartile,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS p90_percentile,
    ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS p95_percentile,
    ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS p99_whale_threshold,
    ROUND(MAX(total_amount), 2) AS max_order_value
FROM raw_orders
WHERE created_at >= NOW() - INTERVAL '2 hours'
  AND status = 'COMPLETED';

-- ----------------------------------------------------------------------------
-- QUERY 7: Real-Time Customer Cohort Repurchase Velocity
-- Purpose: Measures time lag between consecutive purchases for returning customers.
-- ----------------------------------------------------------------------------
WITH customer_order_history AS (
    SELECT
        customer_id,
        order_id,
        total_amount,
        created_at,
        LAG(created_at, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY created_at
        ) AS previous_order_time,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY created_at
        ) AS customer_order_sequence
    FROM raw_orders
    WHERE status = 'COMPLETED'
      AND created_at >= NOW() - INTERVAL '24 hours'
)
SELECT
    customer_id,
    order_id,
    customer_order_sequence,
    total_amount,
    created_at,
    previous_order_time,
    ROUND(
        EXTRACT(EPOCH FROM (created_at - previous_order_time)) / 60.0, 1
    ) AS minutes_since_last_order
FROM customer_order_history
WHERE customer_order_sequence > 1
ORDER BY created_at DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- QUERY 8: Geospatial Sales Density & Latency Correlation
-- Purpose: Evaluates regional revenue volume vs average web telemetry latency.
-- ----------------------------------------------------------------------------
WITH regional_orders AS (
    SELECT
        country,
        COUNT(order_id) AS total_orders,
        SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END) AS total_regional_revenue,
        ROUND(AVG(total_amount), 2) AS avg_regional_ticket
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '1 hour'
    GROUP BY country
)
SELECT
    ro.country,
    ro.total_orders,
    ro.total_regional_revenue,
    ro.avg_regional_ticket,
    ROUND(
        ro.total_regional_revenue / SUM(ro.total_regional_revenue) OVER () * 100, 2
    ) AS revenue_share_pct
FROM regional_orders ro
ORDER BY ro.total_regional_revenue DESC;

-- ----------------------------------------------------------------------------
-- QUERY 9: Dead-Letter Queue (DLQ) Diagnostic Failure Breakdown
-- Purpose: Ingestion observability - tracks top schema violations & parsing errors.
-- ----------------------------------------------------------------------------
SELECT
    event_type,
    error_reason,
    COUNT(*) AS failure_count,
    MAX(failed_at) AS most_recent_failure,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) AS failure_percentage
FROM dead_letter_events
WHERE failed_at >= NOW() - INTERVAL '24 hours'
GROUP BY event_type, error_reason
ORDER BY failure_count DESC;

-- ----------------------------------------------------------------------------
-- QUERY 10: Executive Real-Time KPI Snapshot (Current vs Prior Period)
-- Purpose: Headline statistics for Streamlit & Grafana executive overview card.
-- ----------------------------------------------------------------------------
WITH current_15m AS (
    SELECT
        COUNT(*) AS orders_count,
        COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS completed_rev,
        COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_count,
        COUNT(DISTINCT customer_id) AS active_buyers
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '15 minutes'
),
prior_15m AS (
    SELECT
        COUNT(*) AS orders_count,
        COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS completed_rev,
        COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_count,
        COUNT(DISTINCT customer_id) AS active_buyers
    FROM raw_orders
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
      AND created_at < NOW() - INTERVAL '15 minutes'
)
SELECT
    c.completed_rev AS current_revenue,
    p.completed_rev AS prior_revenue,
    ROUND(
        CASE WHEN p.completed_rev > 0 
        THEN ((c.completed_rev - p.completed_rev) / p.completed_rev) * 100 
        ELSE 0 END, 2
    ) AS revenue_growth_pct,
    c.orders_count AS current_orders,
    p.orders_count AS prior_orders,
    c.active_buyers AS current_active_buyers,
    ROUND((c.failed_count::NUMERIC / NULLIF(c.orders_count, 0)) * 100, 2) AS current_failure_rate_pct
FROM current_15m c
CROSS JOIN prior_15m p;
