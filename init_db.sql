-- Initialize PostgreSQL database for Clickstream project
-- This script sets up the necessary schemas and tables
-- Tables are created in the default 'airflow' database

-- Note: The 'airflow' database is created automatically by PostgreSQL
-- via the POSTGRES_DB=airflow environment variable in docker-compose.yaml

-- Table for real-time product metrics from Spark Streaming
CREATE TABLE IF NOT EXISTS clickstream_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    session_id VARCHAR(100),
    device VARCHAR(50),
    kafka_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_metrics (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    view_count INTEGER DEFAULT 0,
    cart_count INTEGER DEFAULT 0,
    purchase_count INTEGER DEFAULT 0,
    total_events INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 2) DEFAULT 0.0,
    flash_sale_suggested BOOLEAN DEFAULT FALSE,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        product_id,
        window_start,
        window_end
    )
);

-- Table for user segmentation from Airflow DAG
CREATE TABLE IF NOT EXISTS user_segments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    segment_type VARCHAR(50) NOT NULL,
    segment_date DATE NOT NULL,
    view_count INTEGER DEFAULT 0,
    purchase_count INTEGER DEFAULT 0,
    products_viewed INTEGER DEFAULT 0,
    products_purchased INTEGER DEFAULT 0,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, segment_date)
);

-- Table for daily product summary
CREATE TABLE IF NOT EXISTS daily_product_summary (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    summary_date DATE NOT NULL,
    total_views INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0,
    total_carts INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 2) DEFAULT 0.0,
    flash_sale_recommended BOOLEAN DEFAULT FALSE,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, summary_date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_product_metrics_product_id ON product_metrics (product_id);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_event_date ON clickstream_events (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_user_id ON clickstream_events (user_id);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_event_type ON clickstream_events (event_type);

CREATE INDEX IF NOT EXISTS idx_product_metrics_timestamp ON product_metrics (window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_product_metrics_flash_sale ON product_metrics (flash_sale_suggested)
WHERE
    flash_sale_suggested = TRUE;

CREATE INDEX IF NOT EXISTS idx_user_segments_user_id ON user_segments (user_id);

CREATE INDEX IF NOT EXISTS idx_user_segments_segment_type ON user_segments (segment_type);

CREATE INDEX IF NOT EXISTS idx_user_segments_date ON user_segments (segment_date);

CREATE INDEX IF NOT EXISTS idx_daily_product_summary_product_id ON daily_product_summary (product_id);

CREATE INDEX IF NOT EXISTS idx_daily_product_summary_date ON daily_product_summary (summary_date);

CREATE INDEX IF NOT EXISTS idx_daily_product_summary_conversion ON daily_product_summary (conversion_rate DESC);

-- Grant permissions for current database
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO airflow;
