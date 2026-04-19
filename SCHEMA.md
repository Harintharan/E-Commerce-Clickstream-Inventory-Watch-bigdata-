# Database Schema Documentation

## Overview

The PostgreSQL database stores all processed clickstream data, user segments, and product summaries. The schema is designed for analytical queries with proper indexing for performance.

## Database Name
`clickstream_db`

## Tables

### 1. `product_metrics` (Real-time Stream Data)

**Purpose**: Stores windowed aggregations from Spark Structured Streaming

**Columns**:
```sql
id                    SERIAL PRIMARY KEY        -- Unique record identifier
product_id            INTEGER NOT NULL          -- Product identifier
window_start          TIMESTAMP NOT NULL        -- Window start time
window_end            TIMESTAMP NOT NULL        -- Window end time
view_count            INTEGER DEFAULT 0         -- Number of product views
cart_count            INTEGER DEFAULT 0         -- Number of add-to-cart events
purchase_count        INTEGER DEFAULT 0         -- Number of purchases
total_events          INTEGER DEFAULT 0         -- Total events in window
conversion_rate       DECIMAL(5, 2) DEFAULT 0.0 -- Conversion %
flash_sale_suggested  BOOLEAN DEFAULT FALSE     -- Flash Sale trigger flag
processed_timestamp   TIMESTAMP DEFAULT NOW     -- Record creation time
```

**Constraints**:
- UNIQUE(product_id, window_start, window_end)

**Indexes**:
- `idx_product_metrics_product_id` - Query by product_id
- `idx_product_metrics_timestamp` - Query by time range
- `idx_product_metrics_flash_sale` - Query Flash Sale triggers

**Example Query**:
```sql
SELECT product_id, view_count, purchase_count, conversion_rate
FROM product_metrics
WHERE flash_sale_suggested = TRUE
AND window_end >= NOW() - INTERVAL '1 hour'
ORDER BY conversion_rate ASC
LIMIT 10;
```

---

### 2. `user_segments` (Daily Batch Data)

**Purpose**: Stores user segmentation results from Airflow DAG

**Columns**:
```sql
id                    SERIAL PRIMARY KEY        -- Unique record identifier
user_id               INTEGER NOT NULL          -- User identifier
segment_type          VARCHAR(50) NOT NULL      -- 'Window Shopper' or 'Buyer'
segment_date          DATE NOT NULL             -- Date of segmentation
view_count            INTEGER DEFAULT 0         -- Total views by user
purchase_count        INTEGER DEFAULT 0         -- Total purchases by user
products_viewed       INTEGER DEFAULT 0         -- Distinct products viewed
products_purchased    INTEGER DEFAULT 0         -- Distinct products purchased
processed_timestamp   TIMESTAMP DEFAULT NOW     -- Record creation time
```

**Constraints**:
- UNIQUE(user_id, segment_date)
- segment_type IN ('Window Shopper', 'Buyer')

**Indexes**:
- `idx_user_segments_user_id` - Query by user
- `idx_user_segments_segment_type` - Query by segment
- `idx_user_segments_date` - Query by date range

**Segment Definitions**:
- **Window Shopper**: User who only viewed/browsed, no purchases
- **Buyer**: User who made at least one purchase

**Example Query**:
```sql
SELECT segment_type, COUNT(*) as user_count,
       AVG(purchase_count) as avg_purchases,
       SUM(products_purchased) as total_products_sold
FROM user_segments
WHERE segment_date = CURRENT_DATE - 1
GROUP BY segment_type;
```

---

### 3. `daily_product_summary` (Daily Batch Data)

**Purpose**: Stores aggregated daily metrics and recommendations

**Columns**:
```sql
id                      SERIAL PRIMARY KEY        -- Unique record identifier
product_id              INTEGER NOT NULL          -- Product identifier
summary_date            DATE NOT NULL             -- Date of summary
total_views             INTEGER DEFAULT 0         -- Total views for the day
total_purchases         INTEGER DEFAULT 0         -- Total purchases for the day
total_carts             INTEGER DEFAULT 0         -- Total add-to-cart events
unique_visitors         INTEGER DEFAULT 0         -- Distinct users viewing
conversion_rate         DECIMAL(5, 2) DEFAULT 0.0 -- Daily conversion %
flash_sale_recommended  BOOLEAN DEFAULT FALSE     -- Flash Sale recommendation
processed_timestamp     TIMESTAMP DEFAULT NOW     -- Record creation time
```

**Constraints**:
- UNIQUE(product_id, summary_date)

**Indexes**:
- `idx_daily_product_summary_product_id` - Query by product
- `idx_daily_product_summary_date` - Query by date
- `idx_daily_product_summary_conversion` - Query by conversion rate

**Example Query - Top Products**:
```sql
SELECT product_id, total_views, total_purchases, conversion_rate,
       flash_sale_recommended
FROM daily_product_summary
WHERE summary_date = CURRENT_DATE - 1
ORDER BY total_views DESC
LIMIT 5;
```

---

## Relationships

```
product_metrics (Real-time, 10-min windows)
    ↓ (Aggregated by DAG)
daily_product_summary (Daily summary)

product_metrics (Contains event info)
    ↓ (User extraction)
user_segments (Daily user classification)
```

## Data Flow

```
1. Kafka Producer
   ↓ (JSON events)
2. Kafka Topic (clickstream_topic)
   ↓ (Consumed)
3. PySpark Streaming
   ↓ (10-min windowing)
4. product_metrics table
   ↓ (Aggregated by Airflow DAG)
5. daily_product_summary & user_segments tables
   ↓ (Analyzed)
6. Business Intelligence
```

## Performance Notes

### Index Selection
- Use `idx_product_metrics_product_id` for product-level queries
- Use `idx_product_metrics_timestamp` for time-range queries
- Use `idx_product_metrics_flash_sale` for anomaly detection

### Query Optimization Tips
1. Always filter by date for time-based queries
2. Use LIMIT for large result sets
3. Aggregate on summary tables rather than metrics table
4. Consider partial indexes for frequent filter conditions

### Typical Row Counts
- `product_metrics`: ~50-500 rows per minute (depends on window)
- `user_segments`: ~100-1000 rows per day
- `daily_product_summary`: ~50 rows per day

## Maintenance

### Backup Schedule
```sql
-- Daily backup recommended
pg_dump -U airflow clickstream_db > backup_$(date +%Y%m%d).sql
```

### Archive Old Data
```sql
-- Archive metrics older than 30 days
CREATE TABLE product_metrics_archive AS
SELECT * FROM product_metrics
WHERE window_end < CURRENT_DATE - 30;

DELETE FROM product_metrics
WHERE window_end < CURRENT_DATE - 30;
```

### Vacuum and Analyze
```sql
VACUUM ANALYZE product_metrics;
VACUUM ANALYZE user_segments;
VACUUM ANALYZE daily_product_summary;
```

## Access Control

### Default User: `airflow`
- Full read/write privileges
- Used by Spark and Airflow services

### Create Read-Only User
```sql
CREATE ROLE analytics WITH LOGIN PASSWORD 'password';
GRANT CONNECT ON DATABASE clickstream_db TO analytics;
GRANT USAGE ON SCHEMA public TO analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics;
```

## Example Analytical Queries

### 1. Best Selling Products
```sql
SELECT product_id, total_purchases, conversion_rate
FROM daily_product_summary
WHERE summary_date >= CURRENT_DATE - 7
GROUP BY product_id
ORDER BY total_purchases DESC
LIMIT 10;
```

### 2. Flash Sale Candidates
```sql
SELECT product_id, total_views, total_purchases,
       (total_purchases::float / total_views * 100) as conversion_pct
FROM daily_product_summary
WHERE summary_date = CURRENT_DATE - 1
AND total_views > 100
AND total_purchases < 5
ORDER BY total_views DESC;
```

### 3. User Conversion Funnel
```sql
SELECT user_id, segment_date,
       CASE WHEN segment_type = 'Buyer' THEN 'Converted'
            ELSE 'Not Converted' END as status,
       products_viewed, products_purchased
FROM user_segments
WHERE segment_date >= CURRENT_DATE - 30
LIMIT 100;
```

### 4. Product Performance Trends
```sql
SELECT product_id,
       summary_date,
       total_views,
       total_purchases,
       LAG(total_purchases) OVER (PARTITION BY product_id ORDER BY summary_date) as prev_purchases,
       total_purchases - LAG(total_purchases) OVER (PARTITION BY product_id ORDER BY summary_date) as purchase_change
FROM daily_product_summary
WHERE product_id = 1
ORDER BY summary_date DESC
LIMIT 30;
```

---

## Version Control

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-04-18 | Initial schema design |

---

Last Updated: April 18, 2024
