# Daily Summary Report & Email Configuration

## Overview

The Airflow DAG now includes automatic generation and email delivery of daily product summaries. The system:
- Generates formatted text reports with **top 5 most viewed products**
- Saves reports to disk for archival
- Sends HTML+text email with the report
- Handles SMTP authentication gracefully (falls back to local storage if email fails)

## Features

### 1. **Report Generation** (`generate_summary_report` task)
Creates a formatted text file containing:
- **Top 5 Most Viewed Products**
  - Rank, Product ID, Views, Purchases, Conversion Rate, Flash Sale recommendation
- **User Segmentation Summary**
  - Count of Window Shoppers vs Buyers
  - Average purchases and views per segment

Report files are stored at: `/airflow/logs/reports/daily_summary_YYYY-MM-DD.txt`

### 2. **Email Delivery** (`send_summary_email` task)
Sends email with:
- HTML-formatted report body
- Plain-text alternative
- Subject: `Daily E-Commerce Summary Report - YYYY-MM-DD`

If SMTP fails, the system logs a warning but continues—reports are always saved locally.

## Environment Configuration

Set these environment variables to enable email delivery. Add to `docker-compose.yaml` or `.env`:

```yaml
services:
  airflow-webserver:
    environment:
      SMTP_HOST: smtp.gmail.com              # SMTP server
      SMTP_PORT: 587                         # TLS port
      SMTP_USER: your-email@gmail.com        # Sender email
      SMTP_PASSWORD: xxxx-xxxx-xxxx-xxxx     # App-specific password
      REPORT_RECIPIENT: admin@example.com    # Email recipient
      REPORT_SENDER: your-email@gmail.com    # Sender display email
```

### Gmail Configuration (Recommended)

1. **Enable 2-Step Verification** on your Gmail account
2. **Create an App Password**:
   - Go to [Google Account Security](https://myaccount.google.com/security)
   - Select "App passwords" (requires 2FA)
   - Choose "Mail" and "Windows Computer"
   - Copy the 16-character password
3. **Use in configuration**:
   ```
   SMTP_USER=your-email@gmail.com
   SMTP_PASSWORD=xxxx-xxxx-xxxx-xxxx  # The 16-char app password
   ```

### Other SMTP Providers

| Provider | SMTP Host | Port | Notes |
|----------|-----------|------|-------|
| Gmail | smtp.gmail.com | 587 | Requires app password |
| Outlook | smtp-mail.outlook.com | 587 | Use full email address |
| AWS SES | email-smtp.REGION.amazonaws.com | 587 | Requires AWS credentials |
| Custom | - | - | Set your server details |

## DAG Task Flow

```
create_tables
    ↓
segment_users  →  data_quality_validation  →  generate_summary_report  →  send_summary_email
                ↗
    generate_daily_summary
```

**Schedule**: Daily at **11:00 PM UTC** (23:00)

## Accessing Reports

### From Airflow UI
1. Navigate to **Admin** → **XCom**
2. Filter by DAG: `clickstream_daily_batch`
3. Task: `generate_summary_report`
4. View the returned dictionary with `report_path`

### From File System
```bash
# Inside container
docker exec airflow-webserver ls -la /airflow/logs/reports/

# Copy report to host
docker cp airflow-webserver:/airflow/logs/reports/daily_summary_2026-05-10.txt ./
```

## Troubleshooting

### Email Not Received

1. **Check SMTP credentials**:
   ```bash
   # View logs
   docker logs airflow-webserver | grep -i "smtp\|email"
   ```

2. **Verify report was generated**:
   ```bash
   docker exec airflow-webserver ls /airflow/logs/reports/
   ```

3. **Test SMTP connection**:
   ```bash
   docker exec airflow-webserver python3 -c "
   import smtplib
   try:
       server = smtplib.SMTP('smtp.gmail.com', 587)
       server.starttls()
       server.login('your-email@gmail.com', 'your-app-password')
       print('SMTP connection successful')
       server.quit()
   except Exception as e:
       print(f'Error: {e}')
   "
   ```

### Report Generation Failing

Check if `product_metrics` table has data:
```bash
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics WHERE CAST(window_start AS DATE) = CURRENT_DATE - 1;"
```

If empty, ensure the stream processor is running:
```bash
docker logs stream-processor
```

## Example Report Output

```
================================================================================
E-COMMERCE CLICKSTREAM - DAILY SUMMARY REPORT
Date: 2026-05-10
Generated: 2026-05-10 23:15:30 UTC
================================================================================

TOP 5 MOST VIEWED PRODUCTS
--------------------------------------------------------------------------------
Rank   Product ID   Views      Purchases    Conv. Rate   Flash Sale   
--------------------------------------------------------------------------------
1      12           245        8            3.27%        NO           
2      7            198        15           7.58%        NO           
3      31           156        2            1.28%        YES          
4      44           142        5            3.52%        NO           
5      19           135        10           7.41%        NO           

USER SEGMENTATION SUMMARY
--------------------------------------------------------------------------------
Segment Type         User Count      Avg Purchases   Avg Views      
--------------------------------------------------------------------------------
Buyer                25              2.40            18.50          
Window Shopper       75              0.00            12.30          

================================================================================
End of Report
================================================================================
```

## Customization

### Change Report Directory
Edit `dag_segmentation.py`:
```python
REPORT_DIR = '/airflow/logs/reports'  # Change this path
```

### Change Email Format
Edit the `html_content` variable in `send_summary_email()` function to customize email appearance.

### Add Additional Recipients
Modify to support multiple recipients:
```python
REPORT_RECIPIENTS = os.getenv('REPORT_RECIPIENTS', 'admin@example.com,manager@example.com').split(',')
# Then update msg['To'] to handle list
```

## Notes

- Reports are **always saved locally** even if email fails
- Email task runs **after** data quality validation
- XCom is used to pass report path between tasks
- Logs are stored in `/airflow/logs/clickstream_daily_batch_*.log`
