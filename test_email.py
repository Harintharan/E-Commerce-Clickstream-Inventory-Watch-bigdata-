#!/usr/bin/env python3
import sys
from datetime import datetime

sys.path.insert(0, '/airflow/dags')
from dag_segmentation import send_summary_email

try:
    # Create context with execution_date
    context = {
        'execution_date': datetime(2026, 5, 10),
        'task_instance': None
    }
    result = send_summary_email(**context)
    print(f"✅ Email function executed: {result}")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
