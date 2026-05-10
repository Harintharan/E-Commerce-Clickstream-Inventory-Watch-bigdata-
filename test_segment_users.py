#!/usr/bin/env python3
import sys
from datetime import datetime

sys.path.insert(0, '/airflow/dags')
from dag_segmentation import segment_users

try:
    context = {
        'execution_date': datetime(2026, 5, 10),
    }
    result = segment_users(**context)
    print(f"✅ Success: {result}")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
