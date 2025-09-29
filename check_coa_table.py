#!/usr/bin/env python3
"""
Check chart_of_accounts table structure
"""

import sys
import os
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from django.db import connection

# Get the actual database table structure for chart_of_accounts
with connection.cursor() as cursor:
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'chart_of_accounts' ORDER BY ordinal_position;")
    columns = [row[0] for row in cursor.fetchall()]
    print('chart_of_accounts table columns:')
    for col in columns:
        print(f'  - {col}')
