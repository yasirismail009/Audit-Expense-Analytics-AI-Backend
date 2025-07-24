#!/usr/bin/env python3
"""
Test script to check if chart data is saved in AnalyticsProcessingResult database for duplicate analysis
"""

import os
import sys
import django
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, AnalyticsProcessingResult

def check_duplicate_chart_data_in_db():
    """Check if chart data is saved in AnalyticsProcessingResult for duplicate analysis"""
    print("🔍 Checking Duplicate Chart Data in Database")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get all data files
    data_files = DataFile.objects.all()
    print(f"\n📁 Found {data_files.count()} data files")
    
    for data_file in data_files:
        print(f"\n📋 File: {data_file.file_name}")
        print(f"   ID: {data_file.id}")
        print(f"   Status: {data_file.status}")
        print(f"   Records: {data_file.total_records}")
        
        # Check for duplicate analysis results
        duplicate_results = AnalyticsProcessingResult.objects.filter(
            data_file=data_file,
            analytics_type='duplicate_analysis'
        ).order_by('-created_at')
        
        print(f"   Duplicate Analysis Results: {duplicate_results.count()}")
        
        if duplicate_results.exists():
            latest_result = duplicate_results.first()
            print(f"   Latest Result ID: {latest_result.id}")
            print(f"   Created: {latest_result.created_at}")
            print(f"   Status: {latest_result.processing_status}")
            
            # Check chart_data field
            chart_data = latest_result.chart_data
            if chart_data:
                print(f"   ✅ Chart Data: Available")
                print(f"      Type: {type(chart_data)}")
                if isinstance(chart_data, dict):
                    print(f"      Keys: {list(chart_data.keys())}")
                    for key, value in chart_data.items():
                        if isinstance(value, list):
                            print(f"      - {key}: {len(value)} items")
                        elif isinstance(value, dict):
                            print(f"      - {key}: {len(value)} keys")
                        else:
                            print(f"      - {key}: {type(value)}")
                else:
                    print(f"      Content: {chart_data}")
            else:
                print(f"   ❌ Chart Data: Not available")
            
            # Check trial_balance_data for chart_data
            trial_balance_data = latest_result.trial_balance_data
            if trial_balance_data and isinstance(trial_balance_data, dict):
                if 'chart_data' in trial_balance_data:
                    print(f"   ✅ Chart Data in trial_balance_data: Available")
                    chart_data_tb = trial_balance_data['chart_data']
                    if isinstance(chart_data_tb, dict):
                        print(f"      Keys: {list(chart_data_tb.keys())}")
                    else:
                        print(f"      Type: {type(chart_data_tb)}")
                else:
                    print(f"   ❌ Chart Data in trial_balance_data: Not available")
                    print(f"      Available keys: {list(trial_balance_data.keys())}")
            else:
                print(f"   ❌ trial_balance_data: Not available or not dict")
                
        else:
            print(f"   ❌ No duplicate analysis results found")
    
    print("\n✅ Database check completed!")

def main():
    """Main function"""
    try:
        check_duplicate_chart_data_in_db()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 