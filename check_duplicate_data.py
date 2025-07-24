#!/usr/bin/env python
"""
Check duplicate analysis data in database
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import AnalyticsProcessingResult, DataFile

def check_duplicate_data():
    """Check duplicate analysis data in database"""
    print("🔍 Checking Duplicate Analysis Data in Database")
    print("=" * 60)
    
    # Get all duplicate analysis results
    duplicate_results = AnalyticsProcessingResult.objects.filter(
        analytics_type='duplicate_analysis'
    )
    
    print(f"📊 Total duplicate analysis results: {duplicate_results.count()}")
    print()
    
    for result in duplicate_results:
        print(f"📁 File: {result.data_file.file_name}")
        print(f"🆔 File ID: {result.data_file.id}")
        print(f"📊 Analytics ID: {result.id}")
        print(f"📈 Status: {result.processing_status}")
        print(f"📅 Created: {result.created_at}")
        print(f"📅 Processed: {result.processed_at}")
        
        # Check trial balance data
        if result.trial_balance_data:
            print(f"📊 Trial Balance Data Keys: {list(result.trial_balance_data.keys())}")
            
            # Check analysis_info
            analysis_info = result.trial_balance_data.get('analysis_info', {})
            print(f"📊 Analysis Info Keys: {list(analysis_info.keys())}")
            print(f"📊 Total Duplicate Transactions: {analysis_info.get('total_duplicate_transactions', 0)}")
            print(f"📊 Total Duplicate Groups: {analysis_info.get('total_duplicate_groups', 0)}")
            
            # Check duplicate_list
            duplicate_list = result.trial_balance_data.get('duplicate_list', [])
            print(f"📊 Duplicate List Length: {len(duplicate_list)}")
            
        else:
            print("❌ No trial balance data")
        
        # Check chart data
        if result.chart_data:
            print(f"📊 Chart Data Keys: {list(result.chart_data.keys())}")
        else:
            print("❌ No chart data")
        
        print("-" * 40)
    
    # Now check what file was used in the test
    print("\n🔍 Checking Test File:")
    print("-" * 40)
    
    # Get the first comprehensive analytics result (from our test)
    comprehensive_result = AnalyticsProcessingResult.objects.filter(
        analytics_type='comprehensive_expense',
        processing_status='COMPLETED'
    ).first()
    
    if comprehensive_result:
        test_file = comprehensive_result.data_file
        print(f"📁 Test File: {test_file.file_name}")
        print(f"🆔 Test File ID: {test_file.id}")
        
        # Check if this file has duplicate analysis
        file_duplicate_results = AnalyticsProcessingResult.objects.filter(
            data_file=test_file,
            analytics_type='duplicate_analysis',
            processing_status='COMPLETED'
        )
        
        print(f"📊 Duplicate analysis for test file: {file_duplicate_results.count()}")
        
        if file_duplicate_results.exists():
            print("✅ Test file has duplicate analysis")
            for dup_result in file_duplicate_results:
                print(f"📊 Duplicate Analysis ID: {dup_result.id}")
                print(f"📊 Status: {dup_result.processing_status}")
        else:
            print("❌ Test file does NOT have duplicate analysis")
            
            # Check if there are any duplicate results for other files
            other_duplicate_results = AnalyticsProcessingResult.objects.filter(
                analytics_type='duplicate_analysis',
                processing_status='COMPLETED'
            ).exclude(data_file=test_file)
            
            print(f"📊 Other files with duplicate analysis: {other_duplicate_results.count()}")
            for other_result in other_duplicate_results:
                print(f"📁 Other File: {other_result.data_file.file_name} (ID: {other_result.data_file.id})")

if __name__ == "__main__":
    check_duplicate_data() 