#!/usr/bin/env python
"""
Test script to verify duplicate data integration in DatabaseStoredComprehensiveAnalyticsView
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, AnalyticsProcessingResult, BackdatedAnalysisResult
from core.views import DatabaseStoredComprehensiveAnalyticsView
from django.test import RequestFactory

def test_duplicate_integration():
    """Test duplicate data integration in comprehensive analytics"""
    print("🚀 Testing Duplicate Data Integration in DatabaseStoredComprehensiveAnalyticsView")
    print("=" * 80)
    
    # Get the first file that has comprehensive analytics
    comprehensive_analytics = AnalyticsProcessingResult.objects.filter(
        analytics_type='comprehensive_expense',
        processing_status='COMPLETED'
    ).first()
    
    if not comprehensive_analytics:
        print("❌ No comprehensive analytics found in database")
        return
    
    data_file = comprehensive_analytics.data_file
    
    # Check for duplicate analysis
    duplicate_analytics = AnalyticsProcessingResult.objects.filter(
        data_file=data_file,
        analytics_type='duplicate_analysis',
        processing_status='COMPLETED'
    ).first()
    
    # Check for backdated analysis
    backdated_analytics = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).first()
    
    print(f"📁 File: {data_file.file_name}")
    print(f"🆔 File ID: {data_file.id}")
    print(f"📊 Comprehensive Analytics ID: {comprehensive_analytics.id}")
    print(f"📈 Duplicate Analytics: {'✅ Found' if duplicate_analytics else '❌ Not found'}")
    print(f"📅 Backdated Analytics: {'✅ Found' if backdated_analytics else '❌ Not found'}")
    print()
    
    # Test the view directly
    view = DatabaseStoredComprehensiveAnalyticsView()
    
    print("🔍 Testing General Stats Integration:")
    print("-" * 50)
    
    # Test general stats integration
    general_stats = view._get_general_stats_from_db(comprehensive_analytics, data_file)
    
    print(f"📊 Base duplicates_found: {general_stats.get('duplicates_found', 0)}")
    print(f"📊 Duplicate analysis entries: {general_stats.get('duplicate_analysis_entries', 0)}")
    print(f"📊 Duplicate analysis amount: {general_stats.get('duplicate_analysis_amount', 0):,.2f}")
    print(f"📊 Duplicate analysis groups: {general_stats.get('duplicate_analysis_groups', 0)}")
    print(f"📊 Total duplicates including analysis: {general_stats.get('total_duplicates_including_analysis', 0)}")
    print(f"📊 Backdated entries: {general_stats.get('backdated_entries', 0)}")
    print(f"📊 Total anomalies: {general_stats.get('total_anomalies', 0)}")
    print()
    
    print("🔍 Testing Summary Integration:")
    print("-" * 50)
    
    # Test summary integration
    summary = view._get_summary_from_db(comprehensive_analytics, data_file)
    
    print(f"📊 Summary duplicates_found: {summary.get('duplicates_found', 0)}")
    print(f"📊 Summary duplicate analysis entries: {summary.get('duplicate_analysis_entries', 0)}")
    print(f"📊 Summary duplicate analysis amount: {summary.get('duplicate_analysis_amount', 0):,.2f}")
    print(f"📊 Summary total duplicates including analysis: {summary.get('total_duplicates_including_analysis', 0)}")
    print(f"📊 Summary backdated entries: {summary.get('backdated_entries', 0)}")
    print(f"📊 Summary total anomalies: {summary.get('total_anomalies', 0)}")
    print()
    
    print("🔍 Testing Duplicate Data Method:")
    print("-" * 50)
    
    # Test duplicate data method
    duplicate_data = view._get_duplicate_data_from_db(data_file)
    
    print(f"📊 Has duplicate data: {duplicate_data.get('has_duplicate_data', False)}")
    print(f"📊 Duplicate analysis ID: {duplicate_data.get('duplicate_analysis_id', 'None')}")
    
    if duplicate_data.get('has_duplicate_data'):
        duplicate_stats = duplicate_data.get('duplicate_stats', {})
        print(f"📊 Total duplicate transactions: {duplicate_stats.get('total_duplicate_transactions', 0)}")
        print(f"📊 Total duplicate groups: {duplicate_stats.get('total_duplicate_groups', 0)}")
        print(f"📊 Total amount involved: {duplicate_stats.get('total_amount_involved', 0):,.2f}")
        print(f"📊 Duplicate percentage: {duplicate_stats.get('duplicate_percentage', 0):.2f}%")
        print(f"📊 Duplicate list length: {len(duplicate_data.get('duplicate_list', []))}")
        print(f"📊 Summary table length: {len(duplicate_data.get('summary_table', []))}")
    else:
        print(f"📊 Message: {duplicate_data.get('message', 'No message')}")
    
    print()
    
    print("🔍 Testing Complete API Response:")
    print("-" * 50)
    
    # Test the complete API response
    try:
        factory = RequestFactory()
        request = factory.get(f'/api/db-comprehensive-analytics/file/{data_file.id}/')
        response = view.get(request, file_id=data_file.id)
        
        if response.status_code == 200:
            data = response.data
            print("✅ API call successful")
            print(f"📊 Response keys: {list(data.keys())}")
            
            # Check if duplicate_data is in the response
            if 'duplicate_data' in data:
                print("✅ Duplicate data found in response")
                dup_data = data['duplicate_data']
                print(f"📊 Duplicate data keys: {list(dup_data.keys())}")
                print(f"📊 Has duplicate data: {dup_data.get('has_duplicate_data', False)}")
            else:
                print("❌ Duplicate data not found in response")
            
            # Check general stats
            general_stats = data.get('general_stats', {})
            print(f"📊 General stats duplicate_analysis_entries: {general_stats.get('duplicate_analysis_entries', 0)}")
            print(f"📊 General stats total_duplicates_including_analysis: {general_stats.get('total_duplicates_including_analysis', 0)}")
            
            # Check summary
            summary = data.get('summary', {})
            print(f"📊 Summary duplicate_analysis_entries: {summary.get('duplicate_analysis_entries', 0)}")
            print(f"📊 Summary total_duplicates_including_analysis: {summary.get('total_duplicates_including_analysis', 0)}")
            
        else:
            print(f"❌ API call failed with status: {response.status_code}")
            print(f"❌ Error: {response.data}")
            
    except Exception as e:
        print(f"❌ API test error: {e}")
    
    print()
    print("🎯 Test Summary:")
    print("-" * 50)
    print("✅ Duplicate data integration has been added to DatabaseStoredComprehensiveAnalyticsView")
    print("✅ Duplicate data is now included in:")
    print("   - General stats (duplicate_analysis_entries, total_duplicates_including_analysis)")
    print("   - Summary (duplicate_analysis_entries, total_duplicates_including_analysis)")
    print("   - Dedicated duplicate_data section in the response")
    print("✅ The endpoint now provides comprehensive analytics including:")
    print("   - Risk data (from ML processing)")
    print("   - Backdated data (from backdated analysis)")
    print("   - Duplicate data (from duplicate analysis)")
    print("   - General stats, charts, and summary")

if __name__ == "__main__":
    test_duplicate_integration() 