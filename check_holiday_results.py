#!/usr/bin/env python3
"""
Script to check for holiday analysis results in the database
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import HolidayAnalysisResult, DataFile
from django.utils import timezone

def check_holiday_results():
    """Check for holiday analysis results in the database"""
    
    file_id = "f9215989-0e3c-4f64-ba4d-7eb3fd3fe0af"
    
    try:
        data_file = DataFile.objects.get(id=file_id)
        print(f"Checking holiday results for file: {data_file.file_name}")
        
        # Check all holiday analysis results for this file
        holiday_results = HolidayAnalysisResult.objects.filter(data_file=data_file).order_by('-analysis_date')
        print(f"\n📊 Total Holiday Analysis Results: {holiday_results.count()}")
        
        if holiday_results.count() == 0:
            print("❌ No holiday analysis results found for this file")
            
            # Check if there are any holiday results at all
            all_holiday_results = HolidayAnalysisResult.objects.all().order_by('-analysis_date')
            print(f"\n📊 Total Holiday Analysis Results in Database: {all_holiday_results.count()}")
            
            if all_holiday_results.count() > 0:
                print("Recent holiday analysis results:")
                for i, result in enumerate(all_holiday_results[:3]):
                    print(f"  {i+1}. {result.id} - {result.data_file.file_name} - {result.analysis_date}")
            else:
                print("❌ No holiday analysis results found in entire database")
            
            return None
        
        # Show holiday results
        for i, result in enumerate(holiday_results):
            print(f"\n  Holiday Result {i+1}:")
            print(f"    ID: {result.id}")
            print(f"    Status: {result.status}")
            print(f"    Analysis Date: {result.analysis_date}")
            print(f"    Processing Duration: {result.processing_duration} seconds")
            
            # Check analysis info
            analysis_info = result.analysis_info
            if analysis_info:
                print(f"    Total Transactions: {analysis_info.get('total_transactions', 'N/A')}")
                print(f"    Holiday Postings: {analysis_info.get('holiday_postings_count', 'N/A')}")
                print(f"    Holiday Percentage: {analysis_info.get('holiday_percentage', 'N/A')}%")
                print(f"    Unique Holidays: {analysis_info.get('unique_holidays', 'N/A')}")
            
            # Check if there are actual holiday postings
            holiday_postings = result.holiday_postings
            if holiday_postings:
                print(f"    Holiday Postings Found: {len(holiday_postings)}")
                if len(holiday_postings) > 0:
                    print(f"    Sample Holiday Posting: {holiday_postings[0]}")
            else:
                print(f"    No holiday postings found")
            
            if result.error_message:
                print(f"    Error: {result.error_message}")
        
        return holiday_results
        
    except DataFile.DoesNotExist:
        print(f"❌ Error: File with ID {file_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error checking holiday results: {e}")
        return None

def test_holiday_analysis():
    """Test if holiday analysis can be run manually"""
    
    try:
        from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
        from core.models import SAPGLPosting
        
        print("\n🧪 Testing Holiday Analysis...")
        
        # Get a few transactions to test
        transactions = list(SAPGLPosting.objects.all()[:100])
        print(f"Testing with {len(transactions)} transactions")
        
        orchestrator = MLAnalysisOrchestrator()
        
        # Try to run holiday analysis
        print("Running holiday analysis...")
        result = orchestrator.run_holiday_analysis(transactions)
        
        print(f"✅ Holiday analysis completed successfully!")
        print(f"Result keys: {list(result.keys())}")
        
        if 'holiday_postings' in result:
            print(f"Holiday postings found: {len(result['holiday_postings'])}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error testing holiday analysis: {e}")
        return None

if __name__ == "__main__":
    print("🔍 Checking Holiday Analysis Results")
    print("=" * 50)
    
    # Check existing results
    results = check_holiday_results()
    
    # Test holiday analysis
    test_result = test_holiday_analysis() 