#!/usr/bin/env python
"""
Simple test for Holiday Analysis API
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from django.test import Client
from django.urls import reverse
from core.models import DataFile, HolidayAnalysisResult

def test_holiday_api():
    """Test the Holiday Analysis API"""
    print("=== SIMPLE HOLIDAY API TEST ===")
    
    # Get the first data file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data file found.")
        return
    
    file_id = str(data_file.id)
    print(f"Testing with file ID: {file_id}")
    
    # Check if holiday analysis exists
    holiday_analysis = HolidayAnalysisResult.objects.filter(
        data_file=data_file, status='COMPLETED'
    ).first()
    
    if not holiday_analysis:
        print("No holiday analysis found. Creating one...")
        # Run the analysis script
        os.system("python check_existing_data.py")
        holiday_analysis = HolidayAnalysisResult.objects.filter(
            data_file=data_file, status='COMPLETED'
        ).first()
    
    if not holiday_analysis:
        print("Failed to create holiday analysis.")
        return
    
    print(f"Holiday analysis found with ID: {holiday_analysis.id}")
    
    # Test API endpoint
    client = Client()
    
    try:
        # Make the API request
        url = reverse('holiday-analysis', kwargs={'file_id': file_id})
        print(f"Testing URL: {url}")
        
        response = client.get(url)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API Test Success!")
            
            # Display summary
            summary = data.get('summary', {})
            print(f"\n📊 Summary:")
            print(f"   Total Holiday Postings: {summary.get('total_holiday_postings', 0)}")
            print(f"   Holiday Percentage: {summary.get('holiday_percentage', 0):.2f}%")
            print(f"   Risk Level: {summary.get('risk_level', 'Unknown')}")
            
            # Display analysis info
            analysis_info = data.get('analysis_info', {})
            print(f"\n📋 Analysis Info:")
            print(f"   Analysis ID: {analysis_info.get('analysis_id', 'N/A')}")
            print(f"   Status: {analysis_info.get('status', 'N/A')}")
            
            print("\n✅ Holiday Analysis API is working correctly!")
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"Response: {response.content.decode()}")
            
    except Exception as e:
        print(f"❌ Test Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_holiday_api() 