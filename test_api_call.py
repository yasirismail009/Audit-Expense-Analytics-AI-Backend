#!/usr/bin/env python
"""
Test script to simulate the exact API call that's failing
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, BackdatedAnalysisResult
from django.test import RequestFactory
from core.views import DatabaseStoredBackdatedAnalysisView

def test_api_call():
    """Test the exact API call that's failing"""
    print("🔍 Testing Exact API Call")
    print("=" * 50)
    
    # Get the first file that has backdated analysis
    backdated_analysis = BackdatedAnalysisResult.objects.filter(
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).first()
    
    if not backdated_analysis:
        print("❌ No backdated analysis found in database")
        return
    
    data_file = backdated_analysis.data_file
    file_id = str(data_file.id)
    
    print(f"📁 File: {data_file.file_name}")
    print(f"🆔 File ID: {file_id}")
    print(f"📊 Backdated Analysis ID: {backdated_analysis.id}")
    print()
    
    # Create a mock request
    factory = RequestFactory()
    request = factory.get(f'/api/db-comprehensive-backdated-analysis/file/{file_id}/')
    
    # Test the view directly
    view = DatabaseStoredBackdatedAnalysisView()
    
    print("🔍 Testing View with Mock Request:")
    print("-" * 40)
    
    try:
        response = view.get(request, file_id=file_id)
        print(f"   📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print("   ✅ API call successful")
            data = response.data
            print(f"   📋 Response keys: {list(data.keys())}")
        else:
            print(f"   ❌ API call failed with status: {response.status_code}")
            print(f"   📄 Response data: {response.data}")
            
    except Exception as e:
        print(f"   ❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    
    # Test with different file_id formats
    print("🔍 Testing Different File ID Formats:")
    print("-" * 40)
    
    # Test with UUID object
    try:
        response = view.get(request, file_id=data_file.id)
        print(f"   📊 UUID object - Status: {response.status_code}")
    except Exception as e:
        print(f"   ❌ UUID object - Error: {e}")
    
    # Test with string UUID
    try:
        response = view.get(request, file_id=str(data_file.id))
        print(f"   📊 String UUID - Status: {response.status_code}")
    except Exception as e:
        print(f"   ❌ String UUID - Error: {e}")
    
    # Test with file_id with trailing slash
    try:
        response = view.get(request, file_id=f"{file_id}/")
        print(f"   📊 With trailing slash - Status: {response.status_code}")
    except Exception as e:
        print(f"   ❌ With trailing slash - Error: {e}")
    
    print()
    
    # Test the exact query from the view
    print("🔍 Testing Exact Query from View:")
    print("-" * 40)
    
    # This is the exact query from the view
    backdated_results = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).order_by('-analysis_date').first()
    
    if backdated_results:
        print(f"   ✅ Query found result: {backdated_results.id}")
        print(f"   📅 Analysis date: {backdated_results.analysis_date}")
        print(f"   📊 Analysis type: {backdated_results.analysis_type}")
        print(f"   ✅ Status: {backdated_results.status}")
    else:
        print("   ❌ Query found no result")
        
        # Debug the query
        print("   🔍 Debugging query:")
        print(f"      - Data file exists: {DataFile.objects.filter(id=file_id).exists()}")
        print(f"      - Data file ID: {data_file.id}")
        print(f"      - Data file type: {type(data_file.id)}")
        
        # Check all backdated results
        all_backdated = BackdatedAnalysisResult.objects.all()
        print(f"      - Total backdated results: {all_backdated.count()}")
        
        for result in all_backdated:
            print(f"        * ID: {result.id}")
            print(f"          Data file: {result.data_file.id}")
            print(f"          Analysis type: {result.analysis_type}")
            print(f"          Status: {result.status}")
    
    print()
    print("✅ API call test completed!")

def main():
    """Main function"""
    test_api_call()
    
    print("\n" + "=" * 50)
    print("✅ Test completed!")

if __name__ == "__main__":
    main() 