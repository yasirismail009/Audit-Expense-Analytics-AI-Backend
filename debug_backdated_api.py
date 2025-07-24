#!/usr/bin/env python
"""
Debug script to test DatabaseStoredBackdatedAnalysisView logic
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, BackdatedAnalysisResult
from core.views import DatabaseStoredBackdatedAnalysisView

def debug_backdated_api():
    """Debug the DatabaseStoredBackdatedAnalysisView logic"""
    print("🔍 Debugging DatabaseStoredBackdatedAnalysisView")
    print("=" * 60)
    
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
    print(f"📈 Backdated Entries: {backdated_analysis.get_backdated_count()}")
    print()
    
    # Test the view logic directly
    view = DatabaseStoredBackdatedAnalysisView()
    
    print("🔍 Testing View Logic:")
    print("-" * 30)
    
    # Test the query that the view uses
    backdated_results = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).order_by('-analysis_date')
    
    print(f"   📊 Total backdated results found: {backdated_results.count()}")
    
    first_result = backdated_results.first()
    if first_result:
        print(f"   ✅ First result found: {first_result.id}")
        print(f"   📅 Analysis date: {first_result.analysis_date}")
        print(f"   📊 Analysis type: {first_result.analysis_type}")
        print(f"   ✅ Status: {first_result.status}")
    else:
        print("   ❌ No first result found")
    
    print()
    
    # Test the specific query from the view
    print("🔍 Testing Specific View Query:")
    print("-" * 30)
    
    # This is the exact query from the view
    backdated_results_view = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).order_by('-analysis_date').first()
    
    if backdated_results_view:
        print(f"   ✅ View query found result: {backdated_results_view.id}")
        print(f"   📅 Analysis date: {backdated_results_view.analysis_date}")
        print(f"   📊 Analysis type: {backdated_results_view.analysis_type}")
        print(f"   ✅ Status: {backdated_results_view.status}")
    else:
        print("   ❌ View query found no result")
        
        # Let's debug why
        print("   🔍 Debugging why no result found:")
        
        # Check if data_file exists
        print(f"      - Data file exists: {DataFile.objects.filter(id=file_id).exists()}")
        
        # Check all backdated results for this file
        all_backdated = BackdatedAnalysisResult.objects.filter(data_file=data_file)
        print(f"      - Total backdated results for file: {all_backdated.count()}")
        
        for result in all_backdated:
            print(f"        * ID: {result.id}")
            print(f"          Analysis type: {result.analysis_type}")
            print(f"          Status: {result.status}")
            print(f"          Analysis date: {result.analysis_date}")
    
    print()
    
    # Test the response generation
    if backdated_results_view:
        print("🔍 Testing Response Generation:")
        print("-" * 30)
        
        try:
            response = view._generate_backdated_analysis_response(data_file, backdated_results_view)
            print("   ✅ Response generated successfully")
            print(f"   📊 Response keys: {list(response.keys())}")
            
            # Check key sections
            if 'file_info' in response:
                print("   ✅ File info present")
            if 'analysis_info' in response:
                print("   ✅ Analysis info present")
            if 'backdated_list' in response:
                print("   ✅ Backdated list present")
            if 'breakdowns' in response:
                print("   ✅ Breakdowns present")
                
        except Exception as e:
            print(f"   ❌ Error generating response: {e}")
    
    print()
    print("🔍 Summary:")
    print("-" * 30)
    
    if backdated_results_view:
        print("   ✅ View logic should work correctly")
        print("   💡 The issue might be with the URL routing or request handling")
    else:
        print("   ❌ View logic found no results")
        print("   💡 Check the database query conditions")

def main():
    """Main function"""
    debug_backdated_api()
    
    print("\n" + "=" * 60)
    print("✅ Debug completed!")

if __name__ == "__main__":
    main() 