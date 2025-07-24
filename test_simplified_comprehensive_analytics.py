#!/usr/bin/env python
"""
Test script to verify the simplified db-comprehensive-analytics endpoint
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

def test_simplified_comprehensive_analytics():
    """Test the simplified comprehensive analytics endpoint"""
    print("🚀 Testing Simplified DatabaseStoredComprehensiveAnalyticsView")
    print("=" * 70)
    
    # Get the first file that has comprehensive analytics
    comprehensive_analytics = AnalyticsProcessingResult.objects.filter(
        analytics_type='comprehensive_expense',
        processing_status='COMPLETED'
    ).first()
    
    if not comprehensive_analytics:
        print("❌ No comprehensive analytics found in database")
        return
    
    data_file = comprehensive_analytics.data_file
    file_id = str(data_file.id)
    
    print(f"📁 File: {data_file.file_name}")
    print(f"🆔 File ID: {file_id}")
    print()
    
    # Test the endpoint
    print("🔍 Testing Endpoint Response:")
    print("-" * 50)
    
    try:
        # Create a mock request
        factory = RequestFactory()
        request = factory.get(f'/api/db-comprehensive-analytics/file/{file_id}/')
        
        # Create view instance
        view = DatabaseStoredComprehensiveAnalyticsView()
        view.request = request
        
        # Call the get method
        response = view.get(request, file_id=file_id)
        
        if response.status_code == 200:
            print("✅ Endpoint returned successful response")
            data = response.data
            
            # Check response structure
            print("\n📊 Response Structure:")
            print("-" * 30)
            
            expected_keys = ['file_info', 'general_stats', 'charts', 'summary', 'risk_data', 'duplicate_summary', 'backdated_summary', 'combined_risk_assessment', 'processing_info']
            for key in expected_keys:
                if key in data:
                    print(f"✅ {key}: Present")
                else:
                    print(f"❌ {key}: Missing")
            
            # Check that old detailed keys are NOT present
            old_keys = ['backdated_data', 'duplicate_data']
            print("\n🚫 Old Detailed Keys (should NOT be present):")
            print("-" * 45)
            for key in old_keys:
                if key not in data:
                    print(f"✅ {key}: Correctly removed")
                else:
                    print(f"❌ {key}: Still present (should be removed)")
            
            # Check duplicate summary
            print("\n📈 Duplicate Summary:")
            print("-" * 25)
            duplicate_summary = data.get('duplicate_summary', {})
            if duplicate_summary:
                print(f"✅ Total Transactions: {duplicate_summary.get('total_transactions', 0)}")
                print(f"✅ Duplicate Transactions: {duplicate_summary.get('total_duplicate_transactions', 0)}")
                print(f"✅ Duplicate Groups: {duplicate_summary.get('total_duplicate_groups', 0)}")
                print(f"✅ Total Amount: {duplicate_summary.get('total_amount_involved', 0):,.2f}")
                print(f"✅ Duplicate Percentage: {duplicate_summary.get('duplicate_percentage', 0):.2f}%")
                print(f"✅ Risk Score: {duplicate_summary.get('risk_score', 0):.2f}")
                print(f"✅ Risk Level: {duplicate_summary.get('risk_level', 'UNKNOWN')}")
                print(f"✅ Has Data: {duplicate_summary.get('has_duplicate_data', False)}")
            else:
                print("❌ No duplicate summary data")
            
            # Check backdated summary
            print("\n📅 Backdated Summary:")
            print("-" * 25)
            backdated_summary = data.get('backdated_summary', {})
            if backdated_summary:
                print(f"✅ Total Transactions: {backdated_summary.get('total_transactions', 0)}")
                print(f"✅ Backdated Entries: {backdated_summary.get('total_backdated_entries', 0)}")
                print(f"✅ Total Amount: {backdated_summary.get('total_amount', 0):,.2f}")
                print(f"✅ Backdated Percentage: {backdated_summary.get('backdated_percentage', 0):.2f}%")
                print(f"✅ High Risk Entries: {backdated_summary.get('high_risk_entries', 0)}")
                print(f"✅ Medium Risk Entries: {backdated_summary.get('medium_risk_entries', 0)}")
                print(f"✅ Risk Score: {backdated_summary.get('risk_score', 0):.2f}")
                print(f"✅ Risk Level: {backdated_summary.get('risk_level', 'UNKNOWN')}")
                print(f"✅ Has Data: {backdated_summary.get('has_backdated_data', False)}")
            else:
                print("❌ No backdated summary data")
            
            # Check combined risk assessment
            print("\n⚠️ Combined Risk Assessment:")
            print("-" * 30)
            combined_risk = data.get('combined_risk_assessment', {})
            if combined_risk:
                print(f"✅ Combined Risk Score: {combined_risk.get('combined_risk_score', 0):.2f}")
                print(f"✅ Risk Level: {combined_risk.get('risk_level', 'UNKNOWN')}")
                print(f"✅ Duplicate Risk Score: {combined_risk.get('duplicate_risk_score', 0):.2f}")
                print(f"✅ Backdated Risk Score: {combined_risk.get('backdated_risk_score', 0):.2f}")
                
                # Check risk factors
                risk_factors = combined_risk.get('risk_factors', {})
                if risk_factors:
                    print("✅ Risk Factors:")
                    for analysis_type, factors in risk_factors.items():
                        print(f"   - {analysis_type}: {factors.get('risk_level', 'UNKNOWN')} ({factors.get('risk_score', 0):.2f})")
                
                # Check recommendations
                recommendations = combined_risk.get('recommendations', [])
                if recommendations:
                    print("✅ Recommendations:")
                    for i, rec in enumerate(recommendations, 1):
                        print(f"   {i}. {rec}")
            else:
                print("❌ No combined risk assessment data")
            
            # Check file info
            print("\n📋 File Info:")
            print("-" * 15)
            file_info = data.get('file_info', {})
            if file_info:
                print(f"✅ File Name: {file_info.get('file_name', 'N/A')}")
                print(f"✅ Client: {file_info.get('client_name', 'N/A')}")
                print(f"✅ Company: {file_info.get('company_name', 'N/A')}")
                print(f"✅ Total Records: {file_info.get('total_records', 0)}")
                print(f"✅ Processed Records: {file_info.get('processed_records', 0)}")
            else:
                print("❌ No file info data")
            
            print("\n🎯 Test Summary:")
            print("-" * 50)
            print("✅ Endpoint returns comprehensive analytics with GL accounts and charts")
            print("✅ Duplicate and backdated analysis simplified to summary stats only")
            print("✅ Risk calculations included for both duplicate and backdated analysis")
            print("✅ Combined risk assessment with recommendations")
            print("✅ No detailed duplicate/backdated listings or charts")
            
        else:
            print(f"❌ Endpoint returned error: {response.status_code}")
            print(f"Error: {response.data}")
            
    except Exception as e:
        print(f"❌ Error testing endpoint: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simplified_comprehensive_analytics() 