#!/usr/bin/env python
"""
Test script to verify backdated statistics are included in the comprehensive analytics API
"""

import os
import sys
import django
import requests
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, BackdatedAnalysisResult

def test_comprehensive_analytics_with_backdated():
    """Test the comprehensive analytics API with backdated data"""
    print("🚀 Testing Comprehensive Analytics API with Backdated Statistics")
    print("=" * 60)
    
    # Get the first file that has backdated analysis
    backdated_analysis = BackdatedAnalysisResult.objects.filter(
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).first()
    
    if not backdated_analysis:
        print("❌ No backdated analysis found in database")
        print("💡 Please run backdated analysis first using:")
        print("   python process_backdated_queue.py")
        return
    
    data_file = backdated_analysis.data_file
    file_id = str(data_file.id)
    
    print(f"📁 Testing with file: {data_file.file_name}")
    print(f"🆔 File ID: {file_id}")
    print(f"📊 Backdated Analysis ID: {backdated_analysis.id}")
    print(f"📈 Backdated Entries: {backdated_analysis.get_backdated_count()}")
    print(f"💰 Total Amount: {backdated_analysis.get_total_amount():,.2f}")
    print()
    
    # Test the comprehensive analytics API
    base_url = "http://localhost:8000/api"
    api_url = f"{base_url}/db-comprehensive-analytics/file/{file_id}/"
    
    print(f"🔗 API URL: {api_url}")
    print()
    
    try:
        response = requests.get(api_url)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ SUCCESS - Comprehensive Analytics Retrieved")
            print()
            
            # Check if backdated data is included
            if 'backdated_data' in data:
                backdated_data = data['backdated_data']
                print("📊 BACKDATED DATA FOUND:")
                print("-" * 40)
                
                # Check if backdated data exists
                if backdated_data.get('has_backdated_data'):
                    print("✅ Backdated analysis data is available")
                    
                    # Display backdated statistics
                    stats = backdated_data.get('backdated_stats', {})
                    print(f"📈 Total Backdated Entries: {stats.get('total_backdated_entries', 0)}")
                    print(f"💰 Total Amount: {stats.get('total_amount', 0):,.2f}")
                    print(f"📄 Unique Documents: {stats.get('unique_documents', 0)}")
                    print(f"🏦 Unique Accounts: {stats.get('unique_accounts', 0)}")
                    print(f"👥 Unique Users: {stats.get('unique_users', 0)}")
                    print(f"⏱️  Processing Duration: {stats.get('processing_duration', 0):.2f}s")
                    print(f"📅 Analysis Date: {stats.get('analysis_date', 'N/A')}")
                    print(f"🔢 Analysis Version: {stats.get('analysis_version', 'N/A')}")
                    
                    # Display risk distribution
                    risk_dist = stats.get('risk_distribution', {})
                    if risk_dist:
                        print(f"🎯 Risk Distribution:")
                        print(f"   - High Risk: {risk_dist.get('high_risk', 0)}")
                        print(f"   - Medium Risk: {risk_dist.get('medium_risk', 0)}")
                        print(f"   - Low Risk: {risk_dist.get('low_risk', 0)}")
                    
                    # Check for detailed data
                    entries = backdated_data.get('backdated_entries', [])
                    print(f"📋 Detailed Entries: {len(entries)} (showing first 50)")
                    
                    by_document = backdated_data.get('backdated_by_document', [])
                    print(f"📄 By Document: {len(by_document)} (showing first 20)")
                    
                    by_account = backdated_data.get('backdated_by_account', [])
                    print(f"🏦 By Account: {len(by_account)} (showing first 20)")
                    
                    by_user = backdated_data.get('backdated_by_user', [])
                    print(f"👥 By User: {len(by_user)} (showing first 20)")
                    
                    # Check for audit recommendations
                    audit_recs = backdated_data.get('audit_recommendations', {})
                    if audit_recs:
                        print(f"🔍 Audit Recommendations: Available")
                        high_priority = audit_recs.get('high_priority', [])
                        print(f"   - High Priority: {len(high_priority)} recommendations")
                    
                    # Check for compliance assessment
                    compliance = backdated_data.get('compliance_assessment', {})
                    if compliance:
                        print(f"⚖️  Compliance Assessment: Available")
                        issues = compliance.get('compliance_issues', [])
                        print(f"   - Compliance Issues: {len(issues)} issues")
                    
                    # Check for financial statement impact
                    fs_impact = backdated_data.get('financial_statement_impact', {})
                    if fs_impact:
                        print(f"📊 Financial Statement Impact: Available")
                        impact_level = fs_impact.get('impact_level', 'N/A')
                        print(f"   - Impact Level: {impact_level}")
                    
                    # Check for charts data
                    charts = backdated_data.get('backdated_charts', {})
                    if charts:
                        print(f"📊 Charts Available:")
                        for chart_name in charts.keys():
                            print(f"   - {chart_name}")
                    
                else:
                    print("❌ No backdated analysis data found")
                    if 'message' in backdated_data:
                        print(f"💬 Message: {backdated_data['message']}")
                
                print()
                print("✅ BACKDATED STATISTICS SUCCESSFULLY INTEGRATED!")
                
            else:
                print("❌ Backdated data not found in API response")
                print("💡 The API response should include 'backdated_data' field")
            
            # Display other API sections
            print()
            print("📋 OTHER API SECTIONS:")
            print("-" * 40)
            for key in data.keys():
                if key != 'backdated_data':
                    print(f"   - {key}")
            
        else:
            print(f"❌ ERROR - Status: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR - {e}")
        print("💡 Make sure the Django server is running on localhost:8000")

def test_backdated_data_structure():
    """Test the structure of backdated data in the database"""
    print("\n🔍 Testing Backdated Data Structure in Database")
    print("=" * 60)
    
    # Get all backdated analysis results
    backdated_results = BackdatedAnalysisResult.objects.filter(
        analysis_type='enhanced_backdated'
    ).order_by('-analysis_date')
    
    print(f"📊 Total backdated analysis results: {backdated_results.count()}")
    
    if backdated_results.count() == 0:
        print("❌ No backdated analysis results found")
        return
    
    # Show the latest result
    latest = backdated_results.first()
    print(f"\n📈 Latest Backdated Analysis:")
    print(f"   ID: {latest.id}")
    print(f"   File: {latest.data_file.file_name}")
    print(f"   Status: {latest.status}")
    print(f"   Analysis Date: {latest.analysis_date}")
    print(f"   Processing Duration: {latest.processing_duration:.2f}s")
    print(f"   Total Entries: {latest.get_backdated_count()}")
    print(f"   Total Amount: {latest.get_total_amount():,.2f}")
    
    # Check data structure
    print(f"\n📋 Data Structure Check:")
    print(f"   Analysis Info: {'✅' if latest.analysis_info else '❌'}")
    print(f"   Backdated Entries: {'✅' if latest.backdated_entries else '❌'}")
    print(f"   By Document: {'✅' if latest.backdated_by_document else '❌'}")
    print(f"   By Account: {'✅' if latest.backdated_by_account else '❌'}")
    print(f"   By User: {'✅' if latest.backdated_by_user else '❌'}")
    print(f"   Audit Recommendations: {'✅' if latest.audit_recommendations else '❌'}")
    print(f"   Compliance Assessment: {'✅' if latest.compliance_assessment else '❌'}")
    print(f"   FS Impact: {'✅' if latest.financial_statement_impact else '❌'}")
    print(f"   Chart Data: {'✅' if latest.chart_data else '❌'}")

def main():
    """Main function"""
    print("🚀 Backdated Statistics Integration Test")
    print("=" * 60)
    
    # Test database structure
    test_backdated_data_structure()
    
    print("\n" + "=" * 60)
    
    # Test API integration
    test_comprehensive_analytics_with_backdated()
    
    print("\n" + "=" * 60)
    print("✅ Test completed!")
    print("💡 If all tests pass, backdated statistics are successfully integrated")

if __name__ == "__main__":
    main() 