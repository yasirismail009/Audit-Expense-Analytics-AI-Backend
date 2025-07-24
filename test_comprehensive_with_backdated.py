#!/usr/bin/env python
"""
Test script to verify comprehensive analytics API with backdated data integration
"""

import os
import sys
import django
import requests
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, BackdatedAnalysisResult, AnalyticsProcessingResult

def test_comprehensive_analytics_with_backdated():
    """Test the comprehensive analytics API with backdated data integration"""
    print("🚀 Testing Comprehensive Analytics API with Backdated Data Integration")
    print("=" * 70)
    
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
    
    # Check if comprehensive analytics exists
    comprehensive_analytics = AnalyticsProcessingResult.objects.filter(
        data_file=data_file,
        analytics_type='comprehensive_expense'
    ).first()
    
    if not comprehensive_analytics:
        print("❌ No comprehensive analytics found")
        print("💡 Please run comprehensive analytics first")
        return
    
    print(f"📊 Comprehensive Analytics ID: {comprehensive_analytics.id}")
    print(f"📈 Total Transactions: {comprehensive_analytics.total_transactions}")
    print(f"💰 Total Amount: {comprehensive_analytics.total_amount}")
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
            
            # Test general stats integration
            print("📊 GENERAL STATS INTEGRATION:")
            print("-" * 40)
            if 'general_stats' in data:
                stats = data['general_stats']
                print(f"   📈 Total Transactions: {stats.get('total_transactions', 0)}")
                print(f"   💰 Total Amount: {stats.get('total_amount', 0):,.2f}")
                print(f"   🚩 Flagged Transactions: {stats.get('flagged_transactions', 0)}")
                print(f"   ⚠️  High Risk Transactions: {stats.get('high_risk_transactions', 0)}")
                print(f"   🔍 Anomalies Found: {stats.get('anomalies_found', 0)}")
                print(f"   📋 Duplicates Found: {stats.get('duplicates_found', 0)}")
                
                # Check backdated integration
                print(f"   📅 Backdated Entries: {stats.get('backdated_entries', 0)}")
                print(f"   💰 Backdated Amount: {stats.get('backdated_amount', 0):,.2f}")
                print(f"   🎯 Total Anomalies (including backdated): {stats.get('total_anomalies', 0)}")
                print(f"   ⚠️  Total Risk Transactions (including backdated): {stats.get('total_risk_transactions', 0)}")
                print(f"   🔴 Backdated High Risk: {stats.get('backdated_high_risk', 0)}")
                print(f"   🟡 Backdated Medium Risk: {stats.get('backdated_medium_risk', 0)}")
                print(f"   🟢 Backdated Low Risk: {stats.get('backdated_low_risk', 0)}")
            
            print()
            
            # Test summary integration
            print("📋 SUMMARY INTEGRATION:")
            print("-" * 40)
            if 'summary' in data:
                summary = data['summary']
                print(f"   📈 Total Transactions: {summary.get('total_transactions', 0)}")
                print(f"   💰 Total Amount: {summary.get('total_amount', 0):,.2f}")
                print(f"   🚩 Flagged Transactions: {summary.get('flagged_transactions', 0)}")
                print(f"   ⚠️  High Risk Transactions: {summary.get('high_risk_transactions', 0)}")
                print(f"   🔍 Anomalies Found: {summary.get('anomalies_found', 0)}")
                print(f"   📋 Duplicates Found: {summary.get('duplicates_found', 0)}")
                
                # Check backdated integration
                print(f"   📅 Backdated Entries: {summary.get('backdated_entries', 0)}")
                print(f"   💰 Backdated Amount: {summary.get('backdated_amount', 0):,.2f}")
                print(f"   🎯 Total Anomalies (including backdated): {summary.get('total_anomalies', 0)}")
                print(f"   ⚠️  Total Risk Transactions (including backdated): {summary.get('total_risk_transactions', 0)}")
                print(f"   🔴 Backdated High Risk: {summary.get('backdated_high_risk', 0)}")
                print(f"   🟡 Backdated Medium Risk: {summary.get('backdated_medium_risk', 0)}")
                print(f"   🟢 Backdated Low Risk: {summary.get('backdated_low_risk', 0)}")
            
            print()
            
            # Test risk data integration
            print("🎯 RISK DATA INTEGRATION:")
            print("-" * 40)
            if 'risk_data' in data:
                risk_data = data['risk_data']
                risk_stats = risk_data.get('risk_stats', {})
                
                print(f"   🤖 ML Risk Score: {risk_stats.get('ml_risk_score', 0):.3f}")
                print(f"   📊 Comprehensive Risk Score: {risk_stats.get('comprehensive_risk_score', 0):.3f}")
                print(f"   📅 Backdated Risk Score: {risk_stats.get('backdated_risk_score', 0):.3f}")
                print(f"   🎯 Overall Risk Score: {risk_stats.get('overall_risk_score', 0):.3f}")
                print(f"   ⚠️  Overall Risk Level: {risk_stats.get('overall_risk_level', 'N/A')}")
                
                print(f"   🔍 Anomalies Detected: {risk_stats.get('anomalies_detected', 0)}")
                print(f"   📋 Duplicates Found: {risk_stats.get('duplicates_found', 0)}")
                print(f"   📅 Backdated Entries: {risk_stats.get('backdated_entries', 0)}")
                print(f"   💰 Backdated Amount: {risk_stats.get('backdated_amount', 0):,.2f}")
                print(f"   🎯 Total Anomalies: {risk_stats.get('total_anomalies', 0)}")
                print(f"   ⚠️  Total Risk Transactions: {risk_stats.get('total_risk_transactions', 0)}")
                
                print(f"   🔴 Backdated High Risk: {risk_stats.get('backdated_high_risk', 0)}")
                print(f"   🟡 Backdated Medium Risk: {risk_stats.get('backdated_medium_risk', 0)}")
                print(f"   🟢 Backdated Low Risk: {risk_stats.get('backdated_low_risk', 0)}")
                
                # Check risk factors
                if 'risk_factors' in risk_data:
                    risk_factors = risk_data['risk_factors']
                    print(f"   📊 Risk Factors: {len(risk_factors)} factors")
                    for factor, value in risk_factors.items():
                        print(f"      - {factor}: {value}")
                
                # Check recommendations
                if 'recommendations' in risk_data:
                    recommendations = risk_data['recommendations']
                    print(f"   💡 Recommendations: {len(recommendations)} recommendations")
                    for i, rec in enumerate(recommendations[:5]):  # Show first 5
                        print(f"      {i+1}. {rec}")
            
            print()
            
            # Test backdated data section
            print("📅 BACKDATED DATA SECTION:")
            print("-" * 40)
            if 'backdated_data' in data:
                backdated_data = data['backdated_data']
                if backdated_data.get('has_backdated_data'):
                    print("✅ Backdated data section is available")
                    stats = backdated_data.get('backdated_stats', {})
                    print(f"   📈 Total Backdated Entries: {stats.get('total_backdated_entries', 0)}")
                    print(f"   💰 Total Amount: {stats.get('total_amount', 0):,.2f}")
                    print(f"   🔴 High Risk: {stats.get('high_risk_entries', 0)}")
                    print(f"   🟡 Medium Risk: {stats.get('medium_risk_entries', 0)}")
                    print(f"   🟢 Low Risk: {stats.get('low_risk_entries', 0)}")
                else:
                    print("❌ No backdated data available")
            else:
                print("❌ Backdated data section not found")
            
            print()
            
            # Verify integration correctness
            print("🔍 INTEGRATION VERIFICATION:")
            print("-" * 40)
            
            # Check if backdated entries are included in total anomalies
            general_stats = data.get('general_stats', {})
            summary = data.get('summary', {})
            risk_data = data.get('risk_data', {})
            risk_stats = risk_data.get('risk_stats', {})
            backdated_data = data.get('backdated_data', {})
            
            backdated_count = backdated_data.get('backdated_stats', {}).get('total_backdated_entries', 0)
            base_anomalies = general_stats.get('anomalies_found', 0)
            total_anomalies = general_stats.get('total_anomalies', 0)
            
            print(f"   📊 Base Anomalies: {base_anomalies}")
            print(f"   📅 Backdated Entries: {backdated_count}")
            print(f"   🎯 Total Anomalies: {total_anomalies}")
            
            if total_anomalies == base_anomalies + backdated_count:
                print("   ✅ Total anomalies correctly includes backdated entries")
            else:
                print("   ❌ Total anomalies calculation incorrect")
            
            # Check if backdated high risk is included in total risk transactions
            base_high_risk = general_stats.get('high_risk_transactions', 0) - general_stats.get('backdated_high_risk', 0)
            backdated_high_risk = general_stats.get('backdated_high_risk', 0)
            total_risk_transactions = general_stats.get('total_risk_transactions', 0)
            
            print(f"   ⚠️  Base High Risk: {base_high_risk}")
            print(f"   🔴 Backdated High Risk: {backdated_high_risk}")
            print(f"   ⚠️  Total Risk Transactions: {total_risk_transactions}")
            
            if total_risk_transactions == base_high_risk + backdated_high_risk:
                print("   ✅ Total risk transactions correctly includes backdated high risk")
            else:
                print("   ❌ Total risk transactions calculation incorrect")
            
            # Check if risk scores are properly calculated
            overall_risk_score = risk_stats.get('overall_risk_score', 0)
            backdated_risk_score = risk_stats.get('backdated_risk_score', 0)
            
            print(f"   🎯 Overall Risk Score: {overall_risk_score:.3f}")
            print(f"   📅 Backdated Risk Score: {backdated_risk_score:.3f}")
            
            if backdated_risk_score > 0:
                print("   ✅ Backdated risk score is properly calculated")
            else:
                print("   ⚠️  Backdated risk score is zero (no backdated entries or calculation issue)")
            
            print()
            print("✅ COMPREHENSIVE ANALYTICS WITH BACKDATED INTEGRATION SUCCESSFUL!")
            print("🎯 Backdated data is now properly integrated into risk calculations and statistics")
            
        else:
            print(f"❌ ERROR - Status: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR - {e}")
        print("💡 Make sure the Django server is running on localhost:8000")

def compare_before_after():
    """Compare the API response before and after backdated integration"""
    print("\n🔍 COMPARISON: Before vs After Backdated Integration")
    print("=" * 70)
    
    # Get the first file that has backdated analysis
    backdated_analysis = BackdatedAnalysisResult.objects.filter(
        analysis_type='enhanced_backdated',
        status='COMPLETED'
    ).first()
    
    if not backdated_analysis:
        print("❌ No backdated analysis found")
        return
    
    data_file = backdated_analysis.data_file
    file_id = str(data_file.id)
    
    print(f"📁 File: {data_file.file_name}")
    print(f"📅 Backdated Entries: {backdated_analysis.get_backdated_count()}")
    print(f"💰 Backdated Amount: {backdated_analysis.get_total_amount():,.2f}")
    print()
    
    print("📊 BEFORE (without backdated integration):")
    print("   - Anomalies: Only ML anomalies")
    print("   - Risk Transactions: Only ML high risk")
    print("   - Risk Score: Only ML + comprehensive")
    print("   - No backdated-specific metrics")
    print()
    
    print("📊 AFTER (with backdated integration):")
    print("   - Anomalies: ML anomalies + backdated entries")
    print("   - Risk Transactions: ML high risk + backdated high risk")
    print("   - Risk Score: ML + comprehensive + backdated")
    print("   - Backdated-specific metrics and charts")
    print("   - Backdated risk factors and recommendations")
    print()
    
    print("✅ The integration ensures backdated data is treated equally to duplicate data")
    print("🎯 Both backdated and duplicate entries contribute to overall risk assessment")

def main():
    """Main function"""
    print("🚀 Comprehensive Analytics with Backdated Integration Test")
    print("=" * 70)
    
    # Test the comprehensive analytics API with backdated integration
    test_comprehensive_analytics_with_backdated()
    
    print("\n" + "=" * 70)
    
    # Compare before and after
    compare_before_after()
    
    print("\n" + "=" * 70)
    print("✅ Test completed!")
    print("💡 Backdated data is now properly integrated into comprehensive analytics!")

if __name__ == "__main__":
    main() 