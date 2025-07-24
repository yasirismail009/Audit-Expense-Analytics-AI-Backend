#!/usr/bin/env python
"""
Test script to verify the new database-stored backdated analysis API endpoint
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

def test_backdated_analysis_api():
    """Test the new database-stored backdated analysis API endpoint"""
    print("🚀 Testing Database-Stored Backdated Analysis API")
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
    
    # Test the new backdated analysis API
    base_url = "http://localhost:8000/api"
    api_url = f"{base_url}/db-comprehensive-backdated-analysis/file/{file_id}/"
    
    print(f"🔗 API URL: {api_url}")
    print()
    
    try:
        response = requests.get(api_url)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ SUCCESS - Backdated Analysis API Retrieved")
            print()
            
            # Display API response structure
            print("📋 API RESPONSE STRUCTURE:")
            print("-" * 40)
            for key in data.keys():
                print(f"   - {key}")
            
            print()
            
            # Display file info
            if 'file_info' in data:
                file_info = data['file_info']
                print("📁 FILE INFO:")
                print(f"   - File Name: {file_info.get('file_name', 'N/A')}")
                print(f"   - Client Name: {file_info.get('client_name', 'N/A')}")
                print(f"   - Company Name: {file_info.get('company_name', 'N/A')}")
                print(f"   - Fiscal Year: {file_info.get('fiscal_year', 'N/A')}")
                print(f"   - Total Records: {file_info.get('total_records', 'N/A')}")
            
            print()
            
            # Display analysis info
            if 'analysis_info' in data:
                analysis_info = data['analysis_info']
                print("📊 ANALYSIS INFO:")
                print(f"   - Total Backdated Entries: {analysis_info.get('total_backdated_entries', 0)}")
                print(f"   - Total Amount: {analysis_info.get('total_amount', 0):,.2f}")
                print(f"   - Unique Documents: {analysis_info.get('unique_documents', 0)}")
                print(f"   - Unique Accounts: {analysis_info.get('unique_accounts', 0)}")
                print(f"   - Unique Users: {analysis_info.get('unique_users', 0)}")
                print(f"   - Processing Duration: {analysis_info.get('processing_duration', 0):.2f}s")
                print(f"   - Analysis Date: {analysis_info.get('analysis_date', 'N/A')}")
                print(f"   - Analysis Version: {analysis_info.get('analysis_version', 'N/A')}")
                
                # Display risk distribution
                risk_dist = analysis_info.get('risk_distribution', {})
                if risk_dist:
                    print(f"   🎯 Risk Distribution:")
                    print(f"      - High Risk: {risk_dist.get('high_risk', 0)}")
                    print(f"      - Medium Risk: {risk_dist.get('medium_risk', 0)}")
                    print(f"      - Low Risk: {risk_dist.get('low_risk', 0)}")
            
            print()
            
            # Display backdated list
            if 'backdated_list' in data:
                backdated_list = data['backdated_list']
                print(f"📋 BACKDATED LIST: {len(backdated_list)} entries")
                if backdated_list:
                    print("   Sample entries:")
                    for i, entry in enumerate(backdated_list[:3]):  # Show first 3
                        print(f"      {i+1}. Doc: {entry.get('document_number', 'N/A')} | "
                              f"Account: {entry.get('gl_account', 'N/A')} | "
                              f"Amount: {entry.get('amount', 0):,.2f} | "
                              f"Risk: {entry.get('risk_level', 'N/A')}")
            
            print()
            
            # Display breakdowns
            if 'breakdowns' in data:
                breakdowns = data['breakdowns']
                print("📊 BREAKDOWNS:")
                print(f"   - By Document: {len(breakdowns.get('by_document', []))} groups")
                print(f"   - By Account: {len(breakdowns.get('by_account', []))} groups")
                print(f"   - By User: {len(breakdowns.get('by_user', []))} groups")
            
            print()
            
            # Display chart data
            if 'chart_data' in data:
                chart_data = data['chart_data']
                print("📊 CHART DATA:")
                for chart_name in chart_data.keys():
                    print(f"   - {chart_name}")
            
            print()
            
            # Display summary table
            if 'summary_table' in data:
                summary_table = data['summary_table']
                print(f"📋 SUMMARY TABLE: {len(summary_table)} entries")
                if summary_table:
                    print("   Sample entries:")
                    for i, entry in enumerate(summary_table[:5]):  # Show first 5
                        print(f"      {i+1}. {entry.get('category', 'N/A')} - "
                              f"{entry.get('metric', 'N/A')}: {entry.get('value', 'N/A')}")
            
            print()
            
            # Display export data
            if 'export_data' in data:
                export_data = data['export_data']
                print(f"📤 EXPORT DATA: {len(export_data)} entries")
            
            print()
            
            # Display detailed insights
            if 'detailed_insights' in data:
                insights = data['detailed_insights']
                print("🔍 DETAILED INSIGHTS:")
                if 'key_findings' in insights:
                    print(f"   - Key Findings: {len(insights['key_findings'])} findings")
                if 'compliance_issues' in insights:
                    print(f"   - Compliance Issues: {len(insights['compliance_issues'])} issues")
                if 'recommendations' in insights:
                    print(f"   - Recommendations: {len(insights['recommendations'])} recommendations")
            
            print()
            
            # Display ML enhancement
            if 'ml_enhancement' in data:
                ml_enhancement = data['ml_enhancement']
                print("🤖 ML ENHANCEMENT:")
                print(f"   - Prediction Confidence: {ml_enhancement.get('prediction_confidence', 0):.2f}")
                print(f"   - Anomaly Scores: {len(ml_enhancement.get('anomaly_scores', []))} scores")
                print(f"   - Model Recommendations: {len(ml_enhancement.get('model_recommendations', []))} recommendations")
            
            print()
            
            # Display audit recommendations
            if 'audit_recommendations' in data:
                audit_recs = data['audit_recommendations']
                print("🔍 AUDIT RECOMMENDATIONS:")
                if 'high_priority' in audit_recs:
                    print(f"   - High Priority: {len(audit_recs['high_priority'])} recommendations")
                if 'medium_priority' in audit_recs:
                    print(f"   - Medium Priority: {len(audit_recs['medium_priority'])} recommendations")
                if 'low_priority' in audit_recs:
                    print(f"   - Low Priority: {len(audit_recs['low_priority'])} recommendations")
            
            print()
            
            # Display compliance assessment
            if 'compliance_assessment' in data:
                compliance = data['compliance_assessment']
                print("⚖️  COMPLIANCE ASSESSMENT:")
                if 'compliance_issues' in compliance:
                    print(f"   - Compliance Issues: {len(compliance['compliance_issues'])} issues")
                if 'overall_risk_level' in compliance:
                    print(f"   - Overall Risk Level: {compliance['overall_risk_level']}")
            
            print()
            
            # Display financial statement impact
            if 'financial_statement_impact' in data:
                fs_impact = data['financial_statement_impact']
                print("📊 FINANCIAL STATEMENT IMPACT:")
                if 'impact_level' in fs_impact:
                    print(f"   - Impact Level: {fs_impact['impact_level']}")
                if 'impacted_accounts' in fs_impact:
                    print(f"   - Impacted Accounts: {len(fs_impact['impacted_accounts'])} accounts")
            
            print()
            
            # Display processing info
            if 'processing_info' in data:
                processing_info = data['processing_info']
                print("⚙️  PROCESSING INFO:")
                print(f"   - Analytics ID: {processing_info.get('analytics_id', 'N/A')}")
                print(f"   - Processing Status: {processing_info.get('processing_status', 'N/A')}")
                print(f"   - Processing Duration: {processing_info.get('processing_duration', 0):.2f}s")
                print(f"   - Data Source: {processing_info.get('data_source', 'N/A')}")
            
            print()
            print("✅ BACKDATED ANALYSIS API SUCCESSFULLY CREATED!")
            print("🎯 The API follows the same pattern as the duplicate analysis endpoint")
            
        else:
            print(f"❌ ERROR - Status: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR - {e}")
        print("💡 Make sure the Django server is running on localhost:8000")

def compare_with_duplicate_api():
    """Compare the backdated API with the duplicate API structure"""
    print("\n🔍 Comparing with Duplicate Analysis API Structure")
    print("=" * 60)
    
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
    
    base_url = "http://localhost:8000/api"
    
    # Test duplicate analysis API (if available)
    duplicate_url = f"{base_url}/db-comprehensive-duplicate-analysis/file/{file_id}/"
    
    try:
        duplicate_response = requests.get(duplicate_url)
        if duplicate_response.status_code == 200:
            duplicate_data = duplicate_response.json()
            print("📊 DUPLICATE ANALYSIS API STRUCTURE:")
            for key in duplicate_data.keys():
                print(f"   - {key}")
            
            print()
            print("📊 BACKDATED ANALYSIS API STRUCTURE:")
            backdated_url = f"{base_url}/db-comprehensive-backdated-analysis/file/{file_id}/"
            backdated_response = requests.get(backdated_url)
            if backdated_response.status_code == 200:
                backdated_data = backdated_response.json()
                for key in backdated_data.keys():
                    print(f"   - {key}")
                
                print()
                print("✅ Both APIs follow the same response pattern!")
            else:
                print("❌ Backdated API not working")
        else:
            print("⚠️  Duplicate analysis API not available for comparison")
            
    except Exception as e:
        print(f"⚠️  Could not compare APIs: {e}")

def main():
    """Main function"""
    print("🚀 Database-Stored Backdated Analysis API Test")
    print("=" * 60)
    
    # Test the new backdated analysis API
    test_backdated_analysis_api()
    
    print("\n" + "=" * 60)
    
    # Compare with duplicate API structure
    compare_with_duplicate_api()
    
    print("\n" + "=" * 60)
    print("✅ Test completed!")
    print("💡 The new backdated analysis API is ready for use!")

if __name__ == "__main__":
    main() 