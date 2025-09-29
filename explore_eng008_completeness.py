#!/usr/bin/env python
"""
Comprehensive exploration of CompletenessTestResult fields and data for ENG-008
This script will examine all fields, relationships, and data stored in CompletenessTestResult
"""

import os
import sys
import django
import json
from datetime import datetime
from decimal import Decimal

# Setup Django environment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import CompletenessTestResult, Engagement, DataFile
from django.db import connection

def explore_completeness_test_result(engagement_id="ENG-008"):
    """
    Comprehensive exploration of CompletenessTestResult for a specific engagement
    """
    print(f"🔍 Exploring CompletenessTestResult for {engagement_id}")
    print("=" * 80)
    
    # Get all completeness test results for the engagement
    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id)
        print(f"✅ Found Engagement: {engagement.engagement_name}")
        print(f"   Client: {engagement.client.client_name}")
        print(f"   Fiscal Year: {engagement.fiscal_year}")
        print(f"   Status: {engagement.status}")
        print()
    except Engagement.DoesNotExist:
        print(f"❌ Engagement {engagement_id} not found")
        return
    
    # Get all completeness test results
    test_results = CompletenessTestResult.objects.filter(engagement=engagement).order_by('-test_timestamp')
    
    if not test_results.exists():
        print(f"❌ No completeness test results found for {engagement_id}")
        return
    
    print(f"📊 Found {test_results.count()} completeness test result(s)")
    print()
    
    # Explore each test result
    for i, result in enumerate(test_results, 1):
        print(f"📋 TEST RESULT #{i}")
        print("-" * 40)
        explore_single_test_result(result, i)
        print()

def explore_single_test_result(result, result_num):
    """
    Explore a single CompletenessTestResult in detail
    """
    print(f"🆔 ID: {result.id}")
    print(f"⏰ Test Timestamp: {result.test_timestamp}")
    print(f"🔢 Test Version: {result.test_version}")
    print(f"📊 Data Version: {result.data_version}")
    print(f"📝 Version Notes: {result.version_notes}")
    print()
    
    # Basic Results
    print("🎯 OVERALL RESULTS")
    print("-" * 20)
    print(f"Status: {result.overall_status}")
    print(f"Score: {result.completeness_score}%")
    print(f"Explanation: {result.overall_explanation}")
    print()
    
    # File Information
    print("📁 FILE INFORMATION")
    print("-" * 20)
    print(f"GL File: {result.gl_file.file_name if result.gl_file else 'None'}")
    print(f"TB File: {result.tb_file.file_name if result.tb_file else 'None'}")
    print(f"COA File: {result.coa_file.file_name if result.coa_file else 'None'}")
    print()
    
    # Document Verification Results
    print("📄 DOCUMENT VERIFICATION RESULTS")
    print("-" * 30)
    print(f"Total Documents: {result.total_documents}")
    print(f"Balanced Documents: {result.balanced_documents}")
    print(f"Unbalanced Documents: {result.unbalanced_documents}")
    print(f"Balance Rate: {result.document_balance_rate:.2%}")
    print(f"Total Variance: {result.total_document_variance}")
    print(f"Average Variance: {result.average_document_variance}")
    print(f"Transactions with Documents: {result.transactions_with_documents}")
    print(f"Transactions without Documents: {result.transactions_without_documents}")
    print(f"No Document Transaction Count: {result.no_document_transaction_count}")
    print()
    
    # Enhanced Statistics
    print("📈 ENHANCED STATISTICS")
    print("-" * 25)
    print(f"Total GL Records: {result.total_gl_records}")
    print(f"Total TB Records: {result.total_tb_records}")
    print(f"Total COA Records: {result.total_coa_records}")
    print(f"Total Accounts Unified: {result.total_accounts_unified}")
    print(f"Tests Passed: {result.tests_passed}")
    print(f"Total Tests: {result.total_tests}")
    print(f"Critical Issues Count: {result.critical_issues_count}")
    print(f"Processing Duration: {result.processing_duration}s")
    print()
    
    # Step Results
    print("🔍 STEP RESULTS")
    print("-" * 15)
    explore_step_results(result)
    print()
    
    # Comprehensive Statistics
    print("📊 COMPREHENSIVE STATISTICS")
    print("-" * 30)
    explore_comprehensive_statistics(result)
    print()
    
    # Database Fields
    print("🗄️ DATABASE FIELDS")
    print("-" * 20)
    explore_database_fields(result)
    print()

def explore_step_results(result):
    """
    Explore step-by-step test results
    """
    steps = [
        ('Step 1 - File Completeness', result.step1_file_completeness),
        ('Step 2 - GL-TB Reconciliation', result.step2_gl_tb_reconciliation),
        ('Step 3 - Debit-Credit Balance', result.step3_debit_credit_balance),
        ('Step 4 - Account Coverage', result.step4_account_coverage),
        ('Step 5 - COA Hierarchy Validation', result.step5_coa_hierarchy_validation),
        ('Step 6 - Account Linking', result.step6_account_linking),
        ('Step 7 - Transaction Gaps', result.step7_transaction_gaps),
    ]
    
    for step_name, step_data in steps:
        if step_data and isinstance(step_data, dict):
            print(f"{step_name}:")
            if 'passed' in step_data:
                print(f"  ✅ Passed: {step_data['passed']}")
            if 'description' in step_data:
                print(f"  📝 Description: {step_data['description']}")
            if 'explanation' in step_data:
                print(f"  💡 Explanation: {step_data['explanation']}")
            
            # Show key metrics for each step
            key_metrics = ['total_accounts_verified', 'accounts_passed', 'accounts_failed', 
                          'pass_rate', 'total_variance', 'account_verifications_count']
            for metric in key_metrics:
                if metric in step_data:
                    print(f"  📊 {metric}: {step_data[metric]}")
            
            # Show critical issues if any
            if 'critical_issues' in step_data and step_data['critical_issues']:
                print(f"  ⚠️ Critical Issues: {len(step_data['critical_issues'])}")
                for issue in step_data['critical_issues'][:3]:  # Show first 3
                    print(f"    - {issue}")
                if len(step_data['critical_issues']) > 3:
                    print(f"    ... and {len(step_data['critical_issues']) - 3} more")
        else:
            print(f"{step_name}: No data")

def explore_comprehensive_statistics(result):
    """
    Explore comprehensive statistics JSON field
    """
    if not result.comprehensive_statistics:
        print("No comprehensive statistics available")
        return
    
    stats = result.comprehensive_statistics
    
    # Document Statistics
    if 'document_statistics' in stats:
        doc_stats = stats['document_statistics']
        print("📄 Document Statistics:")
        for key, value in doc_stats.items():
            if isinstance(value, (int, float, str)):
                print(f"  {key}: {value}")
    
    # User Analysis
    if 'user_analysis' in stats:
        user_analysis = stats['user_analysis']
        print("👥 User Analysis:")
        for key, value in user_analysis.items():
            if isinstance(value, (int, float, str)):
                print(f"  {key}: {value}")
    
    # Monthly Trends
    if 'monthly_trends' in stats:
        monthly_trends = stats['monthly_trends']
        print("📅 Monthly Trends:")
        for key, value in monthly_trends.items():
            if isinstance(value, (int, float, str)):
                print(f"  {key}: {value}")
    
    # Document Verification Results
    if 'document_verification_results' in stats:
        doc_verifications = stats['document_verification_results']
        print(f"📋 Document Verification Results: {len(doc_verifications)} documents")
        if doc_verifications:
            # Show first few documents
            for i, doc in enumerate(doc_verifications[:3]):
                print(f"  Document {i+1}: {doc.get('document_number', 'N/A')} - "
                      f"Balanced: {doc.get('is_balanced', 'N/A')} - "
                      f"Net Balance: {doc.get('net_balance', 'N/A')}")
            if len(doc_verifications) > 3:
                print(f"  ... and {len(doc_verifications) - 3} more documents")
    
    # Show other top-level keys
    other_keys = [k for k in stats.keys() if k not in ['document_statistics', 'user_analysis', 
                                                       'monthly_trends', 'document_verification_results']]
    if other_keys:
        print("🔍 Other Statistics:")
        for key in other_keys:
            value = stats[key]
            if isinstance(value, (int, float, str, bool)):
                print(f"  {key}: {value}")
            elif isinstance(value, (list, dict)):
                print(f"  {key}: {type(value).__name__} with {len(value)} items")

def explore_database_fields(result):
    """
    Explore all database fields and their values
    """
    # Get all field names and values
    fields_info = []
    
    for field in result._meta.fields:
        field_name = field.name
        field_value = getattr(result, field_name)
        field_type = field.__class__.__name__
        
        # Format the value for display
        if field_value is None:
            display_value = "None"
        elif isinstance(field_value, Decimal):
            display_value = str(field_value)
        elif isinstance(field_value, datetime):
            display_value = field_value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(field_value, (list, dict)):
            display_value = f"{type(field_value).__name__} ({len(field_value)} items)"
        else:
            display_value = str(field_value)
        
        fields_info.append({
            'name': field_name,
            'type': field_type,
            'value': display_value,
            'help_text': getattr(field, 'help_text', '')
        })
    
    # Display fields in categories
    categories = {
        'Basic Info': ['id', 'engagement', 'test_timestamp', 'test_version'],
        'Version Control': ['data_version', 'version_notes'],
        'Files': ['gl_file', 'tb_file', 'coa_file'],
        'Overall Results': ['overall_status', 'overall_explanation', 'completeness_score'],
        'Step Results': ['step1_file_completeness', 'step2_gl_tb_reconciliation', 
                        'step3_debit_credit_balance', 'step4_account_coverage',
                        'step5_coa_hierarchy_validation', 'step6_account_linking', 
                        'step7_transaction_gaps'],
        'Document Verification': ['total_documents', 'balanced_documents', 'unbalanced_documents',
                               'document_balance_rate', 'total_document_variance', 
                               'average_document_variance', 'transactions_with_documents',
                               'transactions_without_documents', 'no_document_transaction_count'],
        'Statistics': ['total_gl_records', 'total_tb_records', 'total_coa_records',
                      'total_accounts_unified', 'tests_passed', 'total_tests',
                      'critical_issues_count', 'comprehensive_statistics', 'processing_duration'],
        'Timestamps': ['created_at', 'updated_at']
    }
    
    for category, field_names in categories.items():
        print(f"{category}:")
        for field_info in fields_info:
            if field_info['name'] in field_names:
                print(f"  {field_info['name']} ({field_info['type']}): {field_info['value']}")
                if field_info['help_text']:
                    print(f"    Help: {field_info['help_text']}")
        print()

def explore_relationships(result):
    """
    Explore relationships and related objects
    """
    print("🔗 RELATIONSHIPS")
    print("-" * 15)
    
    # Engagement relationship
    if result.engagement:
        print(f"Engagement: {result.engagement.engagement_name}")
        print(f"  Client: {result.engagement.client.client_name}")
        print(f"  Fiscal Year: {result.engagement.fiscal_year}")
    
    # File relationships
    if result.gl_file:
        print(f"GL File: {result.gl_file.file_name}")
        print(f"  Status: {result.gl_file.status}")
        print(f"  Records: {result.gl_file.sapglposting_set.count()}")
    
    if result.tb_file:
        print(f"TB File: {result.tb_file.file_name}")
        print(f"  Status: {result.tb_file.status}")
        print(f"  Records: {result.tb_file.trialbalance_set.count()}")
    
    if result.coa_file:
        print(f"COA File: {result.coa_file.file_name}")
        print(f"  Status: {result.coa_file.status}")
        print(f"  Records: {result.coa_file.chartofaccount_set.count()}")

def main():
    """
    Main exploration function
    """
    print("🚀 Starting CompletenessTestResult Exploration for ENG-008")
    print("=" * 80)
    
    try:
        explore_completeness_test_result("ENG-008")
    except Exception as e:
        print(f"❌ Error during exploration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
