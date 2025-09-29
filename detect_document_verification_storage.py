#!/usr/bin/env python
"""
Detect where document verification data is stored in CompletenessTestResult
This script will explore all possible locations where document verification data might be saved
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

from core.models import CompletenessTestResult, Engagement

def detect_document_verification_storage(engagement_id="ENG-008"):
    """
    Detect where document verification data is stored in CompletenessTestResult
    """
    print(f"🔍 Detecting Document Verification Data Storage for {engagement_id}")
    print("=" * 80)
    
    try:
        # Get the latest completeness test result
        result = CompletenessTestResult.objects.filter(
            engagement__engagement_id=engagement_id
        ).order_by('-test_timestamp').first()
        
        if not result:
            print(f"❌ No completeness test results found for {engagement_id}")
            return
        
        print(f"✅ Found CompletenessTestResult: {result.id}")
        print(f"📅 Test Date: {result.test_timestamp}")
        print()
        
        # 1. Check dedicated document verification fields
        print("📄 DEDICATED DOCUMENT VERIFICATION FIELDS")
        print("-" * 50)
        check_dedicated_fields(result)
        print()
        
        # 2. Check comprehensive_statistics JSONField
        print("📊 COMPREHENSIVE_STATISTICS JSONFIELD")
        print("-" * 45)
        check_comprehensive_statistics(result)
        print()
        
        # 3. Check step results for document verification data
        print("🔍 STEP RESULTS FOR DOCUMENT DATA")
        print("-" * 40)
        check_step_results(result)
        print()
        
        # 4. Check all JSON fields for document verification data
        print("🗄️ ALL JSON FIELDS ANALYSIS")
        print("-" * 30)
        check_all_json_fields(result)
        print()
        
        # 5. Search for document verification patterns
        print("🔎 DOCUMENT VERIFICATION PATTERN SEARCH")
        print("-" * 45)
        search_document_patterns(result)
        print()
        
    except Exception as e:
        print(f"❌ Error during detection: {e}")
        import traceback
        traceback.print_exc()

def check_dedicated_fields(result):
    """
    Check dedicated document verification fields
    """
    dedicated_fields = [
        'total_documents', 'balanced_documents', 'unbalanced_documents',
        'document_balance_rate', 'total_document_variance', 'average_document_variance',
        'transactions_with_documents', 'transactions_without_documents', 'no_document_transaction_count'
    ]
    
    print("Dedicated Document Verification Fields:")
    for field in dedicated_fields:
        value = getattr(result, field, None)
        print(f"  {field}: {value}")
    
    # Check if these fields have meaningful data
    has_data = any(getattr(result, field, 0) > 0 for field in ['total_documents', 'balanced_documents', 'unbalanced_documents'])
    print(f"\n✅ Dedicated fields have data: {has_data}")

def check_comprehensive_statistics(result):
    """
    Check comprehensive_statistics JSONField for document verification data
    """
    if not result.comprehensive_statistics:
        print("❌ comprehensive_statistics is empty or None")
        return
    
    stats = result.comprehensive_statistics
    print(f"📊 comprehensive_statistics type: {type(stats)}")
    print(f"📊 comprehensive_statistics keys: {list(stats.keys()) if isinstance(stats, dict) else 'Not a dict'}")
    
    # Check for document verification results
    if isinstance(stats, dict):
        # Check top-level keys
        doc_keys = [k for k in stats.keys() if 'document' in k.lower()]
        if doc_keys:
            print(f"📄 Document-related keys: {doc_keys}")
        
        # Check for document_verification_results
        if 'document_verification_results' in stats:
            doc_results = stats['document_verification_results']
            print(f"✅ Found document_verification_results: {len(doc_results)} documents")
            if doc_results and len(doc_results) > 0:
                print(f"   First document: {doc_results[0] if isinstance(doc_results, list) else 'Not a list'}")
        else:
            print("❌ document_verification_results not found at top level")
        
        # Check document_statistics
        if 'document_statistics' in stats:
            doc_stats = stats['document_statistics']
            print(f"📊 document_statistics: {type(doc_stats)}")
            if isinstance(doc_stats, dict):
                print(f"   Keys: {list(doc_stats.keys())}")
                if 'document_verification_results' in doc_stats:
                    doc_results = doc_stats['document_verification_results']
                    print(f"✅ Found document_verification_results in document_statistics: {len(doc_results)} documents")
                else:
                    print("❌ document_verification_results not found in document_statistics")
        
        # Check enhanced_statistics
        if 'enhanced_statistics' in stats:
            enhanced_stats = stats['enhanced_statistics']
            print(f"📈 enhanced_statistics: {type(enhanced_stats)}")
            if isinstance(enhanced_stats, dict):
                print(f"   Keys: {list(enhanced_stats.keys())}")
                if 'document_verification_results' in enhanced_stats:
                    doc_results = enhanced_stats['document_verification_results']
                    print(f"✅ Found document_verification_results in enhanced_statistics: {len(doc_results)} documents")

def check_step_results(result):
    """
    Check step results for document verification data
    """
    steps = [
        ('step1_file_completeness', result.step1_file_completeness),
        ('step2_gl_tb_reconciliation', result.step2_gl_tb_reconciliation),
        ('step3_debit_credit_balance', result.step3_debit_credit_balance),
        ('step4_account_coverage', result.step4_account_coverage),
        ('step5_coa_hierarchy_validation', result.step5_coa_hierarchy_validation),
        ('step6_account_linking', result.step6_account_linking),
        ('step7_transaction_gaps', result.step7_transaction_gaps),
    ]
    
    for step_name, step_data in steps:
        if step_data and isinstance(step_data, dict):
            # Look for document-related keys
            doc_keys = [k for k in step_data.keys() if 'document' in k.lower()]
            if doc_keys:
                print(f"📄 {step_name} has document keys: {doc_keys}")
            
            # Look for document_verification_results
            if 'document_verification_results' in step_data:
                doc_results = step_data['document_verification_results']
                print(f"✅ {step_name} contains document_verification_results: {len(doc_results)} documents")
            
            # Look for document analysis
            if 'document_analysis' in step_data:
                doc_analysis = step_data['document_analysis']
                print(f"📊 {step_name} contains document_analysis: {type(doc_analysis)}")
            
            # Look for document verification
            if 'document_verification' in step_data:
                doc_verification = step_data['document_verification']
                print(f"🔍 {step_name} contains document_verification: {type(doc_verification)}")

def check_all_json_fields(result):
    """
    Check all JSON fields for document verification data
    """
    json_fields = [
        'step1_file_completeness', 'step2_gl_tb_reconciliation',
        'step3_debit_credit_balance', 'step4_account_coverage',
        'step5_coa_hierarchy_validation', 'step6_account_linking',
        'step7_transaction_gaps', 'comprehensive_statistics'
    ]
    
    for field_name in json_fields:
        field_data = getattr(result, field_name, None)
        if field_data and isinstance(field_data, dict):
            # Search for document-related content
            doc_content = search_for_document_content(field_data, field_name)
            if doc_content:
                print(f"📄 {field_name}: {doc_content}")

def search_for_document_content(data, field_name, path=""):
    """
    Recursively search for document-related content in JSON data
    """
    findings = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            
            # Check if key contains document-related terms
            if any(term in key.lower() for term in ['document', 'verification', 'balance', 'variance']):
                if isinstance(value, list) and len(value) > 0:
                    findings.append(f"{current_path}: list with {len(value)} items")
                elif isinstance(value, dict):
                    findings.append(f"{current_path}: dict with {len(value)} keys")
                else:
                    findings.append(f"{current_path}: {value}")
            
            # Recursively search nested structures
            if isinstance(value, (dict, list)):
                nested_findings = search_for_document_content(value, field_name, current_path)
                findings.extend(nested_findings)
    
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, (dict, list)):
                nested_findings = search_for_document_content(item, field_name, f"{path}[{i}]")
                findings.extend(nested_findings)
    
    return findings

def search_document_patterns(result):
    """
    Search for specific document verification patterns
    """
    patterns_to_find = [
        'document_verification_results',
        'document_analysis',
        'document_statistics',
        'document_balance',
        'document_variance',
        'balanced_documents',
        'unbalanced_documents'
    ]
    
    print("Searching for document verification patterns...")
    
    # Search in comprehensive_statistics
    if result.comprehensive_statistics:
        found_patterns = search_patterns_in_data(result.comprehensive_statistics, patterns_to_find, "comprehensive_statistics")
        if found_patterns:
            print("📊 Found in comprehensive_statistics:")
            for pattern in found_patterns:
                print(f"   {pattern}")
    
    # Search in step results
    step_fields = [
        'step1_file_completeness', 'step2_gl_tb_reconciliation',
        'step3_debit_credit_balance', 'step4_account_coverage',
        'step5_coa_hierarchy_validation', 'step6_account_linking',
        'step7_transaction_gaps'
    ]
    
    for step_field in step_fields:
        step_data = getattr(result, step_field, None)
        if step_data:
            found_patterns = search_patterns_in_data(step_data, patterns_to_find, step_field)
            if found_patterns:
                print(f"🔍 Found in {step_field}:")
                for pattern in found_patterns:
                    print(f"   {pattern}")

def search_patterns_in_data(data, patterns, field_name, path=""):
    """
    Search for specific patterns in JSON data
    """
    findings = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            
            # Check if key matches any pattern
            for pattern in patterns:
                if pattern in key.lower():
                    if isinstance(value, list):
                        findings.append(f"{current_path}: {pattern} (list with {len(value)} items)")
                    elif isinstance(value, dict):
                        findings.append(f"{current_path}: {pattern} (dict with {len(value)} keys)")
                    else:
                        findings.append(f"{current_path}: {pattern} = {value}")
            
            # Recursively search
            if isinstance(value, (dict, list)):
                nested_findings = search_patterns_in_data(value, patterns, field_name, current_path)
                findings.extend(nested_findings)
    
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, (dict, list)):
                nested_findings = search_patterns_in_data(item, patterns, field_name, f"{path}[{i}]")
                findings.extend(nested_findings)
    
    return findings

def main():
    """
    Main detection function
    """
    print("🚀 Starting Document Verification Data Storage Detection")
    print("=" * 80)
    
    try:
        detect_document_verification_storage("ENG-008")
    except Exception as e:
        print(f"❌ Error during detection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
