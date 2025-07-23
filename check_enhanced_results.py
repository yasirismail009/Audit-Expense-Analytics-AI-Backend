#!/usr/bin/env python
"""
Script to check enhanced duplicate analysis results
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from core.models import FileProcessingJob

def check_enhanced_results():
    """Check the results of enhanced duplicate analysis"""
    print("🔍 Checking Enhanced Duplicate Analysis Results...")
    
    # Get the most recent job
    recent_job = FileProcessingJob.objects.order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No jobs found")
        return
    
    print(f"📋 Job Details:")
    print(f"  ID: {str(recent_job.id)[:8]}...")
    print(f"  File: {recent_job.data_file.file_name if recent_job.data_file else 'No file'}")
    print(f"  Status: {recent_job.status}")
    print(f"  Created: {recent_job.created_at}")
    print(f"  Completed: {recent_job.completed_at}")
    
    if recent_job.anomaly_results:
        print(f"\n📊 Anomaly Results:")
        anomaly_results = recent_job.anomaly_results
        
        # Check if duplicate analysis is in anomaly results
        if 'duplicate_analysis' in anomaly_results:
            dup_analysis = anomaly_results['duplicate_analysis']
            print(f"  ✅ Duplicate analysis found in anomaly results")
            
            # Check analysis method
            analysis_method = dup_analysis.get('analysis_method', 'unknown')
            print(f"  📈 Analysis Method: {analysis_method}")
            
            # Check if enhanced analysis was used
            enhanced_used = dup_analysis.get('enhanced_analysis_used', False)
            print(f"  🔧 Enhanced Analysis Used: {enhanced_used}")
            
            # Check ML enhancement
            ml_enhancement = dup_analysis.get('ml_enhancement', {})
            ml_available = ml_enhancement.get('ml_model_available', False)
            ml_used = dup_analysis.get('ml_enhancement_used', False)
            print(f"  🤖 ML Model Available: {ml_available}")
            print(f"  🤖 ML Enhancement Used: {ml_used}")
            
            # Check analysis info
            analysis_info = dup_analysis.get('analysis_info', {})
            if analysis_info:
                print(f"\n📈 Analysis Info:")
                print(f"  Total Transactions: {analysis_info.get('total_transactions', 0)}")
                print(f"  Total Duplicate Groups: {analysis_info.get('total_duplicate_groups', 0)}")
                print(f"  Total Duplicate Transactions: {analysis_info.get('total_duplicate_transactions', 0)}")
                print(f"  Total Amount Involved: {analysis_info.get('total_amount_involved', 0)}")
            
            # Check breakdowns
            breakdowns = dup_analysis.get('breakdowns', {})
            if breakdowns:
                print(f"\n📊 Breakdowns Available:")
                for key in breakdowns.keys():
                    print(f"  - {key}")
                
                # Check duplicate types
                type_breakdown = breakdowns.get('type_breakdown', {})
                if type_breakdown:
                    print(f"\n🔍 Duplicate Types Found:")
                    for dup_type, count in type_breakdown.items():
                        print(f"  - {dup_type}: {count}")
            
            # Check chart data
            chart_data = dup_analysis.get('chart_data', {})
            if chart_data:
                print(f"\n📊 Chart Data Available:")
                for key in chart_data.keys():
                    print(f"  - {key}")
            
            # Check duplicate list
            duplicate_list = dup_analysis.get('duplicate_list', [])
            print(f"\n📋 Duplicate List:")
            print(f"  Total Duplicates: {len(duplicate_list)}")
            if duplicate_list:
                print(f"  Sample Duplicate Types:")
                types_found = set()
                for dup in duplicate_list[:5]:  # Show first 5
                    dup_type = dup.get('duplicate_type', 'Unknown')
                    types_found.add(dup_type)
                for dup_type in types_found:
                    print(f"    - {dup_type}")
        else:
            print(f"  ❌ No duplicate analysis found in anomaly results")
    else:
        print(f"  ❌ No anomaly results found")
    
    # Check if there are any error messages
    if recent_job.error_message:
        print(f"\n❌ Error Message: {recent_job.error_message}")

if __name__ == '__main__':
    check_enhanced_results() 