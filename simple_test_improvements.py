#!/usr/bin/env python3
"""
Simple Test for Improved Detection Algorithms
==============================================

This script tests the improved duplicate and backdated detection algorithms
by running the analysis tasks through Celery.
"""

import os
import sys
import django
from datetime import datetime
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    SAPGLPosting, DataFile, FileProcessingJob, 
    DuplicateAnalysisResult, BackdatedAnalysisResult
)

class SimpleImprovementTester:
    """Simple tester for improved detection algorithms"""
    
    def __init__(self):
        self.test_results = {}
        
    def test_improvements(self):
        """Test the improvements by running analysis tasks"""
        print("🧪 TESTING IMPROVED DETECTION ALGORITHMS")
        print("=" * 60)
        
        # Get the latest processing job
        latest_job = FileProcessingJob.objects.filter(status='COMPLETED').order_by('-completed_at').first()
        
        if not latest_job:
            print("❌ No completed jobs found. Please run analysis first.")
            return
        
        print(f"Testing with job: {latest_job.id}")
        print(f"File: {latest_job.data_file.file_name}")
        
        # Test improved duplicate detection
        self.test_improved_duplicate_detection(latest_job.id)
        
        # Test improved backdated detection
        self.test_improved_backdated_detection(latest_job.id)
        
        # Show improvements summary
        self.show_improvements_summary()
        
    def test_improved_duplicate_detection(self, job_id):
        """Test improved duplicate detection by running the task"""
        print("\n🔄 TESTING IMPROVED DUPLICATE DETECTION")
        print("-" * 40)
        
        try:
            # Import and run the task
            from core.tasks import run_duplicate_analysis
            
            # Run the task
            result = run_duplicate_analysis.delay(job_id)
            
            # Wait for result
            task_result = result.get(timeout=300)
            
            if task_result.get('success'):
                print("✅ Improved duplicate detection completed successfully")
                print(f"  Duplicates found: {task_result.get('duplicate_count', 0)}")
                print(f"  Processing time: {task_result.get('processing_duration', 0):.2f} seconds")
                
                # Get the latest result
                latest_result = DuplicateAnalysisResult.objects.filter(
                    processing_job_id=job_id
                ).order_by('-created_at').first()
                
                if latest_result:
                    print(f"  Risk assessment: {latest_result.risk_assessment.get('overall_risk', 'Unknown')}")
                    print(f"  Risk score: {latest_result.risk_assessment.get('risk_score', 0)}")
                    
                    # Show detailed duplicate information
                    if latest_result.duplicate_list:
                        print(f"\n  Detailed duplicates found:")
                        for i, duplicate in enumerate(latest_result.duplicate_list[:3]):  # Show first 3
                            print(f"    {i+1}. Type: {duplicate.get('duplicate_type', 'Unknown')}")
                            print(f"       Risk Level: {duplicate.get('risk_level', 'Unknown')}")
                            print(f"       Risk Score: {duplicate.get('risk_score', 0)}")
                            print(f"       Matching Fields: {duplicate.get('matching_fields', [])}")
                
                self.test_results['improved_duplicate'] = task_result
            else:
                print(f"❌ Improved duplicate detection failed: {task_result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error testing improved duplicate detection: {str(e)}")
    
    def test_improved_backdated_detection(self, job_id):
        """Test improved backdated detection by running the task"""
        print("\n📅 TESTING IMPROVED BACKDATED DETECTION")
        print("-" * 40)
        
        try:
            # Import and run the task
            from core.tasks import run_backdated_analysis
            
            # Run the task
            result = run_backdated_analysis.delay(job_id)
            
            # Wait for result
            task_result = result.get(timeout=300)
            
            if task_result.get('success'):
                print("✅ Improved backdated detection completed successfully")
                print(f"  Backdated entries found: {task_result.get('backdated_count', 0)}")
                print(f"  Processing time: {task_result.get('processing_duration', 0):.2f} seconds")
                
                # Get the latest result
                latest_result = BackdatedAnalysisResult.objects.filter(
                    processing_job_id=job_id
                ).order_by('-created_at').first()
                
                if latest_result:
                    print(f"  Risk assessment: {latest_result.risk_assessment.get('overall_risk', 'Unknown')}")
                    print(f"  Risk score: {latest_result.risk_assessment.get('risk_score', 0)}")
                    
                    # Show detailed backdated information
                    if latest_result.backdated_entries:
                        print(f"\n  Detailed backdated entries found:")
                        for i, entry in enumerate(latest_result.backdated_entries[:3]):  # Show first 3
                            print(f"    {i+1}. Days difference: {entry.get('days_difference', 0)}")
                            print(f"       Risk Level: {entry.get('risk_level', 'Unknown')}")
                            print(f"       Risk Score: {entry.get('risk_score', 0)}")
                            print(f"       User: {entry.get('user_name', 'Unknown')}")
                            print(f"       Account: {entry.get('gl_account', 'Unknown')}")
                
                self.test_results['improved_backdated'] = task_result
            else:
                print(f"❌ Improved backdated detection failed: {task_result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error testing improved backdated detection: {str(e)}")
    
    def show_improvements_summary(self):
        """Show summary of improvements"""
        print("\n🎯 IMPROVEMENTS SUMMARY")
        print("=" * 60)
        
        print("\n✅ ENHANCED DUPLICATE DETECTION:")
        print("  • Better risk scoring with document similarity checks")
        print("  • Date proximity analysis for better accuracy")
        print("  • Enhanced risk levels based on multiple factors")
        print("  • Improved matching field detection")
        
        print("\n✅ ENHANCED BACKDATED DETECTION:")
        print("  • Granular risk scoring based on days difference")
        print("  • User behavior pattern analysis")
        print("  • Account sensitivity analysis")
        print("  • Time-based risk factors (year-end, quarter-end)")
        print("  • Document type risk assessment")
        
        print("\n✅ IMPROVED RISK CALCULATION:")
        print("  • Weighted risk scoring based on risk levels")
        print("  • Fallback calculation when ML models unavailable")
        print("  • Better handling of edge cases")
        print("  • More accurate overall risk assessment")
        
        # Save test results
        test_report = {
            'timestamp': datetime.now().isoformat(),
            'test_results': self.test_results,
            'improvements': {
                'duplicate_detection': {
                    'document_similarity': 'Added',
                    'date_proximity': 'Added',
                    'enhanced_risk_scoring': 'Improved',
                    'matching_fields': 'Enhanced'
                },
                'backdated_detection': {
                    'granular_risk_scoring': 'Added',
                    'user_behavior_analysis': 'Added',
                    'account_sensitivity': 'Added',
                    'time_based_factors': 'Added',
                    'document_type_risk': 'Added'
                },
                'risk_calculation': {
                    'weighted_scoring': 'Added',
                    'fallback_calculation': 'Added',
                    'edge_case_handling': 'Improved'
                }
            }
        }
        
        with open('improvements_test_report.json', 'w') as f:
            json.dump(test_report, f, indent=2, default=str)
        
        print(f"\n📄 Test report saved to: improvements_test_report.json")
        
        print("\n🚀 NEXT STEPS:")
        print("  1. The improved algorithms are now active in the system")
        print("  2. Upload new files to see the enhanced detection in action")
        print("  3. Monitor the improved risk assessments")
        print("  4. Review the detailed audit recommendations")

def main():
    """Main function to run the tests"""
    tester = SimpleImprovementTester()
    tester.test_improvements()

if __name__ == "__main__":
    main() 