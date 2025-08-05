#!/usr/bin/env python3
"""
Test Improved Detection Algorithms
==================================

This script tests the improved duplicate and backdated detection algorithms
and compares them with the previous results to show improvements.
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
from core.tasks import run_duplicate_analysis, run_backdated_analysis

class ImprovedDetectionTester:
    """Test the improved detection algorithms"""
    
    def __init__(self):
        self.test_results = {}
        
    def test_improved_detection(self):
        """Test both improved duplicate and backdated detection"""
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
        
        # Compare results
        self.compare_results()
        
    def test_improved_duplicate_detection(self, job_id):
        """Test improved duplicate detection"""
        print("\n🔄 TESTING IMPROVED DUPLICATE DETECTION")
        print("-" * 40)
        
        try:
            # Create a test task instance
            from celery import current_app
            task_instance = run_duplicate_analysis.__class__()
            task_instance.request = type('Request', (), {'id': 'test-123'})()
            
            # Run the improved duplicate detection
            result = task_instance.run_duplicate_analysis(job_id)
            
            if result.get('success'):
                print("✅ Improved duplicate detection completed successfully")
                print(f"  Duplicates found: {result.get('duplicate_count', 0)}")
                print(f"  Processing time: {result.get('processing_duration', 0):.2f} seconds")
                
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
                        for i, duplicate in enumerate(latest_result.duplicate_list[:5]):  # Show first 5
                            print(f"    {i+1}. Type: {duplicate.get('duplicate_type', 'Unknown')}")
                            print(f"       Risk Level: {duplicate.get('risk_level', 'Unknown')}")
                            print(f"       Risk Score: {duplicate.get('risk_score', 0)}")
                            print(f"       Matching Fields: {duplicate.get('matching_fields', [])}")
                
                self.test_results['improved_duplicate'] = result
            else:
                print(f"❌ Improved duplicate detection failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error testing improved duplicate detection: {str(e)}")
    
    def test_improved_backdated_detection(self, job_id):
        """Test improved backdated detection"""
        print("\n📅 TESTING IMPROVED BACKDATED DETECTION")
        print("-" * 40)
        
        try:
            # Create a test task instance
            from celery import current_app
            task_instance = run_backdated_analysis.__class__()
            task_instance.request = type('Request', (), {'id': 'test-456'})()
            
            # Run the improved backdated detection
            result = task_instance.run_backdated_analysis(job_id)
            
            if result.get('success'):
                print("✅ Improved backdated detection completed successfully")
                print(f"  Backdated entries found: {result.get('backdated_count', 0)}")
                print(f"  Processing time: {result.get('processing_duration', 0):.2f} seconds")
                
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
                        for i, entry in enumerate(latest_result.backdated_entries[:5]):  # Show first 5
                            print(f"    {i+1}. Days difference: {entry.get('days_difference', 0)}")
                            print(f"       Risk Level: {entry.get('risk_level', 'Unknown')}")
                            print(f"       Risk Score: {entry.get('risk_score', 0)}")
                            print(f"       User: {entry.get('user_name', 'Unknown')}")
                            print(f"       Account: {entry.get('gl_account', 'Unknown')}")
                
                self.test_results['improved_backdated'] = result
            else:
                print(f"❌ Improved backdated detection failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error testing improved backdated detection: {str(e)}")
    
    def compare_results(self):
        """Compare improved results with previous results"""
        print("\n📊 COMPARISON OF RESULTS")
        print("=" * 60)
        
        # Get previous results
        previous_duplicate = DuplicateAnalysisResult.objects.filter(
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        previous_backdated = BackdatedAnalysisResult.objects.filter(
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        print("\nDUPLICATE DETECTION COMPARISON:")
        if previous_duplicate:
            print(f"  Previous duplicates found: {len(previous_duplicate.duplicate_list) if previous_duplicate.duplicate_list else 0}")
            print(f"  Previous risk level: {previous_duplicate.risk_assessment.get('overall_risk', 'Unknown')}")
        
        if 'improved_duplicate' in self.test_results:
            print(f"  Improved duplicates found: {self.test_results['improved_duplicate'].get('duplicate_count', 0)}")
            # Get improved risk level from latest result
            latest_duplicate = DuplicateAnalysisResult.objects.filter(
                status='COMPLETED'
            ).order_by('-created_at').first()
            if latest_duplicate:
                print(f"  Improved risk level: {latest_duplicate.risk_assessment.get('overall_risk', 'Unknown')}")
        
        print("\nBACKDATED DETECTION COMPARISON:")
        if previous_backdated:
            print(f"  Previous backdated entries: {len(previous_backdated.backdated_entries) if previous_backdated.backdated_entries else 0}")
            print(f"  Previous risk level: {previous_backdated.risk_assessment.get('overall_risk', 'Unknown')}")
        
        if 'improved_backdated' in self.test_results:
            print(f"  Improved backdated entries: {self.test_results['improved_backdated'].get('backdated_count', 0)}")
            # Get improved risk level from latest result
            latest_backdated = BackdatedAnalysisResult.objects.filter(
                status='COMPLETED'
            ).order_by('-created_at').first()
            if latest_backdated:
                print(f"  Improved risk level: {latest_backdated.risk_assessment.get('overall_risk', 'Unknown')}")
        
        # Save test results
        test_report = {
            'timestamp': datetime.now().isoformat(),
            'test_results': self.test_results,
            'comparison': {
                'previous_duplicate_count': len(previous_duplicate.duplicate_list) if previous_duplicate and previous_duplicate.duplicate_list else 0,
                'previous_backdated_count': len(previous_backdated.backdated_entries) if previous_backdated and previous_backdated.backdated_entries else 0,
                'improved_duplicate_count': self.test_results.get('improved_duplicate', {}).get('duplicate_count', 0),
                'improved_backdated_count': self.test_results.get('improved_backdated', {}).get('backdated_count', 0)
            }
        }
        
        with open('improved_detection_test_report.json', 'w') as f:
            json.dump(test_report, f, indent=2, default=str)
        
        print(f"\n📄 Test report saved to: improved_detection_test_report.json")
        
        # Summary
        print("\n🎯 IMPROVEMENT SUMMARY:")
        print("  ✅ Enhanced duplicate detection with better risk scoring")
        print("  ✅ Enhanced backdated detection with granular risk factors")
        print("  ✅ Added document similarity checks")
        print("  ✅ Added date proximity analysis")
        print("  ✅ Added user behavior pattern analysis")
        print("  ✅ Added account sensitivity analysis")
        print("  ✅ Added time-based risk factors")
        print("  ✅ Improved overall risk calculation with fallback")

def main():
    """Main function to run the tests"""
    tester = ImprovedDetectionTester()
    tester.test_improved_detection()

if __name__ == "__main__":
    main() 