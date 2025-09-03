#!/usr/bin/env python3
"""
Simple database flush script for Docker environment
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from django.db import connection
from core.models import DataFile, FileProcessingJob, SAPGLPosting

def flush_database():
    """Flush all data from the database"""
    print("🗑️  FLUSHING DATABASE")
    print("=" * 30)
    
    try:
        # Show current counts
        print(f"DataFiles: {DataFile.objects.count()}")
        print(f"FileProcessingJobs: {FileProcessingJob.objects.count()}")
        print(f"SAPGLPostings: {SAPGLPosting.objects.count()}")
        
        # Delete using raw SQL with correct table names
        with connection.cursor() as cursor:
            # Delete in correct order to avoid foreign key issues
            cursor.execute("DELETE FROM file_processing_jobs;")
            cursor.execute("DELETE FROM data_files;")
            cursor.execute("DELETE FROM sap_gl_postings;")
            
            # Delete analysis results
            cursor.execute("DELETE FROM general_analysis_results;")
            cursor.execute("DELETE FROM duplicate_analysis_results;")
            cursor.execute("DELETE FROM backdated_analysis_results;")
            cursor.execute("DELETE FROM user_analysis_results;")
            cursor.execute("DELETE FROM closing_entries_analysis_results;")
            cursor.execute("DELETE FROM unusual_days_analysis_results;")
            cursor.execute("DELETE FROM holiday_analysis_results;")
            cursor.execute("DELETE FROM overall_analysis_results;")
            cursor.execute("DELETE FROM risk_scoring_documents;")
            
            # Delete ML training data
            cursor.execute("DELETE FROM ml_model_training;")
            cursor.execute("DELETE FROM rule_based_model_training;")
            cursor.execute("DELETE FROM duplicate_analysis_model_training;")
            cursor.execute("DELETE FROM backdated_analysis_model_training;")
            cursor.execute("DELETE FROM user_analysis_model_training;")
            cursor.execute("DELETE FROM unusual_days_analysis_model_training;")
            cursor.execute("DELETE FROM closing_entries_analysis_model_training;")
            cursor.execute("DELETE FROM holiday_analysis_model_training;")
            cursor.execute("DELETE FROM overall_risk_analysis_model_training;")
            cursor.execute("DELETE FROM manual_entry_analysis_model_training;")
            
            # Delete other tables
            cursor.execute("DELETE FROM manual_entry_analysis_results;")
            cursor.execute("DELETE FROM analytics_processing_results;")
            cursor.execute("DELETE FROM processing_job_trackers;")
            cursor.execute("DELETE FROM transaction_analyses;")
            cursor.execute("DELETE FROM analysis_sessions;")
            
            print("✅ Database flushed successfully!")
            
        # Verify deletion
        print(f"\nAfter deletion:")
        print(f"DataFiles: {DataFile.objects.count()}")
        print(f"FileProcessingJobs: {FileProcessingJob.objects.count()}")
        print(f"SAPGLPostings: {SAPGLPosting.objects.count()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    flush_database()
