#!/usr/bin/env python3
"""
Script to manually run holiday analysis on the full dataset and save results
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, FileProcessingJob, HolidayAnalysisResult, SAPGLPosting
from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
from django.utils import timezone
import hashlib

def run_holiday_analysis_manual():
    """Manually run holiday analysis on the full dataset"""
    
    file_id = "f9215989-0e3c-4f64-ba4d-7eb3fd3fe0af"
    
    try:
        # Get the data file
        data_file = DataFile.objects.get(id=file_id)
        print(f"Processing file: {data_file.file_name}")
        print(f"File size: {data_file.file_size} bytes")
        
        # Get all transactions for this file
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        print(f"Total transactions: {len(transactions)}")
        
        if len(transactions) == 0:
            print("❌ No transactions found for this file")
            return None
        
        # Initialize the orchestrator
        orchestrator = MLAnalysisOrchestrator()
        
        # Run holiday analysis
        print("\n🔄 Running Holiday Analysis...")
        start_time = timezone.now()
        
        holiday_result = orchestrator.run_holiday_analysis(transactions)
        
        end_time = timezone.now()
        processing_duration = (end_time - start_time).total_seconds()
        
        print(f"✅ Holiday Analysis completed in {processing_duration:.2f} seconds")
        print(f"Result keys: {list(holiday_result.keys())}")
        
        # Check holiday postings
        holiday_postings = holiday_result.get('holiday_postings', [])
        print(f"Holiday postings found: {len(holiday_postings)}")
        
        if len(holiday_postings) > 0:
            print("Sample holiday postings:")
            for i, posting in enumerate(holiday_postings[:3]):
                print(f"  {i+1}. {posting.get('document_number', 'N/A')} - {posting.get('posting_date', 'N/A')} - {posting.get('holiday_name', 'N/A')}")
        else:
            print("No holiday postings found in the dataset")
        
        # Create a processing job for reference
        timestamp = timezone.now().isoformat()
        unique_hash = hashlib.sha256(f"{data_file.file_name}_manual_holiday_{timestamp}".encode()).hexdigest()
        
        job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash=unique_hash,
            run_anomalies=True,
            requested_anomalies=['holiday'],
            status='COMPLETED',
            started_at=start_time,
            completed_at=end_time,
            processing_duration=processing_duration
        )
        
        # Save holiday analysis results to database
        print("\n💾 Saving holiday analysis results to database...")
        
        holiday_analysis_result = HolidayAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='holiday_analysis',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'holiday_postings_count': holiday_result.get('holiday_postings_count', 0),
                'holiday_percentage': holiday_result.get('holiday_percentage', 0),
                'unique_holidays': holiday_result.get('unique_holidays', 0),
                'country_code': holiday_result.get('country_code', 'saudiarabian'),
                'fiscal_year': holiday_result.get('fiscal_year', '')
            },
            holiday_postings=holiday_result.get('holiday_postings', []),
            holiday_by_fs_line=holiday_result.get('holiday_by_fs_line', []),
            holiday_by_account=holiday_result.get('holiday_by_account', []),
            holiday_by_user=holiday_result.get('holiday_by_user', []),
            gl_activity_by_holiday=holiday_result.get('gl_activity_by_holiday', {}),
            audit_recommendations=holiday_result.get('risk_assessment', {}),
            compliance_assessment={
                'total_holiday_postings': len(holiday_result.get('holiday_postings', [])),
                'holiday_percentage': holiday_result.get('analysis_info', {}).get('holiday_percentage', 0),
                'unique_holidays': holiday_result.get('analysis_info', {}).get('unique_holidays', 0)
            },
            financial_statement_impact={
                'holiday_amount': sum(p.get('amount', 0) for p in holiday_result.get('holiday_postings', [])),
                'high_value_holiday_amount': sum(p.get('amount', 0) for p in holiday_result.get('holiday_postings', []) if p.get('amount', 0) > 1000000)
            },
            chart_data=holiday_result.get('chart_data', {}),
            export_data=holiday_result.get('export_data', []),
            processing_duration=processing_duration,
            status='COMPLETED'
        )
        
        print(f"✅ Holiday analysis saved to database with ID: {holiday_analysis_result.id}")
        
        # Show summary
        analysis_info = holiday_result.get('analysis_info', {})
        print(f"\n📊 Holiday Analysis Summary:")
        print(f"  Total Transactions: {analysis_info.get('total_transactions', 'N/A')}")
        print(f"  Holiday Postings: {analysis_info.get('holiday_postings_count', 'N/A')}")
        print(f"  Holiday Percentage: {analysis_info.get('holiday_percentage', 'N/A')}%")
        print(f"  Unique Holidays: {analysis_info.get('unique_holidays', 'N/A')}")
        print(f"  Country Code: {analysis_info.get('country_code', 'N/A')}")
        print(f"  Fiscal Year: {analysis_info.get('fiscal_year', 'N/A')}")
        
        return holiday_analysis_result
        
    except DataFile.DoesNotExist:
        print(f"❌ Error: File with ID {file_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error running holiday analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("🎯 Manual Holiday Analysis")
    print("=" * 50)
    
    result = run_holiday_analysis_manual()
    
    if result:
        print(f"\n✅ SUCCESS! Holiday analysis completed and saved")
        print(f"Result ID: {result.id}")
    else:
        print(f"\n❌ Holiday analysis failed") 