#!/usr/bin/env python
"""
Script to check existing SAPGLPosting data and run Holiday Analysis
"""

import os
import sys
import django
from datetime import datetime, date
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, DataFile, HolidayAnalysisResult
from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
from django.utils import timezone

def check_existing_data():
    """Check existing data in the database"""
    print("=== EXISTING DATA ANALYSIS ===")
    
    # Check total records
    total_postings = SAPGLPosting.objects.count()
    total_files = DataFile.objects.count()
    
    print(f"Total SAPGLPosting records: {total_postings}")
    print(f"Total DataFile records: {total_files}")
    
    if total_postings == 0:
        print("No SAPGLPosting records found in database.")
        return None
    
    # Get sample records
    sample_records = SAPGLPosting.objects.all()[:10]
    print(f"\nSample records (first 10):")
    for i, record in enumerate(sample_records, 1):
        print(f"{i}. ID: {record.id}")
        print(f"   Document: {record.document_number}")
        print(f"   Date: {record.posting_date}")
        print(f"   Amount: {record.amount_local_currency}")
        print(f"   User: {record.user_name}")
        print(f"   Account: {record.gl_account}")
        print(f"   Type: {record.transaction_type}")
        print(f"   Fiscal Year: {record.fiscal_year}")
        print()
    
    # Check date range
    dates = SAPGLPosting.objects.values_list('posting_date', flat=True).distinct().order_by('posting_date')
    if dates:
        print(f"Date range: {dates.first()} to {dates.last()}")
        print(f"Unique dates: {dates.count()}")
    
    # Check data files
    if total_files > 0:
        print(f"\nData Files:")
        for file in DataFile.objects.all():
            print(f"- {file.file_name} (Fiscal Year: {file.fiscal_year})")
            if file.audit_start_date and file.audit_end_date:
                print(f"  Audit period: {file.audit_start_date} to {file.audit_end_date}")
    
    return sample_records

def run_holiday_analysis_on_existing_data():
    """Run Holiday Analysis on existing data"""
    print("\n=== RUNNING HOLIDAY ANALYSIS ===")
    
    # Get all transactions
    transactions = list(SAPGLPosting.objects.all())
    
    if not transactions:
        print("No transactions found for analysis.")
        return None
    
    print(f"Running Holiday Analysis on {len(transactions)} transactions...")
    
    # Initialize orchestrator
    orchestrator = MLAnalysisOrchestrator()
    
    # Run holiday analysis
    try:
        start_time = timezone.now()
        holiday_result = orchestrator.run_holiday_analysis(transactions)
        end_time = timezone.now()
        
        processing_duration = (end_time - start_time).total_seconds()
        holiday_result['processing_duration'] = processing_duration
        
        print(f"Analysis completed in {processing_duration:.2f} seconds")
        
        return holiday_result
        
    except Exception as e:
        print(f"Error running Holiday Analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_report(holiday_result):
    """Generate a comprehensive report from Holiday Analysis results"""
    if not holiday_result:
        print("No results to generate report from.")
        return
    
    print("\n" + "="*80)
    print("HOLIDAY ANALYSIS REPORT")
    print("="*80)
    
    # Analysis Info
    analysis_info = holiday_result.get('analysis_info', {})
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"   Total Transactions Analyzed: {analysis_info.get('total_transactions', 0):,}")
    print(f"   Holiday Postings Found: {analysis_info.get('holiday_postings_count', 0):,}")
    print(f"   Holiday Percentage: {analysis_info.get('holiday_percentage', 0):.2f}%")
    print(f"   Unique Holidays: {analysis_info.get('unique_holidays', 0)}")
    print(f"   Country Code: {analysis_info.get('country_code', 'Unknown')}")
    print(f"   Fiscal Year: {analysis_info.get('fiscal_year', 'Unknown')}")
    print(f"   Analysis Date: {analysis_info.get('analysis_date', 'Unknown')}")
    
    # Risk Assessment
    risk_assessment = holiday_result.get('risk_assessment', {})
    print(f"\n⚠️  RISK ASSESSMENT:")
    print(f"   Overall Risk Score: {risk_assessment.get('overall_risk_score', 0):.2f}/100")
    print(f"   Holiday Risk Score: {risk_assessment.get('holiday_risk_score', 0):.2f}/100")
    print(f"   High Value Holiday Risk: {risk_assessment.get('high_value_holiday_risk_score', 0):.2f}/100")
    print(f"   Unusual Pattern Risk: {risk_assessment.get('unusual_pattern_risk_score', 0):.2f}/100")
    print(f"   Risk Level: {risk_assessment.get('risk_level', 'Unknown')}")
    
    # Recommendations
    recommendations = risk_assessment.get('recommendations', [])
    if recommendations:
        print(f"\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(recommendations, 1):
            print(f"   {i}. {rec}")
    
    # Holiday Postings Details
    holiday_postings = holiday_result.get('holiday_postings', [])
    if holiday_postings:
        print(f"\n🎯 HOLIDAY POSTINGS DETAILS:")
        print(f"   Found {len(holiday_postings)} transactions posted on holidays")
        
        # Group by holiday
        holiday_groups = {}
        for posting in holiday_postings:
            holiday_name = posting.get('holiday_name', 'Unknown')
            if holiday_name not in holiday_groups:
                holiday_groups[holiday_name] = []
            holiday_groups[holiday_name].append(posting)
        
        for holiday_name, postings in holiday_groups.items():
            total_amount = sum(p.get('amount', 0) for p in postings)
            high_value_count = len([p for p in postings if p.get('amount', 0) > 1000000])
            print(f"\n   📅 {holiday_name}:")
            print(f"      Transactions: {len(postings)}")
            print(f"      Total Amount: {total_amount:,.2f} SAR")
            print(f"      High Value (>1M): {high_value_count}")
            
            # Show top transactions
            top_transactions = sorted(postings, key=lambda x: x.get('amount', 0), reverse=True)[:3]
            for i, trans in enumerate(top_transactions, 1):
                print(f"      {i}. {trans.get('document_number', 'N/A')} - {trans.get('amount', 0):,.2f} SAR - {trans.get('user_name', 'Unknown')}")
    
    # Chart Data Summary
    chart_data = holiday_result.get('chart_data', {})
    if chart_data:
        print(f"\n📈 CHART DATA SUMMARY:")
        
        if 'holiday_by_fs_line' in chart_data:
            fs_data = chart_data['holiday_by_fs_line']
            print(f"   Holiday Postings by FS Line: {len(fs_data.get('labels', []))} categories")
        
        if 'holiday_by_account' in chart_data:
            account_data = chart_data['holiday_by_account']
            print(f"   Holiday Postings by Account: {len(account_data.get('labels', []))} accounts")
        
        if 'holiday_by_user' in chart_data:
            user_data = chart_data['holiday_by_user']
            print(f"   Holiday Postings by User: {len(user_data.get('labels', []))} users")
        
        if 'gl_activity_by_holiday' in chart_data:
            holiday_data = chart_data['gl_activity_by_holiday']
            print(f"   GL Activity by Holiday: {len(holiday_data.get('labels', []))} holidays")
    
    # Export Data Summary
    export_data = holiday_result.get('export_data', [])
    if export_data:
        print(f"\n📋 EXPORT DATA:")
        print(f"   Export Records: {len(export_data)}")
        print(f"   Ready for export to Excel/CSV")
    
    print("\n" + "="*80)
    print("REPORT COMPLETED")
    print("="*80)

def save_results_to_database(holiday_result):
    """Save Holiday Analysis results to database"""
    if not holiday_result:
        return None
    
    try:
        # Get the first data file (assuming we have one)
        data_file = DataFile.objects.first()
        if not data_file:
            print("No DataFile found. Creating a test one...")
            data_file = DataFile.objects.create(
                file_name="test_holiday_analysis.csv",
                fiscal_year=2024,
                upload_date=timezone.now()
            )
        
        # Create HolidayAnalysisResult
        holiday_analysis_result = HolidayAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='holiday_analysis',
            analysis_version='1.0.0',
            analysis_info=holiday_result.get('analysis_info', {}),
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
            processing_duration=holiday_result.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        print(f"\n✅ Results saved to database with ID: {holiday_analysis_result.id}")
        return holiday_analysis_result
        
    except Exception as e:
        print(f"Error saving results to database: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the complete analysis"""
    print("HOLIDAY ANALYSIS TEST ON EXISTING DATA")
    print("="*50)
    
    # Check existing data
    sample_records = check_existing_data()
    
    if not sample_records:
        print("No data found. Please upload some data first.")
        return
    
    # Run Holiday Analysis
    holiday_result = run_holiday_analysis_on_existing_data()
    
    if holiday_result:
        # Generate report
        generate_report(holiday_result)
        
        # Save to database
        save_results_to_database(holiday_result)
        
        # Save report to file
        report_filename = f"holiday_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_filename, 'w', encoding='utf-8') as f:
            # Redirect stdout to file
            import sys
            original_stdout = sys.stdout
            sys.stdout = f
            
            generate_report(holiday_result)
            
            # Restore stdout
            sys.stdout = original_stdout
        
        print(f"\n📄 Report saved to: {report_filename}")
        
    else:
        print("Failed to run Holiday Analysis.")

if __name__ == "__main__":
    main() 