#!/usr/bin/env python3
"""
Docker Data Analysis Script
===========================

This script performs comprehensive manual analysis of uploaded data to identify:
1. Duplicate transactions (all 6 types)
2. Backdated entries
3. Overall risk calculation issues
4. Celery processing problems

Run this in Docker to analyze the current data and identify issues.
"""

import os
import sys
import django
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
from collections import defaultdict, Counter
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    SAPGLPosting, DataFile, FileProcessingJob, 
    DuplicateAnalysisResult, BackdatedAnalysisResult, 
    OverallAnalysisResult, RiskScoringDocument
)

class DockerDataAnalyzer:
    """Comprehensive data analyzer for Docker environment"""
    
    def __init__(self):
        self.analysis_results = {}
        self.issues_found = []
        
    def analyze_all_data(self):
        """Run comprehensive analysis on all data"""
        print("🔍 DOCKER DATA ANALYSIS STARTED")
        print("=" * 60)
        
        # 1. Check data files and jobs
        self.analyze_data_files()
        
        # 2. Check for duplicates
        self.analyze_duplicates()
        
        # 3. Check for backdated entries
        self.analyze_backdated_entries()
        
        # 4. Check overall risk calculation
        self.analyze_overall_risk()
        
        # 5. Check Celery processing status
        self.analyze_celery_status()
        
        # 6. Generate comprehensive report
        self.generate_report()
        
    def analyze_data_files(self):
        """Analyze uploaded data files"""
        print("\n📁 ANALYZING DATA FILES")
        print("-" * 40)
        
        data_files = DataFile.objects.all()
        print(f"Total data files: {data_files.count()}")
        
        for file in data_files:
            print(f"\nFile: {file.file_name}")
            print(f"  Uploaded: {file.uploaded_at}")
            print(f"  Size: {file.file_size} bytes")
            
            # Get transactions for this file
            transactions = SAPGLPosting.objects.filter(data_file=file)
            print(f"  Transactions: {transactions.count()}")
            
            if transactions.exists():
                # Basic statistics
                amounts = [float(t.amount_local_currency) for t in transactions]
                print(f"  Total amount: {sum(amounts):,.2f}")
                print(f"  Average amount: {sum(amounts)/len(amounts):,.2f}")
                print(f"  Min amount: {min(amounts):,.2f}")
                print(f"  Max amount: {max(amounts):,.2f}")
                
                # Date range
                dates = [t.posting_date for t in transactions if t.posting_date]
                if dates:
                    print(f"  Date range: {min(dates)} to {max(dates)}")
                
                # Users
                users = set(t.user_name for t in transactions if t.user_name)
                print(f"  Unique users: {len(users)}")
                
                # GL Accounts
                accounts = set(t.gl_account for t in transactions if t.gl_account)
                print(f"  Unique GL accounts: {len(accounts)}")
    
    def analyze_duplicates(self):
        """Analyze duplicate transactions using all 6 types"""
        print("\n🔄 ANALYZING DUPLICATES")
        print("-" * 40)
        
        all_transactions = SAPGLPosting.objects.all()
        print(f"Total transactions to analyze: {all_transactions.count()}")
        
        # Convert to list for analysis
        transactions_list = list(all_transactions)
        
        # Initialize duplicate detection
        duplicates_found = {
            'type_1': [],  # Account + Amount
            'type_2': [],  # Account + Source + Amount
            'type_3': [],  # Account + User + Amount
            'type_4': [],  # Account + Posted Date + Amount
            'type_5': [],  # Account + Effective Date + Amount
            'type_6': [],  # Account + Effective Date + Posted Date + User + Source + Amount
        }
        
        # Create lookup dictionaries for each type
        lookup_tables = {
            'type_1': defaultdict(list),
            'type_2': defaultdict(list),
            'type_3': defaultdict(list),
            'type_4': defaultdict(list),
            'type_5': defaultdict(list),
            'type_6': defaultdict(list),
        }
        
        print("Building lookup tables...")
        
        for i, transaction in enumerate(transactions_list):
            if i % 1000 == 0:
                print(f"  Processed {i}/{len(transactions_list)} transactions...")
            
            # Type 1: Account + Amount
            key1 = (transaction.gl_account, float(transaction.amount_local_currency))
            lookup_tables['type_1'][key1].append(transaction)
            
            # Type 2: Account + Source + Amount
            key2 = (transaction.gl_account, transaction.document_type, float(transaction.amount_local_currency))
            lookup_tables['type_2'][key2].append(transaction)
            
            # Type 3: Account + User + Amount
            key3 = (transaction.gl_account, transaction.user_name, float(transaction.amount_local_currency))
            lookup_tables['type_3'][key3].append(transaction)
            
            # Type 4: Account + Posted Date + Amount
            if transaction.posting_date:
                key4 = (transaction.gl_account, transaction.posting_date, float(transaction.amount_local_currency))
                lookup_tables['type_4'][key4].append(transaction)
            
            # Type 5: Account + Effective Date + Amount
            if transaction.document_date:
                key5 = (transaction.gl_account, transaction.document_date, float(transaction.amount_local_currency))
                lookup_tables['type_5'][key5].append(transaction)
            
            # Type 6: Account + Effective Date + Posted Date + User + Source + Amount
            if transaction.document_date and transaction.posting_date:
                key6 = (transaction.gl_account, transaction.document_date, transaction.posting_date, 
                       transaction.user_name, transaction.document_type, float(transaction.amount_local_currency))
                lookup_tables['type_6'][key6].append(transaction)
        
        print("Finding duplicates...")
        
        # Find duplicates for each type
        for duplicate_type, lookup_table in lookup_tables.items():
            print(f"\nChecking {duplicate_type}...")
            duplicate_count = 0
            
            for key, transactions in lookup_table.items():
                if len(transactions) > 1:
                    duplicate_count += len(transactions) - 1
                    duplicates_found[duplicate_type].extend(transactions)
                    
                    # Show first few duplicates
                    if len(duplicates_found[duplicate_type]) <= 10:
                        print(f"  Found {len(transactions)} duplicates for key: {key}")
                        for t in transactions[:3]:  # Show first 3
                            print(f"    - {t.document_number}: {t.gl_account} | {t.amount_local_currency} | {t.posting_date}")
            
            print(f"  Total {duplicate_type} duplicates: {duplicate_count}")
        
        # Check existing duplicate analysis results
        print("\nChecking existing duplicate analysis results...")
        duplicate_results = DuplicateAnalysisResult.objects.all()
        print(f"Existing duplicate analysis results: {duplicate_results.count()}")
        
        for result in duplicate_results:
            print(f"  File: {result.data_file.file_name if result.data_file else 'Unknown'}")
            print(f"  Status: {result.status}")
            print(f"  Duplicates found: {len(result.duplicate_list) if result.duplicate_list else 0}")
        
        self.analysis_results['duplicates'] = duplicates_found
    
    def analyze_backdated_entries(self):
        """Analyze backdated entries"""
        print("\n📅 ANALYZING BACKDATED ENTRIES")
        print("-" * 40)
        
        all_transactions = SAPGLPosting.objects.all()
        backdated_entries = []
        
        print(f"Total transactions to check: {all_transactions.count()}")
        
        for i, transaction in enumerate(all_transactions):
            if i % 1000 == 0:
                print(f"  Processed {i}/{all_transactions.count()} transactions...")
            
            if transaction.document_date and transaction.posting_date:
                days_difference = (transaction.posting_date - transaction.document_date).days
                
                if days_difference > 0:  # Backdated
                    backdated_entries.append({
                        'transaction': transaction,
                        'days_difference': days_difference,
                        'risk_level': self._get_backdated_risk_level(days_difference)
                    })
        
        print(f"\nTotal backdated entries found: {len(backdated_entries)}")
        
        if backdated_entries:
            # Group by risk level
            risk_groups = defaultdict(list)
            for entry in backdated_entries:
                risk_groups[entry['risk_level']].append(entry)
            
            for risk_level, entries in risk_groups.items():
                print(f"\n{risk_level} Risk Backdated Entries: {len(entries)}")
                for entry in entries[:5]:  # Show first 5
                    t = entry['transaction']
                    print(f"  - {t.document_number}: {t.document_date} -> {t.posting_date} ({entry['days_difference']} days)")
        
        # Check existing backdated analysis results
        print("\nChecking existing backdated analysis results...")
        backdated_results = BackdatedAnalysisResult.objects.all()
        print(f"Existing backdated analysis results: {backdated_results.count()}")
        
        for result in backdated_results:
            print(f"  File: {result.data_file.file_name if result.data_file else 'Unknown'}")
            print(f"  Status: {result.status}")
            print(f"  Backdated entries: {len(result.backdated_entries) if result.backdated_entries else 0}")
        
        self.analysis_results['backdated'] = backdated_entries
    
    def analyze_overall_risk(self):
        """Analyze overall risk calculation"""
        print("\n⚠️ ANALYZING OVERALL RISK CALCULATION")
        print("-" * 40)
        
        # Check existing overall analysis results
        overall_results = OverallAnalysisResult.objects.all()
        print(f"Existing overall analysis results: {overall_results.count()}")
        
        for result in overall_results:
            print(f"\nFile: {result.data_file.file_name if result.data_file else 'Unknown'}")
            print(f"Status: {result.status}")
            print(f"Processing duration: {result.processing_duration}")
            
            if result.risk_assessment:
                print(f"Risk assessment: {result.risk_assessment}")
            else:
                print("❌ No risk assessment found!")
                self.issues_found.append(f"Missing risk assessment for {result.data_file.file_name if result.data_file else 'Unknown'}")
        
        # Check existing risk scoring documents
        risk_documents = RiskScoringDocument.objects.all()
        print(f"\nExisting risk scoring documents: {risk_documents.count()}")
        
        for doc in risk_documents:
            print(f"\nDocument: {doc.document_type}")
            print(f"Status: {doc.status}")
            print(f"Overall risk score: {doc.overall_risk_score}")
            print(f"High risk transactions: {doc.high_risk_transactions}")
            print(f"Medium risk transactions: {doc.medium_risk_transactions}")
            print(f"Low risk transactions: {doc.low_risk_transactions}")
            
            if doc.overall_risk_score == 0:
                print("❌ Overall risk score is 0 - potential issue!")
                self.issues_found.append(f"Zero overall risk score for {doc.document_type}")
        
        self.analysis_results['overall_risk'] = {
            'overall_results_count': overall_results.count(),
            'risk_documents_count': risk_documents.count()
        }
    
    def analyze_celery_status(self):
        """Analyze Celery processing status"""
        print("\n🌿 ANALYZING CELERY STATUS")
        print("-" * 40)
        
        # Check processing jobs
        jobs = FileProcessingJob.objects.all()
        print(f"Total processing jobs: {jobs.count()}")
        
        status_counts = Counter(job.status for job in jobs)
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
        
        # Check for failed jobs
        failed_jobs = jobs.filter(status='FAILED')
        if failed_jobs.exists():
            print(f"\n❌ Failed jobs found: {failed_jobs.count()}")
            for job in failed_jobs[:5]:  # Show first 5
                print(f"  - {job.data_file.file_name if job.data_file else 'Unknown'}: {job.error_message}")
                self.issues_found.append(f"Failed job: {job.data_file.file_name if job.data_file else 'Unknown'}")
        
        # Check for stuck jobs
        processing_jobs = jobs.filter(status='PROCESSING')
        if processing_jobs.exists():
            print(f"\n⚠️ Stuck processing jobs: {processing_jobs.count()}")
            for job in processing_jobs:
                if job.started_at:
                    duration = (datetime.now(job.started_at.tzinfo) - job.started_at).total_seconds()
                    if duration > 3600:  # More than 1 hour
                        print(f"  - {job.data_file.file_name if job.data_file else 'Unknown'}: {duration/3600:.1f} hours")
                        self.issues_found.append(f"Stuck job: {job.data_file.file_name if job.data_file else 'Unknown'}")
        
        self.analysis_results['celery_status'] = {
            'total_jobs': jobs.count(),
            'status_counts': dict(status_counts),
            'failed_jobs': failed_jobs.count(),
            'stuck_jobs': processing_jobs.count()
        }
    
    def _get_backdated_risk_level(self, days_difference):
        """Get risk level for backdated entry"""
        if days_difference > 30:
            return 'Critical'
        elif days_difference > 7:
            return 'High'
        elif days_difference > 3:
            return 'Medium'
        else:
            return 'Low'
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        print("\n📊 COMPREHENSIVE ANALYSIS REPORT")
        print("=" * 60)
        
        # Summary
        print("\nSUMMARY:")
        print(f"  Issues found: {len(self.issues_found)}")
        
        if self.issues_found:
            print("\nISSUES IDENTIFIED:")
            for i, issue in enumerate(self.issues_found, 1):
                print(f"  {i}. {issue}")
        else:
            print("  ✅ No major issues identified")
        
        # Duplicate summary
        if 'duplicates' in self.analysis_results:
            print("\nDUPLICATE ANALYSIS:")
            for duplicate_type, transactions in self.analysis_results['duplicates'].items():
                print(f"  {duplicate_type}: {len(transactions)} duplicates")
        
        # Backdated summary
        if 'backdated' in self.analysis_results:
            print(f"\nBACKDATED ENTRIES: {len(self.analysis_results['backdated'])}")
        
        # Recommendations
        print("\nRECOMMENDATIONS:")
        if len(self.issues_found) > 0:
            print("  1. Fix failed Celery jobs")
            print("  2. Investigate missing risk assessments")
            print("  3. Review duplicate detection algorithms")
            print("  4. Check Celery worker connectivity")
        else:
            print("  1. System appears to be working correctly")
            print("  2. Monitor for new issues")
        
        # Save report to file
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'issues_found': self.issues_found,
            'analysis_results': self.analysis_results
        }
        
        with open('docker_analysis_report.json', 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        print(f"\n📄 Report saved to: docker_analysis_report.json")

def main():
    """Main function to run the analysis"""
    analyzer = DockerDataAnalyzer()
    analyzer.analyze_all_data()

if __name__ == "__main__":
    main() 