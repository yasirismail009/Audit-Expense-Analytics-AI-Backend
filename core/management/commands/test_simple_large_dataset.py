#!/usr/bin/env python3
"""
Simple test command for large dataset processing with proper risk scoring methodology
"""

import os
import django
import time
import logging
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.db import transaction

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting
from core.large_dataset_optimizer import LargeDatasetOptimizer

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Simple test of large dataset processing with proper risk scoring methodology'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--data-file-id',
            type=str,
            required=True,
            help='Data file ID to process'
        )
        parser.add_argument(
            '--sample-size',
            type=int,
            default=100,
            help='Number of records to process (default: 100)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=2,
            help='Number of parallel workers (default: 2)'
        )
    
    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('=== Large Dataset Processing Test with Risk Scoring Methodology ===')
        )
        
        data_file_id = options['data_file_id']
        sample_size = options['sample_size']
        max_workers = options['workers']
        
        # Check if data file exists
        try:
            data_file = DataFile.objects.get(id=data_file_id)
            self.stdout.write(f"Data File: {data_file.file_name}")
        except DataFile.DoesNotExist:
            raise CommandError(f"Data file with ID {data_file_id} not found")
        
        # Get total records
        total_records = SAPGLPosting.objects.filter(data_file_id=data_file_id).count()
        self.stdout.write(f"Total Records: {total_records}")
        
        # Get sample of records
        sample_records = list(SAPGLPosting.objects.filter(
            data_file_id=data_file_id
        )[:sample_size])
        
        self.stdout.write(f"Processing sample of {len(sample_records)} records")
        
        # Test parallel processing with proper risk scoring
        self._test_parallel_processing_with_risk_scoring(sample_records, max_workers, data_file)
        
        # Show results
        self._show_results(data_file_id)
        
        self.stdout.write(
            self.style.SUCCESS("\n=== Test completed successfully ===")
        )
    
    def _test_parallel_processing_with_risk_scoring(self, sample_records, max_workers: int, data_file):
        """Test parallel processing with proper risk scoring methodology"""
        self.stdout.write(f"Testing parallel processing with {max_workers} workers using risk scoring methodology...")
        
        start_time = time.time()
        
        # Create a parallel processing test with proper risk scoring
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def process_record_with_risk_scoring(record):
            """Process a single record with proper risk scoring methodology"""
            try:
                # Initialize analysis results
                analysis_results = {}
                
                # 1. Duplicate Analysis Risk Scoring (70-95 points)
                duplicate_risk = self._calculate_duplicate_risk(record)
                analysis_results['duplicate_analysis'] = {
                    'risk_score': duplicate_risk,
                    'analysis_type': 'duplicate'
                }
                
                # 2. Backdated Analysis Risk Scoring (50-100 points)
                backdated_risk = self._calculate_backdated_risk(record)
                analysis_results['backdated_analysis'] = {
                    'risk_score': backdated_risk,
                    'analysis_type': 'backdated'
                }
                
                # 3. User Analysis Risk Scoring (0-100 points)
                user_risk = self._calculate_user_risk(record)
                analysis_results['user_analysis'] = {
                    'risk_score': user_risk,
                    'analysis_type': 'user'
                }
                
                # 4. Unusual Days Analysis Risk Scoring (50-100 points)
                unusual_days_risk = self._calculate_unusual_days_risk(record)
                analysis_results['unusual_days_analysis'] = {
                    'risk_score': unusual_days_risk,
                    'analysis_type': 'unusual_days'
                }
                
                # 5. Closing Entries Analysis Risk Scoring (30-100 points)
                closing_entries_risk = self._calculate_closing_entries_risk(record)
                analysis_results['closing_entries_analysis'] = {
                    'risk_score': closing_entries_risk,
                    'analysis_type': 'closing_entries'
                }
                
                # Calculate overall risk score using the methodology
                overall_risk_score = self._calculate_overall_risk_score(record, analysis_results, data_file)
                
                return {
                    'record_id': str(record.id),
                    'success': True,
                    'analysis_results': analysis_results,
                    'overall_risk_score': overall_risk_score,
                    'risk_level': self._get_risk_level(overall_risk_score)
                }
                
            except Exception as e:
                return {
                    'record_id': str(record.id),
                    'success': False,
                    'error': str(e)
                }
        
        # Process records in parallel
        processed_count = 0
        failed_count = 0
        total_risk_score = 0
        risk_level_counts = {'Low': 0, 'Medium': 0, 'High': 0, 'Critical': 0}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all records for processing
            future_to_record = {executor.submit(process_record_with_risk_scoring, record): record for record in sample_records}
            
            # Collect results as they complete
            for future in as_completed(future_to_record):
                result = future.result()
                if result['success']:
                    processed_count += 1
                    total_risk_score += result['overall_risk_score']
                    risk_level = result['risk_level']
                    risk_level_counts[risk_level] += 1
                else:
                    failed_count += 1
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Display results
        self.stdout.write("\n=== Processing Results ===")
        self.stdout.write(f"Total Records: {len(sample_records)}")
        self.stdout.write(f"Processed Records: {processed_count}")
        self.stdout.write(f"Failed Records: {failed_count}")
        self.stdout.write(f"Success Rate: {processed_count / len(sample_records) * 100:.2f}%")
        self.stdout.write(f"Processing Time: {processing_time:.2f} seconds")
        self.stdout.write(f"Records per Second: {processed_count / processing_time:.2f}")
        self.stdout.write(f"Average Risk Score: {total_risk_score / processed_count if processed_count > 0 else 0:.2f}")
        self.stdout.write(f"Parallel Workers Used: {max_workers}")
        
        # Display risk level distribution
        self.stdout.write("\n=== Risk Level Distribution ===")
        for risk_level, count in risk_level_counts.items():
            percentage = (count / processed_count * 100) if processed_count > 0 else 0
            self.stdout.write(f"{risk_level} Risk: {count} records ({percentage:.1f}%)")
        
        self.stdout.write(
            self.style.SUCCESS("✓ Parallel processing with risk scoring completed")
        )
    
    def _calculate_duplicate_risk(self, record):
        """Calculate duplicate analysis risk score (70-95 points)"""
        try:
            # Check for duplicate flags based on the methodology
            if hasattr(record, 'is_duplicate') and record.is_duplicate:
                # Determine duplicate type based on available data
                if hasattr(record, 'duplicate_type'):
                    duplicate_type = record.duplicate_type
                    if duplicate_type == 'type_6':
                        return 95.0
                    elif duplicate_type == 'type_5':
                        return 90.0
                    elif duplicate_type == 'type_4':
                        return 85.0
                    elif duplicate_type == 'type_3':
                        return 80.0
                    elif duplicate_type == 'type_2':
                        return 75.0
                    else:
                        return 70.0  # type_1
                else:
                    return 70.0  # Default to type_1
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating duplicate risk: {e}")
            return 0.0
    
    def _calculate_backdated_risk(self, record):
        """Calculate backdated analysis risk score (50-100 points)"""
        try:
            if hasattr(record, 'is_backdated') and record.is_backdated:
                if hasattr(record, 'backdated_days'):
                    days_difference = record.backdated_days
                else:
                    # Calculate days difference if available
                    if hasattr(record, 'document_date') and hasattr(record, 'posting_date'):
                        if record.document_date and record.posting_date:
                            days_difference = (record.posting_date - record.document_date).days
                        else:
                            days_difference = 1  # Default
                    else:
                        days_difference = 1  # Default
                
                # Apply risk scoring methodology
                if days_difference > 30:
                    return 100.0  # Critical
                elif days_difference > 14:
                    return 85.0   # High
                elif days_difference > 7:
                    return 70.0   # Medium
                else:
                    return 50.0   # Low
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating backdated risk: {e}")
            return 0.0
    
    def _calculate_user_risk(self, record):
        """Calculate user analysis risk score (0-100 points)"""
        try:
            # Base user risk calculation
            base_risk = 0.0
            
            # Check for user anomalies
            if hasattr(record, 'user_name') and record.user_name:
                # Basic user risk - could be enhanced with more sophisticated analysis
                # For now, assign a moderate risk if user exists
                base_risk = 20.0
                
                # Add additional risk factors if available
                if hasattr(record, 'is_duplicate') and record.is_duplicate:
                    base_risk += 15.0
                if hasattr(record, 'is_backdated') and record.is_backdated:
                    base_risk += 10.0
                
                # Cap at 100
                return min(base_risk, 100.0)
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating user risk: {e}")
            return 0.0
    
    def _calculate_unusual_days_risk(self, record):
        """Calculate unusual days analysis risk score (50-100 points)"""
        try:
            if hasattr(record, 'posting_date') and record.posting_date:
                day_of_week = record.posting_date.weekday()
                
                # Check for weekend postings (Friday=4, Saturday=5)
                if day_of_week in [4, 5]:  # Friday, Saturday
                    base_risk = 50.0  # Base weekend posting risk
                    
                    # Add additional risk factors
                    if hasattr(record, 'transaction_type') and record.transaction_type == 'DEBIT':
                        base_risk += 10.0  # Debit transactions on weekend
                    
                    if record.posting_date.day >= 25:
                        base_risk += 10.0  # Month-end weekend posting
                    
                    if record.posting_date.month == 12 and record.posting_date.day >= 25:
                        base_risk += 10.0  # Year-end weekend posting
                    
                    return min(base_risk, 100.0)
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating unusual days risk: {e}")
            return 0.0
    
    def _calculate_closing_entries_risk(self, record):
        """Calculate closing entries analysis risk score (30-100 points)"""
        try:
            if hasattr(record, 'posting_date') and record.posting_date:
                # Check for month-end postings
                if record.posting_date.day >= 28:  # Near month-end
                    base_risk = 30.0  # Base closing entry risk
                    
                    # Add additional risk factors
                    if hasattr(record, 'transaction_type') and record.transaction_type == 'DEBIT':
                        base_risk += 10.0  # Debit closing entry
                    
                    if record.posting_date.day >= 25:
                        base_risk += 5.0   # Month-end closing
                    
                    if record.posting_date.month == 12 and record.posting_date.day >= 25:
                        base_risk += 10.0  # Year-end closing
                    
                    return min(base_risk, 100.0)
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating closing entries risk: {e}")
            return 0.0
    
    def _calculate_overall_risk_score(self, record, analysis_results, data_file):
        """Calculate overall risk score using the methodology"""
        try:
            risk_score = 0.0
            
            # 1. Duplicate Analysis Contribution (max 25 points)
            duplicate_risk = analysis_results.get('duplicate_analysis', {}).get('risk_score', 0.0)
            duplicate_contribution = min(duplicate_risk / 4.0, 25.0)
            
            # 2. Backdated Analysis Contribution (max 20 points)
            backdated_risk = analysis_results.get('backdated_analysis', {}).get('risk_score', 0.0)
            backdated_contribution = min(backdated_risk / 5.0, 20.0)
            
            # 3. User Analysis Contribution (max 20 points)
            user_risk = analysis_results.get('user_analysis', {}).get('risk_score', 0.0)
            user_contribution = min(user_risk / 5.0, 20.0)
            
            # 4. Unusual Days Analysis Contribution (max 15 points)
            unusual_days_risk = analysis_results.get('unusual_days_analysis', {}).get('risk_score', 0.0)
            unusual_days_contribution = min(unusual_days_risk / 6.67, 15.0)
            
            # 5. Closing Entries Analysis Contribution (max 15 points)
            closing_entries_risk = analysis_results.get('closing_entries_analysis', {}).get('risk_score', 0.0)
            closing_entries_contribution = min(closing_entries_risk / 6.67, 15.0)
            
            # 6. Fiscal Year Validation (max 5 points)
            fiscal_year_mismatch = 0.0
            audit_period_violation = 0.0
            
            if hasattr(record, 'fiscal_year') and data_file.fiscal_year:
                if record.fiscal_year != data_file.fiscal_year:
                    fiscal_year_mismatch = 3.0
            
            if hasattr(record, 'posting_date') and data_file.audit_start_date and data_file.audit_end_date:
                if record.posting_date < data_file.audit_start_date or record.posting_date > data_file.audit_end_date:
                    audit_period_violation = 2.0
            
            total_risk_score = (duplicate_contribution + backdated_contribution + 
                              user_contribution + unusual_days_contribution + 
                              closing_entries_contribution + fiscal_year_mismatch + 
                              audit_period_violation)
            
            return min(total_risk_score, 100.0)
            
        except Exception as e:
            logger.error(f"Error calculating overall risk score: {e}")
            return 0.0
    
    def _get_risk_level(self, risk_score):
        """Get risk level based on risk score"""
        if risk_score >= 80:
            return 'Critical'
        elif risk_score >= 60:
            return 'High'
        elif risk_score >= 30:
            return 'Medium'
        else:
            return 'Low'
    
    def _show_results(self, data_file_id: str):
        """Show analysis results"""
        self.stdout.write("\n=== Analysis Results ===")
        
        # Get records with risk scores
        records_with_risk = SAPGLPosting.objects.filter(
            data_file_id=data_file_id,
            overall_risk_score__gt=0
        )
        
        self.stdout.write(f"Records with risk scores: {records_with_risk.count()}")
        
        # Show risk score distribution based on methodology
        risk_ranges = [
            (0, 30, "Low Risk (0-29)"),
            (30, 60, "Medium Risk (30-59)"),
            (60, 80, "High Risk (60-79)"),
            (80, 100, "Critical Risk (80-100)")
        ]
        
        for min_score, max_score, label in risk_ranges:
            count = records_with_risk.filter(
                overall_risk_score__gte=min_score,
                overall_risk_score__lt=max_score
            ).count()
            self.stdout.write(f"{label}: {count} records")
        
        # Show anomaly types
        anomaly_types = {}
        for record in records_with_risk:
            for anomaly in record.anomaly_types or []:
                anomaly_types[anomaly] = anomaly_types.get(anomaly, 0) + 1
        
        if anomaly_types:
            self.stdout.write("\nAnomaly Types Found:")
            for anomaly_type, count in anomaly_types.items():
                self.stdout.write(f"  {anomaly_type}: {count} records")
        
        # Show sample high-risk records
        high_risk_records = records_with_risk.filter(
            overall_risk_score__gte=60
        )[:5]
        
        if high_risk_records:
            self.stdout.write("\nSample High-Risk Records:")
            for record in high_risk_records:
                self.stdout.write(f"  ID: {record.id}, Risk: {record.overall_risk_score:.1f}, "
                                f"Account: {record.gl_account}, Amount: {record.amount_local_currency}")
        
        # Show system performance info
        import psutil
        memory = psutil.virtual_memory()
        self.stdout.write(f"\nSystem Memory: {memory.percent}% used ({memory.used / (1024**3):.2f} GB / {memory.total / (1024**3):.2f} GB)")
        self.stdout.write(f"CPU Usage: {psutil.cpu_percent()}%") 