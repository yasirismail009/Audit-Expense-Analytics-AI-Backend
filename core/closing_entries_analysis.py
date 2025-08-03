#!/usr/bin/env python3
"""
Closing Entries Analysis Module

This module provides comprehensive closing entries analysis functionality to identify:
- Journal entries posted during month closing period
- Configurable closing window (x days before, y days after month-end)
- Post-close flag analysis
- Closing entries by FS line
- Closing entries by user
- Month-end transaction patterns

Charts and Analysis:
- Breakdown of Journal Lines per Post Close Flag and FSLI
- Breakdown of Journal Lines per Post Close Flag and User
- Month-end Activity Patterns
- Closing Window Analysis
"""

import os
import django
from django.utils import timezone
from django.db.models import Count, Sum, Avg, Max, Min, Q
from decimal import Decimal
import logging
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
from calendar import monthrange

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class ClosingEntriesAnalyzer:
    """Closing Entries Analysis class for identifying month-end closing transactions"""
    
    def __init__(self, days_before_month_end=5, days_after_month_end=5):
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        try:
            django.setup()
        except:
            pass
        
        from .models import SAPGLPosting, ClosingEntriesAnalysisResult
        
        self.SAPGLPosting = SAPGLPosting
        self.ClosingEntriesAnalysisResult = ClosingEntriesAnalysisResult
        
        # Analysis configuration
        self.analysis_config = {
            'currency': 'SAR',
            'days_before_month_end': days_before_month_end,  # Default 5 days before
            'days_after_month_end': days_after_month_end,    # Default 5 days after
            'high_value_threshold': 1000000,  # High value transaction threshold
            'risk_weights': {
                'closing_entries_count': 0.30,
                'high_value_closing': 0.25,
                'post_close_entries': 0.25,
                'unusual_closing_patterns': 0.20
            }
        }
    
    def run_closing_entries_analysis(self, data_file, processing_job=None):
        """
        Run comprehensive closing entries analysis
        
        Args:
            data_file: DataFile object
            processing_job: FileProcessingJob object (optional)
            
        Returns:
            dict: Complete closing entries analysis results
        """
        try:
            logger.info(f"Starting closing entries analysis for file: {data_file.file_name}")
            start_time = timezone.now()
            
            # Get all transactions for this file
            transactions = self.SAPGLPosting.objects.filter(
                document_number__in=data_file.get_transaction_document_numbers()
            )
            
            # 1. Closing Entries Detection
            closing_entries = self._detect_closing_entries(transactions)
            
            # 2. Post-Close Flag Analysis
            post_close_analysis = self._analyze_post_close_flags(transactions, closing_entries)
            
            # 3. Closing Entries by FS Line
            fs_line_closing = self._analyze_closing_by_fs_line(transactions, closing_entries)
            
            # 4. Closing Entries by User
            user_closing = self._analyze_closing_by_user(transactions, closing_entries)
            
            # 5. Month-end Activity Patterns
            month_end_patterns = self._analyze_month_end_patterns(transactions)
            
            # 6. Closing Window Analysis
            closing_window_analysis = self._analyze_closing_windows(transactions)
            
            # 7. Risk Assessment
            risk_assessment = self._assess_closing_entries_risk(transactions, closing_entries, post_close_analysis)
            
            # 8. Chart Data Generation
            chart_data = self._generate_chart_data(
                closing_entries,
                post_close_analysis,
                fs_line_closing,
                user_closing,
                month_end_patterns,
                closing_window_analysis
            )
            
            # 9. Export Data
            export_data = self._generate_export_data(
                closing_entries,
                post_close_analysis,
                fs_line_closing,
                user_closing
            )
            
            # Calculate processing duration
            processing_duration = (timezone.now() - start_time).total_seconds()
            
            # Create analysis result
            analysis_result = {
                'analysis_info': {
                    'total_transactions': transactions.count(),
                    'closing_entries_count': len(closing_entries),
                    'days_before_month_end': self.analysis_config['days_before_month_end'],
                    'days_after_month_end': self.analysis_config['days_after_month_end'],
                    'analysis_date': timezone.now().isoformat(),
                    'processing_duration': processing_duration
                },
                'closing_entries': closing_entries,
                'post_close_analysis': post_close_analysis,
                'fs_line_closing': fs_line_closing,
                'user_closing': user_closing,
                'month_end_patterns': month_end_patterns,
                'closing_window_analysis': closing_window_analysis,
                'risk_assessment': risk_assessment,
                'chart_data': chart_data,
                'export_data': export_data
            }
            
            # Convert any remaining Decimal objects to float for JSON serialization
            analysis_result = self._convert_decimals_to_float(analysis_result)
            
            # Save to database
            closing_entries_analysis = self.ClosingEntriesAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                processing_duration=processing_duration,
                **analysis_result
            )
            
            logger.info(f"Closing entries analysis completed for file: {data_file.file_name} in {processing_duration:.2f} seconds")
            
            return {
                'analysis_id': str(closing_entries_analysis.id),
                'processing_duration': processing_duration,
                'results': analysis_result
            }
            
        except Exception as e:
            logger.error(f"Error in closing entries analysis: {e}")
            raise
    
    def _detect_closing_entries(self, transactions):
        """Detect closing entries within the configured window"""
        closing_entries = []
        days_before = self.analysis_config['days_before_month_end']
        days_after = self.analysis_config['days_after_month_end']
        
        for transaction in transactions:
            if transaction.posting_date:
                # Check if transaction is within closing window
                if self._is_within_closing_window(transaction.posting_date, days_before, days_after):
                    closing_entries.append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'gl_account': transaction.gl_account,
                        'amount': float(transaction.amount_local_currency),
                        'user_name': transaction.user_name,
                        'posting_date': transaction.posting_date.isoformat(),
                        'month_end_date': self._get_month_end_date(transaction.posting_date),
                        'days_from_month_end': self._get_days_from_month_end(transaction.posting_date),
                        'is_post_close': self._is_post_close_entry(transaction.posting_date),
                        'closing_window_type': self._get_closing_window_type(transaction.posting_date, days_before, days_after),
                        'risk_score': self._calculate_closing_risk_score(transaction),
                        'transaction_type': transaction.transaction_type,
                        'fiscal_year': transaction.fiscal_year,
                        'posting_period': transaction.posting_period,
                        'fs_line': self._get_fs_line_from_account(transaction.gl_account)
                    })
        
        return closing_entries
    
    def _is_within_closing_window(self, posting_date, days_before, days_after):
        """Check if posting date is within closing window"""
        month_end = self._get_month_end_date(posting_date)
        days_from_end = self._get_days_from_month_end(posting_date)
        
        return abs(days_from_end) <= max(days_before, days_after)
    
    def _get_month_end_date(self, posting_date):
        """Get the last day of the month for the posting date"""
        from calendar import monthrange
        year = posting_date.year
        month = posting_date.month
        
        try:
            month_range = monthrange(year, month)
            if isinstance(month_range, tuple) and len(month_range) >= 2:
                last_day = month_range[1]
            else:
                # Fallback: calculate last day manually
                last_day = self._calculate_last_day_manual(year, month)
        except Exception:
            # Fallback: calculate last day manually
            last_day = self._calculate_last_day_manual(year, month)
        
        return datetime(year, month, last_day).date()
    
    def _calculate_last_day_manual(self, year, month):
        """Calculate last day of month manually"""
        if month == 12:
            return 31
        elif month in [4, 6, 9, 11]:
            return 30
        elif month == 2:
            # Leap year calculation
            if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                return 29
            else:
                return 28
        else:
            return 31
    
    def _get_days_from_month_end(self, posting_date):
        """Calculate days from month end (negative = before, positive = after)"""
        month_end = self._get_month_end_date(posting_date)
        return (posting_date - month_end).days
    
    def _is_post_close_entry(self, posting_date):
        """Check if entry is posted after month end"""
        days_from_end = self._get_days_from_month_end(posting_date)
        return days_from_end > 0
    
    def _get_closing_window_type(self, posting_date, days_before, days_after):
        """Get the type of closing window entry"""
        days_from_end = self._get_days_from_month_end(posting_date)
        
        if days_from_end < 0:
            return 'pre_close'
        elif days_from_end == 0:
            return 'month_end'
        else:
            return 'post_close'
    
    def _analyze_post_close_flags(self, transactions, closing_entries):
        """Analyze post-close flag patterns"""
        post_close_analysis = {
            'total_post_close_entries': 0,
            'post_close_by_fs_line': {},
            'post_close_by_user': {},
            'post_close_by_month': {},
            'high_value_post_close': [],
            'post_close_patterns': {}
        }
        
        for entry in closing_entries:
            if entry['is_post_close']:
                post_close_analysis['total_post_close_entries'] += 1
                
                # By FS Line
                fs_line = entry['fs_line']
                if fs_line not in post_close_analysis['post_close_by_fs_line']:
                    post_close_analysis['post_close_by_fs_line'][fs_line] = {
                        'count': 0,
                        'total_amount': 0,
                        'entries': []
                    }
                post_close_analysis['post_close_by_fs_line'][fs_line]['count'] += 1
                post_close_analysis['post_close_by_fs_line'][fs_line]['total_amount'] += entry['amount']
                post_close_analysis['post_close_by_fs_line'][fs_line]['entries'].append(entry)
                
                # By User
                user_name = entry['user_name']
                if user_name not in post_close_analysis['post_close_by_user']:
                    post_close_analysis['post_close_by_user'][user_name] = {
                        'count': 0,
                        'total_amount': 0,
                        'entries': []
                    }
                post_close_analysis['post_close_by_user'][user_name]['count'] += 1
                post_close_analysis['post_close_by_user'][user_name]['total_amount'] += entry['amount']
                post_close_analysis['post_close_by_user'][user_name]['entries'].append(entry)
                
                # By Month
                month_key = f"{entry['posting_date'][:7]}"  # YYYY-MM
                if month_key not in post_close_analysis['post_close_by_month']:
                    post_close_analysis['post_close_by_month'][month_key] = {
                        'count': 0,
                        'total_amount': 0
                    }
                post_close_analysis['post_close_by_month'][month_key]['count'] += 1
                post_close_analysis['post_close_by_month'][month_key]['total_amount'] += entry['amount']
                
                # High value post-close entries
                if entry['amount'] > self.analysis_config['high_value_threshold']:
                    post_close_analysis['high_value_post_close'].append(entry)
        
        return post_close_analysis
    
    def _analyze_closing_by_fs_line(self, transactions, closing_entries):
        """Analyze closing entries by financial statement line"""
        fs_line_closing = {}
        
        for entry in closing_entries:
            fs_line = entry['fs_line']
            if fs_line not in fs_line_closing:
                fs_line_closing[fs_line] = {
                    'fs_line': fs_line,
                    'total_closing_entries': 0,
                    'total_amount': 0,
                    'pre_close_count': 0,
                    'month_end_count': 0,
                    'post_close_count': 0,
                    'pre_close_amount': 0,
                    'month_end_amount': 0,
                    'post_close_amount': 0,
                    'unique_users': set(),
                    'entries': []
                }
            
            fs_line_closing[fs_line]['total_closing_entries'] += 1
            fs_line_closing[fs_line]['total_amount'] += entry['amount']
            fs_line_closing[fs_line]['unique_users'].add(entry['user_name'])
            fs_line_closing[fs_line]['entries'].append(entry)
            
            # Categorize by closing window type
            window_type = entry['closing_window_type']
            if window_type == 'pre_close':
                fs_line_closing[fs_line]['pre_close_count'] += 1
                fs_line_closing[fs_line]['pre_close_amount'] += entry['amount']
            elif window_type == 'month_end':
                fs_line_closing[fs_line]['month_end_count'] += 1
                fs_line_closing[fs_line]['month_end_amount'] += entry['amount']
            elif window_type == 'post_close':
                fs_line_closing[fs_line]['post_close_count'] += 1
                fs_line_closing[fs_line]['post_close_amount'] += entry['amount']
        
        # Convert sets to lists for JSON serialization
        for fs_line in fs_line_closing:
            fs_line_closing[fs_line]['unique_users'] = list(fs_line_closing[fs_line]['unique_users'])
        
        return fs_line_closing
    
    def _analyze_closing_by_user(self, transactions, closing_entries):
        """Analyze closing entries by user"""
        user_closing = {}
        
        for entry in closing_entries:
            user_name = entry['user_name']
            if user_name not in user_closing:
                user_closing[user_name] = {
                    'user_name': user_name,
                    'total_closing_entries': 0,
                    'total_amount': 0,
                    'pre_close_count': 0,
                    'month_end_count': 0,
                    'post_close_count': 0,
                    'pre_close_amount': 0,
                    'month_end_amount': 0,
                    'post_close_amount': 0,
                    'unique_fs_lines': set(),
                    'entries': []
                }
            
            user_closing[user_name]['total_closing_entries'] += 1
            user_closing[user_name]['total_amount'] += entry['amount']
            user_closing[user_name]['unique_fs_lines'].add(entry['fs_line'])
            user_closing[user_name]['entries'].append(entry)
            
            # Categorize by closing window type
            window_type = entry['closing_window_type']
            if window_type == 'pre_close':
                user_closing[user_name]['pre_close_count'] += 1
                user_closing[user_name]['pre_close_amount'] += entry['amount']
            elif window_type == 'month_end':
                user_closing[user_name]['month_end_count'] += 1
                user_closing[user_name]['month_end_amount'] += entry['amount']
            elif window_type == 'post_close':
                user_closing[user_name]['post_close_count'] += 1
                user_closing[user_name]['post_close_amount'] += entry['amount']
        
        # Convert sets to lists for JSON serialization
        for user_name in user_closing:
            user_closing[user_name]['unique_fs_lines'] = list(user_closing[user_name]['unique_fs_lines'])
        
        return user_closing
    
    def _analyze_month_end_patterns(self, transactions):
        """Analyze month-end activity patterns"""
        month_end_patterns = {}
        
        for transaction in transactions:
            if transaction.posting_date:
                month_key = f"{transaction.posting_date.year}-{transaction.posting_date.month:02d}"
                if month_key not in month_end_patterns:
                    month_end_patterns[month_key] = {
                        'month': month_key,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'closing_entries': 0,
                        'closing_amount': 0,
                        'post_close_entries': 0,
                        'post_close_amount': 0,
                        'unique_users': set(),
                        'unique_accounts': set()
                    }
                
                month_end_patterns[month_key]['total_transactions'] += 1
                month_end_patterns[month_key]['total_amount'] += float(transaction.amount_local_currency)
                month_end_patterns[month_key]['unique_users'].add(transaction.user_name)
                month_end_patterns[month_key]['unique_accounts'].add(transaction.gl_account)
                
                # Check if it's a closing entry
                if self._is_within_closing_window(transaction.posting_date, 
                                                self.analysis_config['days_before_month_end'],
                                                self.analysis_config['days_after_month_end']):
                    month_end_patterns[month_key]['closing_entries'] += 1
                    month_end_patterns[month_key]['closing_amount'] += float(transaction.amount_local_currency)
                    
                    # Check if it's post-close
                    if self._is_post_close_entry(transaction.posting_date):
                        month_end_patterns[month_key]['post_close_entries'] += 1
                        month_end_patterns[month_key]['post_close_amount'] += float(transaction.amount_local_currency)
        
        # Convert sets to lists for JSON serialization
        for month_key in month_end_patterns:
            month_end_patterns[month_key]['unique_users'] = list(month_end_patterns[month_key]['unique_users'])
            month_end_patterns[month_key]['unique_accounts'] = list(month_end_patterns[month_key]['unique_accounts'])
        
        return month_end_patterns
    
    def _analyze_closing_windows(self, transactions):
        """Analyze activity within different closing windows"""
        closing_window_analysis = {
            'pre_close_window': {'count': 0, 'amount': 0, 'entries': []},
            'month_end_day': {'count': 0, 'amount': 0, 'entries': []},
            'post_close_window': {'count': 0, 'amount': 0, 'entries': []},
            'window_distribution': {}
        }
        
        for transaction in transactions:
            if transaction.posting_date:
                days_from_end = self._get_days_from_month_end(transaction.posting_date)
                amount = float(transaction.amount_local_currency)
                
                # Categorize by window
                if days_from_end < 0:
                    closing_window_analysis['pre_close_window']['count'] += 1
                    closing_window_analysis['pre_close_window']['amount'] += amount
                    closing_window_analysis['pre_close_window']['entries'].append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'amount': amount,
                        'days_from_end': days_from_end
                    })
                elif days_from_end == 0:
                    closing_window_analysis['month_end_day']['count'] += 1
                    closing_window_analysis['month_end_day']['amount'] += amount
                    closing_window_analysis['month_end_day']['entries'].append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'amount': amount,
                        'days_from_end': days_from_end
                    })
                elif days_from_end > 0:
                    closing_window_analysis['post_close_window']['count'] += 1
                    closing_window_analysis['post_close_window']['amount'] += amount
                    closing_window_analysis['post_close_window']['entries'].append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'amount': amount,
                        'days_from_end': days_from_end
                    })
                
                # Track distribution by days from month end
                if days_from_end not in closing_window_analysis['window_distribution']:
                    closing_window_analysis['window_distribution'][days_from_end] = {
                        'count': 0,
                        'amount': 0
                    }
                closing_window_analysis['window_distribution'][days_from_end]['count'] += 1
                closing_window_analysis['window_distribution'][days_from_end]['amount'] += amount
        
        return closing_window_analysis
    
    def _assess_closing_entries_risk(self, transactions, closing_entries, post_close_analysis):
        """Assess risk based on closing entries patterns"""
        total_transactions = transactions.count()
        closing_count = len(closing_entries)
        post_close_count = post_close_analysis['total_post_close_entries']
        high_value_post_close = len(post_close_analysis['high_value_post_close'])
        
        # Calculate risk scores
        closing_entries_risk = (closing_count / total_transactions * 100) if total_transactions > 0 else 0
        post_close_risk = (post_close_count / total_transactions * 100) if total_transactions > 0 else 0
        high_value_post_close_risk = (high_value_post_close / total_transactions * 100) if total_transactions > 0 else 0
        
        # Unusual closing patterns (e.g., too many post-close entries)
        unusual_pattern_risk = 0
        if closing_count > 0:
            post_close_ratio = post_close_count / closing_count
            if post_close_ratio > 0.3:  # More than 30% post-close entries
                unusual_pattern_risk = post_close_ratio * 100
        
        # Overall risk assessment
        overall_risk_score = (
            closing_entries_risk * self.analysis_config['risk_weights']['closing_entries_count'] +
            post_close_risk * self.analysis_config['risk_weights']['post_close_entries'] +
            high_value_post_close_risk * self.analysis_config['risk_weights']['high_value_closing'] +
            unusual_pattern_risk * self.analysis_config['risk_weights']['unusual_closing_patterns']
        )
        
        return {
            'overall_risk_score': overall_risk_score,
            'closing_entries_risk_score': closing_entries_risk,
            'post_close_risk_score': post_close_risk,
            'high_value_post_close_risk_score': high_value_post_close_risk,
            'unusual_pattern_risk_score': unusual_pattern_risk,
            'risk_level': self._get_risk_level(overall_risk_score),
            'recommendations': self._get_closing_risk_recommendations(overall_risk_score, closing_count, post_close_count, high_value_post_close)
        }
    
    def _generate_chart_data(self, closing_entries, post_close_analysis, fs_line_closing, user_closing, month_end_patterns, closing_window_analysis):
        """Generate chart data for visualizations"""
        chart_data = {}
        
        # 1. Post Close Flag by FSLI
        if post_close_analysis['post_close_by_fs_line']:
            fs_lines = list(post_close_analysis['post_close_by_fs_line'].keys())
            post_close_counts = [post_close_analysis['post_close_by_fs_line'][fs]['count'] for fs in fs_lines]
            post_close_amounts = [post_close_analysis['post_close_by_fs_line'][fs]['total_amount'] for fs in fs_lines]
            
            chart_data['post_close_by_fs_line'] = {
                'labels': fs_lines,
                'counts': post_close_counts,
                'amounts': post_close_amounts
            }
        
        # 2. Post Close Flag by User
        if post_close_analysis['post_close_by_user']:
            users = list(post_close_analysis['post_close_by_user'].keys())
            post_close_counts = [post_close_analysis['post_close_by_user'][user]['count'] for user in users]
            post_close_amounts = [post_close_analysis['post_close_by_user'][user]['total_amount'] for user in users]
            
            chart_data['post_close_by_user'] = {
                'labels': users,
                'counts': post_close_counts,
                'amounts': post_close_amounts
            }
        
        # 3. Closing Window Distribution
        if closing_window_analysis['window_distribution']:
            days = sorted(closing_window_analysis['window_distribution'].keys())
            counts = [closing_window_analysis['window_distribution'][day]['count'] for day in days]
            amounts = [closing_window_analysis['window_distribution'][day]['amount'] for day in days]
            
            chart_data['closing_window_distribution'] = {
                'labels': [f"Day {day}" for day in days],
                'counts': counts,
                'amounts': amounts,
                'days_from_end': days
            }
        
        # 4. Month-end Activity Patterns
        if month_end_patterns:
            months = sorted(month_end_patterns.keys())
            total_transactions = [month_end_patterns[month]['total_transactions'] for month in months]
            closing_entries = [month_end_patterns[month]['closing_entries'] for month in months]
            post_close_entries = [month_end_patterns[month]['post_close_entries'] for month in months]
            
            chart_data['month_end_patterns'] = {
                'labels': months,
                'total_transactions': total_transactions,
                'closing_entries': closing_entries,
                'post_close_entries': post_close_entries
            }
        
        # 5. FS Line Closing Analysis
        if fs_line_closing:
            fs_lines = list(fs_line_closing.keys())
            pre_close_counts = [fs_line_closing[fs]['pre_close_count'] for fs in fs_lines]
            month_end_counts = [fs_line_closing[fs]['month_end_count'] for fs in fs_lines]
            post_close_counts = [fs_line_closing[fs]['post_close_count'] for fs in fs_lines]
            
            chart_data['fs_line_closing'] = {
                'labels': fs_lines,
                'pre_close_counts': pre_close_counts,
                'month_end_counts': month_end_counts,
                'post_close_counts': post_close_counts
            }
        
        return chart_data
    
    def _generate_export_data(self, closing_entries, post_close_analysis, fs_line_closing, user_closing):
        """Generate export data for reporting"""
        export_data = []
        
        # Closing entries export
        for entry in closing_entries:
            export_data.append({
                'type': 'closing_entry',
                'transaction_id': entry['transaction_id'],
                'document_number': entry['document_number'],
                'gl_account': entry['gl_account'],
                'amount': entry['amount'],
                'user_name': entry['user_name'],
                'posting_date': entry['posting_date'],
                'closing_window_type': entry['closing_window_type'],
                'is_post_close': entry['is_post_close'],
                'days_from_month_end': entry['days_from_month_end'],
                'risk_score': entry['risk_score'],
                'fs_line': entry['fs_line']
            })
        
        # Post-close analysis export
        for fs_line, data in post_close_analysis['post_close_by_fs_line'].items():
            export_data.append({
                'type': 'post_close_by_fs_line',
                'fs_line': fs_line,
                'count': data['count'],
                'total_amount': data['total_amount']
            })
        
        for user_name, data in post_close_analysis['post_close_by_user'].items():
            export_data.append({
                'type': 'post_close_by_user',
                'user_name': user_name,
                'count': data['count'],
                'total_amount': data['total_amount']
            })
        
        return export_data
    
    def _get_fs_line_from_account(self, account_id: str) -> str:
        """Get FS line from account ID"""
        if not account_id:
            return 'Unknown'
        
        try:
            # Simple FS line mapping based on account ID patterns
            if account_id.startswith('1'):  # Assets
                return 'Assets'
            elif account_id.startswith('2'):  # Liabilities
                return 'Liabilities'
            elif account_id.startswith('3'):  # Equity
                return 'Equity'
            elif account_id.startswith('4'):  # Revenue
                return 'Revenue'
            elif account_id.startswith('5'):  # Expenses
                return 'Expenses'
            else:
                return 'Other'
        except:
            return 'Unknown'
    
    def _calculate_closing_risk_score(self, transaction) -> float:
        """Calculate risk score for closing entry"""
        risk_score = 30.0  # Base risk for closing entry
        
        # Increase risk for high value transactions
        if float(transaction.amount_local_currency) > self.analysis_config['high_value_threshold']:
            risk_score += 30.0
        
        # Increase risk for post-close entries
        if self._is_post_close_entry(transaction.posting_date):
            risk_score += 25.0
        
        # Increase risk for unusual transaction types
        if transaction.transaction_type == 'DEBIT':
            risk_score += 10.0
        
        # Increase risk for month-end or year-end
        if transaction.posting_date:
            if transaction.posting_date.day >= 25:
                risk_score += 5.0
            if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
                risk_score += 10.0
        
        return min(risk_score, 100.0)
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Get risk level based on score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_closing_risk_recommendations(self, risk_score: float, closing_count: int, post_close_count: int, high_value_post_close: int) -> List[str]:
        """Get recommendations based on closing entries risk assessment"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.extend([
                'Immediate investigation of closing entries required',
                'Review all post-close entries for validity',
                'Implement closing entry controls',
                'Monitor high-value closing transactions closely'
            ])
        elif risk_score >= 60:
            recommendations.extend([
                'Investigate closing entry patterns',
                'Review post-close entry procedures',
                'Consider implementing closing entry restrictions',
                'Monitor unusual closing patterns'
            ])
        elif risk_score >= 30:
            recommendations.extend([
                'Monitor closing entry trends',
                'Review closing procedures',
                'Consider implementing closing entry controls'
            ])
        else:
            recommendations.append('Normal closing entry patterns detected')
        
        if closing_count > 0:
            recommendations.append(f'Found {closing_count} closing entries requiring review')
        
        if post_close_count > 0:
            recommendations.append(f'Found {post_close_count} post-close entries requiring investigation')
        
        if high_value_post_close > 0:
            recommendations.append(f'Found {high_value_post_close} high-value post-close entries requiring immediate review')
        
        return recommendations
    
    def _convert_decimals_to_float(self, data):
        """Convert Decimal objects to float for JSON serialization"""
        if isinstance(data, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_decimals_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data 