#!/usr/bin/env python3
"""
Unusual Days Analysis Module

This module provides comprehensive unusual days analysis functionality to identify:
- Journal entries posted on weekends (Friday/Saturday)
- Unusual posting patterns by day of week
- GL activity patterns by day of week
- User posting patterns by day of week
- FS line activity by day of week

Charts and Analysis:
- GL Activity by Day of Week
- Breakdown of Journal Lines per Day and User
- Breakdown of Journal Lines per Day and FS Line
- Weekend vs Weekday Activity Comparison
- Unusual Day Posting Patterns
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

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class UnusualDaysAnalyzer:
    """Unusual Days Analysis class for identifying weekend and unusual day postings"""
    
    def __init__(self):
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        try:
            django.setup()
        except:
            pass
        
        from .models import SAPGLPosting, UnusualDaysAnalysisResult
        
        self.SAPGLPosting = SAPGLPosting
        self.UnusualDaysAnalysisResult = UnusualDaysAnalysisResult
        
        # Analysis configuration
        self.analysis_config = {
            'currency': 'SAR',
            'weekend_days': [4, 5],  # Friday=4, Saturday=5
            'unusual_threshold': 0.1,  # 10% threshold for unusual patterns
            'risk_weights': {
                'weekend_postings': 0.40,
                'unusual_day_patterns': 0.30,
                'user_weekend_activity': 0.20,
                'high_value_weekend': 0.10
            }
        }
    
    def run_unusual_days_analysis(self, data_file, processing_job=None):
        """
        Run comprehensive unusual days analysis
        
        Args:
            data_file: DataFile object
            processing_job: FileProcessingJob object (optional)
            
        Returns:
            dict: Complete unusual days analysis results
        """
        try:
            logger.info(f"Starting unusual days analysis for file: {data_file.file_name}")
            start_time = timezone.now()
            
            # Get all transactions for this file
            transactions = self.SAPGLPosting.objects.filter(
                document_number__in=data_file.get_transaction_document_numbers()
            )
            
            # 1. Weekend Postings Analysis
            weekend_postings = self._analyze_weekend_postings(transactions)
            
            # 2. Day of Week Activity Analysis
            day_of_week_activity = self._analyze_day_of_week_activity(transactions)
            
            # 3. User Day of Week Patterns
            user_day_patterns = self._analyze_user_day_patterns(transactions)
            
            # 4. FS Line Day of Week Patterns
            fs_line_day_patterns = self._analyze_fs_line_day_patterns(transactions)
            
            # 5. Unusual Day Detection
            unusual_days = self._detect_unusual_days(transactions, day_of_week_activity)
            
            # 6. Risk Assessment
            risk_assessment = self._assess_unusual_days_risk(transactions, weekend_postings, unusual_days)
            
            # 7. Chart Data Generation
            chart_data = self._generate_chart_data(
                day_of_week_activity, 
                user_day_patterns, 
                fs_line_day_patterns,
                weekend_postings,
                unusual_days
            )
            
            # 8. Export Data
            export_data = self._generate_export_data(
                weekend_postings,
                unusual_days,
                day_of_week_activity,
                user_day_patterns,
                fs_line_day_patterns
            )
            
            # Calculate processing duration
            processing_duration = (timezone.now() - start_time).total_seconds()
            
            # Create analysis result
            analysis_result = {
                'analysis_info': {
                    'total_transactions': transactions.count(),
                    'weekend_transactions': len(weekend_postings),
                    'unusual_day_transactions': len(unusual_days),
                    'analysis_date': timezone.now().isoformat(),
                    'processing_duration': processing_duration
                },
                'weekend_postings': weekend_postings,
                'day_of_week_activity': day_of_week_activity,
                'user_day_patterns': user_day_patterns,
                'fs_line_day_patterns': fs_line_day_patterns,
                'unusual_days': unusual_days,
                'risk_assessment': risk_assessment,
                'chart_data': chart_data,
                'export_data': export_data
            }
            
            # Convert any remaining Decimal objects to float for JSON serialization
            analysis_result = self._convert_decimals_to_float(analysis_result)
            
            # Save to database
            unusual_days_analysis = self.UnusualDaysAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                processing_duration=processing_duration,
                **analysis_result
            )
            
            logger.info(f"Unusual days analysis completed for file: {data_file.file_name} in {processing_duration:.2f} seconds")
            
            return {
                'analysis_id': str(unusual_days_analysis.id),
                'processing_duration': processing_duration,
                'results': analysis_result
            }
            
        except Exception as e:
            logger.error(f"Error in unusual days analysis: {e}")
            raise
    
    def _analyze_weekend_postings(self, transactions):
        """Analyze weekend postings (Friday/Saturday)"""
        weekend_postings = []
        
        for transaction in transactions:
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                
                # Friday = 4, Saturday = 5
                if day_of_week in self.analysis_config['weekend_days']:
                    weekend_postings.append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'gl_account': transaction.gl_account,
                        'amount': float(transaction.amount_local_currency),
                        'user_name': transaction.user_name,
                        'posting_date': transaction.posting_date.isoformat(),
                        'day_of_week': day_of_week,
                        'day_name': self._get_day_name(day_of_week),
                        'is_weekend': True,
                        'risk_score': self._calculate_weekend_risk_score(transaction),
                        'transaction_type': transaction.transaction_type,
                        'fiscal_year': transaction.fiscal_year,
                        'posting_period': transaction.posting_period
                    })
        
        return weekend_postings
    
    def _analyze_day_of_week_activity(self, transactions):
        """Analyze activity patterns by day of week"""
        day_activity = {}
        
        for day in range(7):  # 0=Monday, 1=Tuesday, ..., 6=Sunday
            day_transactions = [t for t in transactions if t.posting_date and t.posting_date.weekday() == day]
            
            if day_transactions:
                amounts = [float(t.amount_local_currency) for t in day_transactions]
                debit_transactions = [t for t in day_transactions if t.transaction_type == 'DEBIT']
                credit_transactions = [t for t in day_transactions if t.transaction_type == 'CREDIT']
                
                day_activity[day] = {
                    'day_of_week': day,
                    'day_name': self._get_day_name(day),
                    'total_transactions': len(day_transactions),
                    'total_amount': sum(amounts),
                    'average_amount': sum(amounts) / len(amounts) if amounts else 0,
                    'max_amount': max(amounts) if amounts else 0,
                    'min_amount': min(amounts) if amounts else 0,
                    'debit_count': len(debit_transactions),
                    'credit_count': len(credit_transactions),
                    'debit_amount': sum(float(t.amount_local_currency) for t in debit_transactions),
                    'credit_amount': sum(float(t.amount_local_currency) for t in credit_transactions),
                    'unique_users': len(set(t.user_name for t in day_transactions)),
                    'unique_accounts': len(set(t.gl_account for t in day_transactions)),
                    'is_weekend': day in self.analysis_config['weekend_days']
                }
        
        return day_activity
    
    def _analyze_user_day_patterns(self, transactions):
        """Analyze user posting patterns by day of week"""
        user_patterns = {}
        
        for transaction in transactions:
            user = transaction.user_name
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                
                if user not in user_patterns:
                    user_patterns[user] = {
                        'user_name': user,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'day_breakdown': {i: {'count': 0, 'amount': 0} for i in range(7)},
                        'weekend_transactions': 0,
                        'weekend_amount': 0
                    }
                
                user_patterns[user]['total_transactions'] += 1
                user_patterns[user]['total_amount'] += float(transaction.amount_local_currency)
                user_patterns[user]['day_breakdown'][day_of_week]['count'] += 1
                user_patterns[user]['day_breakdown'][day_of_week]['amount'] += float(transaction.amount_local_currency)
                
                if day_of_week in self.analysis_config['weekend_days']:
                    user_patterns[user]['weekend_transactions'] += 1
                    user_patterns[user]['weekend_amount'] += float(transaction.amount_local_currency)
        
        return list(user_patterns.values())
    
    def _analyze_fs_line_day_patterns(self, transactions):
        """Analyze FS line activity by day of week"""
        fs_line_patterns = {}
        
        for transaction in transactions:
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                fs_line = self._get_fs_line_from_account(transaction.gl_account)
                
                if fs_line not in fs_line_patterns:
                    fs_line_patterns[fs_line] = {
                        'fs_line': fs_line,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'day_breakdown': {i: {'count': 0, 'amount': 0} for i in range(7)},
                        'weekend_transactions': 0,
                        'weekend_amount': 0
                    }
                
                fs_line_patterns[fs_line]['total_transactions'] += 1
                fs_line_patterns[fs_line]['total_amount'] += float(transaction.amount_local_currency)
                fs_line_patterns[fs_line]['day_breakdown'][day_of_week]['count'] += 1
                fs_line_patterns[fs_line]['day_breakdown'][day_of_week]['amount'] += float(transaction.amount_local_currency)
                
                if day_of_week in self.analysis_config['weekend_days']:
                    fs_line_patterns[fs_line]['weekend_transactions'] += 1
                    fs_line_patterns[fs_line]['weekend_amount'] += float(transaction.amount_local_currency)
        
        return list(fs_line_patterns.values())
    
    def _detect_unusual_days(self, transactions, day_activity):
        """Detect unusual day posting patterns"""
        unusual_days = []
        
        if not day_activity:
            return unusual_days
        
        # Calculate average activity per day
        total_transactions = sum(day['total_transactions'] for day in day_activity.values())
        avg_transactions_per_day = total_transactions / len(day_activity) if day_activity else 0
        
        # Calculate standard deviation
        transaction_counts = [day['total_transactions'] for day in day_activity.values()]
        std_dev = np.std(transaction_counts) if len(transaction_counts) > 1 else 0
        
        # Identify unusual patterns
        for day_info in day_activity.values():
            day_transactions = day_info['total_transactions']
            
            # Check if significantly different from average
            if avg_transactions_per_day > 0:
                deviation = abs(day_transactions - avg_transactions_per_day) / avg_transactions_per_day
                
                if deviation > self.analysis_config['unusual_threshold']:
                    unusual_days.append({
                        'day_of_week': day_info['day_of_week'],
                        'day_name': day_info['day_name'],
                        'transaction_count': day_transactions,
                        'average_transactions': avg_transactions_per_day,
                        'deviation_percentage': deviation * 100,
                        'is_weekend': day_info['is_weekend'],
                        'unusual_type': 'high_activity' if day_transactions > avg_transactions_per_day else 'low_activity'
                    })
        
        return unusual_days
    
    def _assess_unusual_days_risk(self, transactions, weekend_postings, unusual_days):
        """Assess risk based on unusual days patterns"""
        total_transactions = transactions.count()
        weekend_count = len(weekend_postings)
        unusual_count = len(unusual_days)
        
        # Calculate risk scores
        weekend_risk = (weekend_count / total_transactions * 100) if total_transactions > 0 else 0
        unusual_pattern_risk = (unusual_count / 7 * 100) if unusual_count > 0 else 0  # 7 days of week
        
        # High value weekend transactions
        high_value_weekend = [p for p in weekend_postings if p['amount'] > 1000000]
        high_value_risk = (len(high_value_weekend) / total_transactions * 100) if total_transactions > 0 else 0
        
        # Overall risk assessment
        overall_risk_score = (
            weekend_risk * self.analysis_config['risk_weights']['weekend_postings'] +
            unusual_pattern_risk * self.analysis_config['risk_weights']['unusual_day_patterns'] +
            high_value_risk * self.analysis_config['risk_weights']['high_value_weekend']
        )
        
        return {
            'overall_risk_score': overall_risk_score,
            'weekend_risk_score': weekend_risk,
            'unusual_pattern_risk_score': unusual_pattern_risk,
            'high_value_weekend_risk_score': high_value_risk,
            'risk_level': self._get_risk_level(overall_risk_score),
            'recommendations': self._get_risk_recommendations(overall_risk_score, weekend_count, unusual_count)
        }
    
    def _generate_chart_data(self, day_activity, user_patterns, fs_line_patterns, weekend_postings, unusual_days):
        """Generate chart data for visualizations"""
        chart_data = {}
        
        # 1. GL Activity by Day of Week
        if day_activity:
            days = list(day_activity.keys())
            day_names = [day_activity[day]['day_name'] for day in days]
            transaction_counts = [day_activity[day]['total_transactions'] for day in days]
            amounts = [day_activity[day]['total_amount'] for day in days]
            
            chart_data['gl_activity_by_day'] = {
                'labels': day_names,
                'transaction_counts': transaction_counts,
                'amounts': amounts,
                'weekend_days': [i for i, day in enumerate(days) if day_activity[day]['is_weekend']]
            }
        
        # 2. User Day Patterns
        if user_patterns:
            chart_data['user_day_patterns'] = {
                'users': [p['user_name'] for p in user_patterns],
                'weekend_transactions': [p['weekend_transactions'] for p in user_patterns],
                'weekend_amounts': [p['weekend_amount'] for p in user_patterns],
                'total_transactions': [p['total_transactions'] for p in user_patterns]
            }
        
        # 3. FS Line Day Patterns
        if fs_line_patterns:
            chart_data['fs_line_day_patterns'] = {
                'fs_lines': [p['fs_line'] for p in fs_line_patterns],
                'weekend_transactions': [p['weekend_transactions'] for p in fs_line_patterns],
                'weekend_amounts': [p['weekend_amount'] for p in fs_line_patterns],
                'total_transactions': [p['total_transactions'] for p in fs_line_patterns]
            }
        
        # 4. Weekend vs Weekday Comparison
        if day_activity:
            weekend_data = [day_activity[day] for day in day_activity if day_activity[day]['is_weekend']]
            weekday_data = [day_activity[day] for day in day_activity if not day_activity[day]['is_weekend']]
            
            chart_data['weekend_vs_weekday'] = {
                'weekend_transactions': sum(d['total_transactions'] for d in weekend_data),
                'weekday_transactions': sum(d['total_transactions'] for d in weekday_data),
                'weekend_amount': sum(d['total_amount'] for d in weekend_data),
                'weekday_amount': sum(d['total_amount'] for d in weekday_data)
            }
        
        return chart_data
    
    def _generate_export_data(self, weekend_postings, unusual_days, day_activity, user_patterns, fs_line_patterns):
        """Generate export data for reporting"""
        export_data = []
        
        # Weekend postings export
        for posting in weekend_postings:
            export_data.append({
                'type': 'weekend_posting',
                'transaction_id': posting['transaction_id'],
                'document_number': posting['document_number'],
                'gl_account': posting['gl_account'],
                'amount': posting['amount'],
                'user_name': posting['user_name'],
                'posting_date': posting['posting_date'],
                'day_name': posting['day_name'],
                'risk_score': posting['risk_score']
            })
        
        # Unusual days export
        for unusual in unusual_days:
            export_data.append({
                'type': 'unusual_day',
                'day_name': unusual['day_name'],
                'transaction_count': unusual['transaction_count'],
                'deviation_percentage': unusual['deviation_percentage'],
                'unusual_type': unusual['unusual_type']
            })
        
        return export_data
    
    def _get_day_name(self, day_of_week):
        """Get day name from day of week number"""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return days[day_of_week] if 0 <= day_of_week <= 6 else 'Unknown'
    
    def _get_fs_line_from_account(self, account_id):
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
    
    def _calculate_weekend_risk_score(self, transaction):
        """Calculate risk score for weekend posting"""
        risk_score = 50.0  # Base risk for weekend posting
        
        # Increase risk for high value transactions
        if float(transaction.amount_local_currency) > 1000000:
            risk_score += 30.0
        
        # Increase risk for unusual transaction types
        if transaction.transaction_type == 'DEBIT':
            risk_score += 10.0
        
        # Increase risk for month-end or year-end
        if transaction.posting_date:
            if transaction.posting_date.day >= 25:
                risk_score += 10.0
            if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
                risk_score += 10.0
        
        return min(risk_score, 100.0)
    
    def _get_risk_level(self, risk_score):
        """Get risk level based on score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_risk_recommendations(self, risk_score, weekend_count, unusual_count):
        """Get recommendations based on risk assessment"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.extend([
                'Immediate investigation of weekend postings required',
                'Review all weekend transactions for validity',
                'Implement weekend posting restrictions',
                'Monitor user activity patterns closely'
            ])
        elif risk_score >= 60:
            recommendations.extend([
                'Investigate weekend posting patterns',
                'Review unusual day activity',
                'Consider implementing posting time restrictions',
                'Monitor high-value weekend transactions'
            ])
        elif risk_score >= 30:
            recommendations.extend([
                'Monitor weekend posting trends',
                'Review unusual day patterns',
                'Consider implementing controls for weekend postings'
            ])
        else:
            recommendations.append('Normal weekend posting patterns detected')
        
        if weekend_count > 0:
            recommendations.append(f'Found {weekend_count} weekend transactions requiring review')
        
        if unusual_count > 0:
            recommendations.append(f'Detected {unusual_count} unusual day patterns')
        
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