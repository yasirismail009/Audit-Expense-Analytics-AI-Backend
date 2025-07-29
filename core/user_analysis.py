#!/usr/bin/env python3
"""
User Analysis Module

This module provides comprehensive user analysis functionality to identify:
- Transactions posted by users of interest
- Unexpected user activity patterns
- User risk assessment
- User activity anomalies

Charts and Analysis:
- Number of Transaction per User vs. Avg. Transaction Posted
- Debit Value Per User vs. Avg. Debit Value  
- Number of Unique Users per Account
- Number of Unique Users per FS Line
- Listing by User and transactions of users
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

class UserAnalyzer:
    """User Analysis class for identifying user activity patterns and anomalies"""
    
    def __init__(self):
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        try:
            django.setup()
        except:
            pass
        
        from .models import SAPGLPosting, UserAnalysisResult
        
        self.SAPGLPosting = SAPGLPosting
        self.UserAnalysisResult = UserAnalysisResult
        
        # Analysis configuration
        self.analysis_config = {
            'currency': 'SAR',
            'high_value_threshold': 100000,  # High value transaction threshold
            'anomaly_threshold': 2.0,  # Standard deviations for anomaly detection
            'risk_weights': {
                'transaction_frequency': 0.25,
                'amount_patterns': 0.30,
                'account_diversity': 0.20,
                'temporal_patterns': 0.15,
                'value_anomalies': 0.10
            }
        }
    
    def run_user_analysis(self, data_file, processing_job=None):
        """
        Run comprehensive user analysis
        
        Args:
            data_file: DataFile object
            processing_job: FileProcessingJob object (optional)
            
        Returns:
            dict: Complete user analysis results
        """
        try:
            logger.info(f"Starting user analysis for file: {data_file.file_name}")
            start_time = timezone.now()
            
            # Get all transactions for this file
            transactions = self.SAPGLPosting.objects.filter(
                document_number__in=data_file.get_transaction_document_numbers()
            )
            
            # 1. User Transaction Summary
            user_transaction_summary = self._generate_user_transaction_summary(transactions)
            
            # 2. User Debit Analysis
            user_debit_analysis = self._analyze_user_debit_patterns(transactions)
            
            # 3. User Account Distribution
            user_account_distribution = self._analyze_user_account_distribution(transactions)
            
            # 4. User FS Line Distribution
            user_fs_line_distribution = self._analyze_user_fs_line_distribution(transactions)
            
            # 5. User Anomalies Detection
            user_anomalies = self._detect_user_anomalies(transactions, user_transaction_summary)
            
            # 6. User Risk Assessment
            user_risk_assessment = self._assess_user_risk(transactions, user_transaction_summary, user_anomalies)
            
            # 7. User Patterns Analysis
            user_patterns = self._analyze_user_patterns(transactions, user_transaction_summary)
            
            # 8. Chart Data Generation
            chart_data = self._generate_chart_data(
                user_transaction_summary, 
                user_debit_analysis, 
                user_account_distribution, 
                user_fs_line_distribution,
                user_anomalies
            )
            
            # 9. Export Data
            export_data = self._generate_export_data(
                user_transaction_summary, 
                user_debit_analysis, 
                user_anomalies, 
                user_risk_assessment
            )
            
            # Calculate processing duration
            processing_duration = (timezone.now() - start_time).total_seconds()
            
            # Create analysis result
            analysis_result = {
                'analysis_info': {
                    'total_users': len(user_transaction_summary),
                    'total_transactions': transactions.count(),
                    'analysis_date': timezone.now().isoformat(),
                    'processing_duration': processing_duration
                },
                'user_transaction_summary': user_transaction_summary,
                'user_debit_analysis': user_debit_analysis,
                'user_account_distribution': user_account_distribution,
                'user_fs_line_distribution': user_fs_line_distribution,
                'user_anomalies': user_anomalies,
                'user_risk_assessment': user_risk_assessment,
                'user_patterns': user_patterns,
                'chart_data': chart_data,
                'export_data': export_data
            }
            
            # Convert any remaining Decimal objects to float for JSON serialization
            analysis_result = self._convert_decimals_to_float(analysis_result)
            
            # Save to database
            user_analysis = self.UserAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                processing_duration=processing_duration,
                **analysis_result
            )
            
            logger.info(f"User analysis completed for file: {data_file.file_name} in {processing_duration:.2f} seconds")
            
            return {
                'analysis_id': str(user_analysis.id),
                'processing_duration': processing_duration,
                'results': analysis_result
            }
            
        except Exception as e:
            logger.error(f"Error in user analysis: {e}")
            raise
    
    def _generate_user_transaction_summary(self, transactions):
        """Generate summary of transactions per user"""
        if not transactions:
            return []
        
        # Group transactions by user
        user_stats = transactions.values('user_name').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            avg_amount=Avg('amount_local_currency'),
            max_amount=Max('amount_local_currency'),
            min_amount=Min('amount_local_currency'),
            unique_accounts=Count('gl_account', distinct=True),
            unique_documents=Count('document_number', distinct=True),
            debit_count=Count('id', filter=Q(transaction_type='DEBIT')),
            credit_count=Count('id', filter=Q(transaction_type='CREDIT')),
            debit_amount=Sum('amount_local_currency', filter=Q(transaction_type='DEBIT')),
            credit_amount=Sum('amount_local_currency', filter=Q(transaction_type='CREDIT'))
        ).order_by('-transaction_count')
        
        # Calculate overall averages for comparison
        overall_avg_transactions = transactions.count() / len(user_stats) if user_stats else 0
        overall_avg_amount = transactions.aggregate(avg=Avg('amount_local_currency'))['avg'] or Decimal('0.00')
        
        user_summary = []
        for user_stat in user_stats:
            user_data = {
                'user_name': user_stat['user_name'],
                'transaction_count': user_stat['transaction_count'],
                'total_amount': float(user_stat['total_amount'] or 0),
                'avg_amount': float(user_stat['avg_amount'] or 0),
                'max_amount': float(user_stat['max_amount'] or 0),
                'min_amount': float(user_stat['min_amount'] or 0),
                'unique_accounts': user_stat['unique_accounts'],
                'unique_documents': user_stat['unique_documents'],
                'debit_count': user_stat['debit_count'],
                'credit_count': user_stat['credit_count'],
                'debit_amount': float(user_stat['debit_amount'] or 0),
                'credit_amount': float(user_stat['credit_amount'] or 0),
                'vs_avg_transactions': user_stat['transaction_count'] - overall_avg_transactions,
                'vs_avg_amount': float(user_stat['avg_amount'] or 0) - float(overall_avg_amount),
                'transaction_ratio': user_stat['transaction_count'] / overall_avg_transactions if overall_avg_transactions > 0 else 0,
                'amount_ratio': float(user_stat['avg_amount'] or 0) / float(overall_avg_amount) if overall_avg_amount > 0 else 0
            }
            user_summary.append(user_data)
        
        return user_summary
    
    def _analyze_user_debit_patterns(self, transactions):
        """Analyze debit value patterns per user"""
        if not transactions:
            return []
        
        # Get debit transactions only
        debit_transactions = transactions.filter(transaction_type='DEBIT')
        
        if not debit_transactions:
            return []
        
        # Group by user
        user_debit_stats = debit_transactions.values('user_name').annotate(
            debit_count=Count('id'),
            total_debit=Sum('amount_local_currency'),
            avg_debit=Avg('amount_local_currency'),
            max_debit=Max('amount_local_currency'),
            min_debit=Min('amount_local_currency'),
            unique_accounts=Count('gl_account', distinct=True)
        ).order_by('-total_debit')
        
        # Calculate overall averages
        overall_avg_debit = debit_transactions.aggregate(avg=Avg('amount_local_currency'))['avg'] or Decimal('0.00')
        overall_total_debit = debit_transactions.aggregate(total=Sum('amount_local_currency'))['total'] or Decimal('0.00')
        
        debit_analysis = []
        for user_stat in user_debit_stats:
            user_data = {
                'user_name': user_stat['user_name'],
                'debit_count': user_stat['debit_count'],
                'total_debit': float(user_stat['total_debit'] or 0),
                'avg_debit': float(user_stat['avg_debit'] or 0),
                'max_debit': float(user_stat['max_debit'] or 0),
                'min_debit': float(user_stat['min_debit'] or 0),
                'unique_accounts': user_stat['unique_accounts'],
                'vs_avg_debit': float(user_stat['avg_debit'] or 0) - float(overall_avg_debit),
                'debit_percentage': (float(user_stat['total_debit'] or 0) / float(overall_total_debit) * 100) if overall_total_debit > 0 else 0,
                'debit_ratio': float(user_stat['avg_debit'] or 0) / float(overall_avg_debit) if overall_avg_debit > 0 else 0
            }
            debit_analysis.append(user_data)
        
        return debit_analysis
    
    def _analyze_user_account_distribution(self, transactions):
        """Analyze number of unique users per account"""
        if not transactions:
            return []
        
        # Group by account and count unique users
        account_user_stats = transactions.values('gl_account').annotate(
            unique_users=Count('user_name', distinct=True),
            total_transactions=Count('id'),
            total_amount=Sum('amount_local_currency')
        ).order_by('-unique_users')
        
        account_distribution = []
        for account_stat in account_user_stats:
            account_data = {
                'gl_account': account_stat['gl_account'],
                'unique_users': account_stat['unique_users'],
                'total_transactions': account_stat['total_transactions'],
                'total_amount': float(account_stat['total_amount'] or 0),
                'avg_transactions_per_user': account_stat['total_transactions'] / account_stat['unique_users'] if account_stat['unique_users'] > 0 else 0,
                'avg_amount_per_user': float(account_stat['total_amount'] or 0) / account_stat['unique_users'] if account_stat['unique_users'] > 0 else 0
            }
            account_distribution.append(account_data)
        
        return account_distribution
    
    def _analyze_user_fs_line_distribution(self, transactions):
        """Analyze number of unique users per FS line (account category)"""
        if not transactions:
            return []
        
        # Get account categories (simplified FS line mapping)
        fs_line_mapping = self._get_fs_line_mapping()
        
        # Add FS line to transactions
        transactions_with_fs = []
        for transaction in transactions:
            fs_line = fs_line_mapping.get(transaction.gl_account[:4], 'Other')
            transactions_with_fs.append({
                'user_name': transaction.user_name,
                'gl_account': transaction.gl_account,
                'fs_line': fs_line,
                'amount': transaction.amount_local_currency
            })
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(transactions_with_fs)
        
        if df.empty:
            return []
        
        # Group by FS line and count unique users
        fs_line_stats = df.groupby('fs_line').agg({
            'user_name': 'nunique',
            'gl_account': 'nunique',
            'amount': ['sum', 'count']
        }).reset_index()
        
        fs_line_stats.columns = ['fs_line', 'unique_users', 'unique_accounts', 'total_amount', 'total_transactions']
        
        fs_line_distribution = []
        for _, row in fs_line_stats.iterrows():
            fs_data = {
                'fs_line': row['fs_line'],
                'unique_users': int(row['unique_users']),
                'unique_accounts': int(row['unique_accounts']),
                'total_amount': float(row['total_amount']),
                'total_transactions': int(row['total_transactions']),
                'avg_users_per_account': row['unique_users'] / row['unique_accounts'] if row['unique_accounts'] > 0 else 0,
                'avg_amount_per_user': row['total_amount'] / row['unique_users'] if row['unique_users'] > 0 else 0
            }
            fs_line_distribution.append(fs_data)
        
        return fs_line_distribution
    
    def _get_fs_line_mapping(self):
        """Get mapping of GL accounts to FS lines"""
        # Simplified FS line mapping based on account ranges
        return {
            '1000': 'Assets',
            '1100': 'Assets',
            '1200': 'Assets',
            '1300': 'Assets',
            '1400': 'Assets',
            '1500': 'Assets',
            '2000': 'Liabilities',
            '2100': 'Liabilities',
            '2200': 'Liabilities',
            '2300': 'Liabilities',
            '3000': 'Equity',
            '3100': 'Equity',
            '3200': 'Equity',
            '4000': 'Revenue',
            '4100': 'Revenue',
            '4200': 'Revenue',
            '5000': 'Expenses',
            '5100': 'Expenses',
            '5200': 'Expenses',
            '5300': 'Expenses',
            '5400': 'Expenses',
            '5500': 'Expenses',
            '6000': 'Other',
            '7000': 'Other',
            '8000': 'Other',
            '9000': 'Other'
        }
    
    def _detect_user_anomalies(self, transactions, user_summary):
        """Detect user anomalies using statistical methods and ML models"""
        if not user_summary:
            return []
        
        anomalies = []
        
        # 1. Statistical anomaly detection (always run)
        statistical_anomalies = self._detect_statistical_anomalies(user_summary)
        anomalies.extend(statistical_anomalies)
        
        # 2. ML-based anomaly detection (with automatic training)
        ml_anomalies_found = 0
        try:
            from .specialized_ml_models import SpecializedAnomalyDetector
            ml_detector = SpecializedAnomalyDetector()
            
            # The detect_user_anomalies_ml method now handles training automatically
            ml_anomalies = ml_detector.detect_user_anomalies_ml(transactions)
            ml_anomalies_found = len(ml_anomalies)
            
            # Convert ML anomalies to our format
            for ml_anomaly in ml_anomalies:
                anomalies.append({
                    'user_name': ml_anomaly['user_name'],
                    'anomaly_type': ml_anomaly['anomaly_type'],
                    'anomaly_value': ml_anomaly['anomaly_score'],
                    'expected_range': 'ML-detected',
                    'severity': ml_anomaly['severity'],
                    'description': ml_anomaly['description'],
                    'ml_detected': True,
                    'features': ml_anomaly.get('features', {})
                })
                
        except Exception as e:
            logger.info(f"ML anomaly detection not available for this dataset: {e}")
        
        # Log analysis method summary
        statistical_count = len([a for a in anomalies if not a.get('ml_detected', False)])
        logger.info(f"User anomaly detection completed: {statistical_count} statistical anomalies, {ml_anomalies_found} ML anomalies")
        
        return anomalies
    
    def _detect_statistical_anomalies(self, user_summary):
        """Detect user anomalies using statistical methods"""
        if not user_summary:
            return []
        
        anomalies = []
        
        # Convert to DataFrame for statistical analysis
        df = pd.DataFrame(user_summary)
        
        if df.empty:
            return anomalies
        
        # 1. Transaction count anomalies
        transaction_mean = df['transaction_count'].mean()
        transaction_std = df['transaction_count'].std()
        transaction_threshold = self.analysis_config['anomaly_threshold']
        
        high_transaction_users = df[df['transaction_count'] > (transaction_mean + transaction_threshold * transaction_std)]
        low_transaction_users = df[df['transaction_count'] < (transaction_mean - transaction_threshold * transaction_std)]
        
        for _, user in high_transaction_users.iterrows():
            anomalies.append({
                'user_name': user['user_name'],
                'anomaly_type': 'high_transaction_frequency',
                'anomaly_value': user['transaction_count'],
                'expected_range': f"0 - {transaction_mean + transaction_threshold * transaction_std:.0f}",
                'severity': 'high' if user['transaction_count'] > (transaction_mean + 3 * transaction_std) else 'medium',
                'description': f"User has {user['transaction_count']} transactions, significantly above average",
                'ml_detected': False
            })
        
        for _, user in low_transaction_users.iterrows():
            anomalies.append({
                'user_name': user['user_name'],
                'anomaly_type': 'low_transaction_frequency',
                'anomaly_value': user['transaction_count'],
                'expected_range': f"{max(0, transaction_mean - transaction_threshold * transaction_std):.0f} - {transaction_mean + transaction_threshold * transaction_std:.0f}",
                'severity': 'medium',
                'description': f"User has {user['transaction_count']} transactions, significantly below average",
                'ml_detected': False
            })
        
        # 2. Amount anomalies
        amount_mean = df['avg_amount'].mean()
        amount_std = df['avg_amount'].std()
        
        high_amount_users = df[df['avg_amount'] > (amount_mean + transaction_threshold * amount_std)]
        
        for _, user in high_amount_users.iterrows():
            anomalies.append({
                'user_name': user['user_name'],
                'anomaly_type': 'high_average_amount',
                'anomaly_value': user['avg_amount'],
                'expected_range': f"0 - {amount_mean + transaction_threshold * amount_std:.2f}",
                'severity': 'high' if user['avg_amount'] > (amount_mean + 3 * amount_std) else 'medium',
                'description': f"User has average transaction amount of {user['avg_amount']:.2f}, significantly above average",
                'ml_detected': False
            })
        
        # 3. Account diversity anomalies
        account_mean = df['unique_accounts'].mean()
        account_std = df['unique_accounts'].std()
        
        high_diversity_users = df[df['unique_accounts'] > (account_mean + transaction_threshold * account_std)]
        
        for _, user in high_diversity_users.iterrows():
            anomalies.append({
                'user_name': user['user_name'],
                'anomaly_type': 'high_account_diversity',
                'anomaly_value': user['unique_accounts'],
                'expected_range': f"0 - {account_mean + transaction_threshold * account_std:.0f}",
                'severity': 'medium',
                'description': f"User posts to {user['unique_accounts']} different accounts, above average diversity",
                'ml_detected': False
            })
        
        # 4. Debit/Credit ratio anomalies
        debit_ratio_mean = df['debit_count'].sum() / df['transaction_count'].sum()
        unusual_debit_users = df[abs(df['debit_count'] / df['transaction_count'] - debit_ratio_mean) > 0.3]
        
        for _, user in unusual_debit_users.iterrows():
            user_debit_ratio = user['debit_count'] / user['transaction_count']
            anomalies.append({
                'user_name': user['user_name'],
                'anomaly_type': 'unusual_debit_credit_pattern',
                'anomaly_value': user_debit_ratio,
                'expected_range': f"{max(0, debit_ratio_mean - 0.3):.2f} - {min(1, debit_ratio_mean + 0.3):.2f}",
                'severity': 'medium',
                'description': f"User has unusual debit/credit ratio of {user_debit_ratio:.2f}",
                'ml_detected': False
            })
        
        return anomalies
    
    def _assess_user_risk(self, transactions, user_summary, user_anomalies):
        """Assess risk level for each user"""
        if not user_summary:
            return {'user_risk_scores': [], 'risk_distribution': {}, 'high_risk_users': []}
        
        user_risk_scores = []
        risk_weights = self.analysis_config['risk_weights']
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(user_summary)
        
        if df.empty:
            return {'user_risk_scores': [], 'risk_distribution': {}, 'high_risk_users': []}
        
        # Calculate risk scores for each user
        for _, user in df.iterrows():
            risk_score = 0.0
            
            # 1. Transaction frequency risk
            transaction_ratio = user['transaction_ratio']
            if transaction_ratio > 2.0:
                risk_score += risk_weights['transaction_frequency'] * 100
            elif transaction_ratio > 1.5:
                risk_score += risk_weights['transaction_frequency'] * 50
            elif transaction_ratio < 0.5:
                risk_score += risk_weights['transaction_frequency'] * 30
            
            # 2. Amount pattern risk
            amount_ratio = user['amount_ratio']
            if amount_ratio > 2.0:
                risk_score += risk_weights['amount_patterns'] * 100
            elif amount_ratio > 1.5:
                risk_score += risk_weights['amount_patterns'] * 60
            elif amount_ratio < 0.5:
                risk_score += risk_weights['amount_patterns'] * 20
            
            # 3. Account diversity risk
            account_diversity = user['unique_accounts']
            if account_diversity > 10:
                risk_score += risk_weights['account_diversity'] * 80
            elif account_diversity > 5:
                risk_score += risk_weights['account_diversity'] * 40
            
            # 4. Check for anomalies
            user_anomaly_count = len([a for a in user_anomalies if a['user_name'] == user['user_name']])
            if user_anomaly_count > 0:
                risk_score += risk_weights['value_anomalies'] * (user_anomaly_count * 30)
            
            # Determine risk level
            if risk_score >= 70:
                risk_level = 'critical'
            elif risk_score >= 50:
                risk_level = 'high'
            elif risk_score >= 30:
                risk_level = 'medium'
            else:
                risk_level = 'low'
            
            user_risk_scores.append({
                'user_name': user['user_name'],
                'risk_score': round(risk_score, 2),
                'risk_level': risk_level,
                'transaction_count': user['transaction_count'],
                'avg_amount': user['avg_amount'],
                'unique_accounts': user['unique_accounts'],
                'anomaly_count': user_anomaly_count
            })
        
        # Calculate risk distribution
        risk_distribution = {
            'low': len([u for u in user_risk_scores if u['risk_level'] == 'low']),
            'medium': len([u for u in user_risk_scores if u['risk_level'] == 'medium']),
            'high': len([u for u in user_risk_scores if u['risk_level'] == 'high']),
            'critical': len([u for u in user_risk_scores if u['risk_level'] == 'critical'])
        }
        
        # Get high risk users
        high_risk_users = [u for u in user_risk_scores if u['risk_level'] in ['high', 'critical']]
        
        return {
            'user_risk_scores': user_risk_scores,
            'risk_distribution': risk_distribution,
            'high_risk_users': high_risk_users
        }
    
    def _analyze_user_patterns(self, transactions, user_summary):
        """Analyze user activity patterns and trends"""
        if not transactions or not user_summary:
            return {}
        
        # Convert to DataFrame
        df = pd.DataFrame(user_summary)
        
        if df.empty:
            return {}
        
        patterns = {
            'transaction_patterns': {
                'most_active_users': df.nlargest(5, 'transaction_count')[['user_name', 'transaction_count']].to_dict('records'),
                'highest_value_users': df.nlargest(5, 'total_amount')[['user_name', 'total_amount']].to_dict('records'),
                'most_diverse_users': df.nlargest(5, 'unique_accounts')[['user_name', 'unique_accounts']].to_dict('records')
            },
            'statistical_patterns': {
                'transaction_count_stats': {
                    'mean': float(df['transaction_count'].mean()),
                    'median': float(df['transaction_count'].median()),
                    'std': float(df['transaction_count'].std()),
                    'min': int(df['transaction_count'].min()),
                    'max': int(df['transaction_count'].max())
                },
                'amount_stats': {
                    'mean': float(df['avg_amount'].mean()),
                    'median': float(df['avg_amount'].median()),
                    'std': float(df['avg_amount'].std()),
                    'min': float(df['avg_amount'].min()),
                    'max': float(df['avg_amount'].max())
                }
            }
        }
        
        return patterns
    
    def _generate_chart_data(self, user_summary, user_debit_analysis, account_distribution, fs_line_distribution, user_anomalies):
        """Generate chart data for visualizations"""
        if not user_summary:
            return {}
        
        # 1. Transaction per User vs Average
        transaction_chart = {
            'labels': [user['user_name'] for user in user_summary[:20]],  # Top 20 users
            'datasets': [
                {
                    'label': 'Transactions per User',
                    'data': [user['transaction_count'] for user in user_summary[:20]],
                    'backgroundColor': 'rgba(54, 162, 235, 0.5)',
                    'borderColor': 'rgba(54, 162, 235, 1)',
                    'borderWidth': 1
                },
                {
                    'label': 'Average Transactions',
                    'data': [user_summary[0]['vs_avg_transactions'] + user_summary[0]['transaction_count']] * len(user_summary[:20]),
                    'backgroundColor': 'rgba(255, 99, 132, 0.5)',
                    'borderColor': 'rgba(255, 99, 132, 1)',
                    'borderWidth': 1,
                    'type': 'line'
                }
            ]
        }
        
        # 2. Debit Value per User vs Average
        debit_chart = {
            'labels': [user['user_name'] for user in user_debit_analysis[:20]],
            'datasets': [
                {
                    'label': 'Debit Value per User',
                    'data': [user['total_debit'] for user in user_debit_analysis[:20]],
                    'backgroundColor': 'rgba(75, 192, 192, 0.5)',
                    'borderColor': 'rgba(75, 192, 192, 1)',
                    'borderWidth': 1
                }
            ]
        }
        
        # 3. Unique Users per Account
        account_chart = {
            'labels': [account['gl_account'] for account in account_distribution[:15]],
            'datasets': [
                {
                    'label': 'Unique Users per Account',
                    'data': [account['unique_users'] for account in account_distribution[:15]],
                    'backgroundColor': 'rgba(255, 159, 64, 0.5)',
                    'borderColor': 'rgba(255, 159, 64, 1)',
                    'borderWidth': 1
                }
            ]
        }
        
        # 4. Unique Users per FS Line
        fs_line_chart = {
            'labels': [fs['fs_line'] for fs in fs_line_distribution],
            'datasets': [
                {
                    'label': 'Unique Users per FS Line',
                    'data': [fs['unique_users'] for fs in fs_line_distribution],
                    'backgroundColor': 'rgba(153, 102, 255, 0.5)',
                    'borderColor': 'rgba(153, 102, 255, 1)',
                    'borderWidth': 1
                }
            ]
        }
        
        # 5. Anomaly Distribution
        anomaly_types = {}
        for anomaly in user_anomalies:
            anomaly_type = anomaly['anomaly_type']
            if anomaly_type not in anomaly_types:
                anomaly_types[anomaly_type] = 0
            anomaly_types[anomaly_type] += 1
        
        anomaly_chart = {
            'labels': list(anomaly_types.keys()),
            'datasets': [
                {
                    'label': 'Anomaly Count by Type',
                    'data': list(anomaly_types.values()),
                    'backgroundColor': [
                        'rgba(255, 99, 132, 0.5)',
                        'rgba(54, 162, 235, 0.5)',
                        'rgba(255, 205, 86, 0.5)',
                        'rgba(75, 192, 192, 0.5)'
                    ],
                    'borderColor': [
                        'rgba(255, 99, 132, 1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 205, 86, 1)',
                        'rgba(75, 192, 192, 1)'
                    ],
                    'borderWidth': 1
                }
            ]
        }
        
        return {
            'transaction_vs_average': transaction_chart,
            'debit_value_analysis': debit_chart,
            'users_per_account': account_chart,
            'users_per_fs_line': fs_line_chart,
            'anomaly_distribution': anomaly_chart
        }
    
    def _generate_export_data(self, user_summary, user_debit_analysis, user_anomalies, user_risk_assessment):
        """Generate export-ready data"""
        export_data = []
        
        # Add user summary data
        for user in user_summary:
            export_data.append({
                'data_type': 'user_summary',
                'user_name': user['user_name'],
                'transaction_count': user['transaction_count'],
                'total_amount': user['total_amount'],
                'avg_amount': user['avg_amount'],
                'unique_accounts': user['unique_accounts'],
                'debit_count': user['debit_count'],
                'credit_count': user['credit_count'],
                'debit_amount': user['debit_amount'],
                'credit_amount': user['credit_amount'],
                'vs_avg_transactions': user['vs_avg_transactions'],
                'vs_avg_amount': user['vs_avg_amount']
            })
        
        # Add user anomalies
        for anomaly in user_anomalies:
            export_data.append({
                'data_type': 'user_anomaly',
                'user_name': anomaly['user_name'],
                'anomaly_type': anomaly['anomaly_type'],
                'anomaly_value': anomaly['anomaly_value'],
                'severity': anomaly['severity'],
                'description': anomaly['description']
            })
        
        # Add user risk scores
        for user_risk in user_risk_assessment.get('user_risk_scores', []):
            export_data.append({
                'data_type': 'user_risk',
                'user_name': user_risk['user_name'],
                'risk_score': user_risk['risk_score'],
                'risk_level': user_risk['risk_level'],
                'transaction_count': user_risk['transaction_count'],
                'avg_amount': user_risk['avg_amount'],
                'unique_accounts': user_risk['unique_accounts'],
                'anomaly_count': user_risk['anomaly_count']
            })
        
        return export_data 

    def _convert_decimals_to_float(self, data):
        """Recursively converts Decimal objects to float in a dictionary."""
        if isinstance(data, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_decimals_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        return data 