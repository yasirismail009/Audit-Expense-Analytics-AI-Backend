"""
General Analysis Module for SAP GL Posting Analysis System

This module handles the first type of analysis:
- Trial Balance calculations (Total Debit, Total Credit, Net)
- GL Account summaries with debits, credits, and balances
- User activity summaries per GL account
- Statistical calculations (mean, standard deviation, etc.)
- Chart data generation for visualizations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Q, Count, Sum, Avg, Min, Max, StdDev
from django.utils import timezone
import logging
from calendar import monthrange
import holidays
from django.db import models
from .models import SAPGLPosting, GLAccount, GeneralAnalysisResult

logger = logging.getLogger(__name__)

class GeneralAnalyzer:
    """Main analyzer for general analysis including trial balance, GL account summaries, and statistical calculations"""
    
    def __init__(self):
        self.analysis_config = {
            'currency': 'SAR',
            'decimal_places': 2,
            'statistical_measures': ['mean', 'median', 'std', 'min', 'max', 'q1', 'q3'],
            'chart_types': ['trial_balance', 'account_distribution', 'user_activity', 'amount_distribution']
        }
    
    def run_general_analysis(self, transactions, data_file, processing_job=None):
        """
        Run comprehensive general analysis on transactions
        
        Args:
            transactions: QuerySet of SAPGLPosting objects
            data_file: DataFile object
            processing_job: FileProcessingJob object (optional)
            
        Returns:
            dict: Complete general analysis results
        """
        try:
            logger.info(f"Starting general analysis for file: {data_file.file_name}")
            start_time = timezone.now()
            
            # Convert to list for processing
            transaction_list = list(transactions)
            
            # 1. Trial Balance Summary
            trial_balance_summary = self._calculate_trial_balance(transaction_list)
            
            # 2. GL Account Summaries
            gl_account_summaries = self._generate_gl_account_summaries(transaction_list)
            
            # 3. User Summaries per GL Account
            user_summaries = self._generate_user_summaries(transaction_list)
            
            # 4. Statistical Calculations
            statistical_calculations = self._calculate_statistics(transaction_list)
            
            # 5. Chart Data
            chart_data = self._generate_chart_data(transaction_list, trial_balance_summary, gl_account_summaries)
            
            # 6. Export Data
            export_data = self._generate_export_data(transaction_list, trial_balance_summary, gl_account_summaries)
            
            # Calculate processing duration
            processing_duration = (timezone.now() - start_time).total_seconds()
            
            # Create analysis result
            analysis_result = {
                'trial_balance_summary': trial_balance_summary,
                'gl_account_summaries': gl_account_summaries,
                'user_summaries': user_summaries,
                'statistical_calculations': statistical_calculations,
                'chart_data': chart_data,
                'export_data': export_data
            }
            
            # Save to database
            general_analysis = GeneralAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                processing_duration=processing_duration,
                **analysis_result
            )
            
            logger.info(f"General analysis completed for file: {data_file.file_name} in {processing_duration:.2f} seconds")
            
            return {
                'analysis_id': str(general_analysis.id),
                'processing_duration': processing_duration,
                'results': analysis_result
            }
            
        except Exception as e:
            logger.error(f"Error in general analysis: {e}")
            raise
    
    def _calculate_trial_balance(self, transactions):
        """Calculate comprehensive trial balance summary"""
        if not transactions:
            return {
                'total_debits': 0.0,
                'total_credits': 0.0,
                'net_balance': 0.0,
                'debit_count': 0,
                'credit_count': 0,
                'total_transactions': 0,
                'currency': self.analysis_config['currency']
            }
        
        # Calculate totals
        total_debits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'DEBIT')
        total_credits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'CREDIT')
        net_balance = total_debits - total_credits
        
        # Count transactions
        debit_count = sum(1 for t in transactions if t.transaction_type == 'DEBIT')
        credit_count = sum(1 for t in transactions if t.transaction_type == 'CREDIT')
        total_transactions = len(transactions)
        
        # Get currency
        currency = transactions[0].local_currency if transactions else self.analysis_config['currency']
        
        return {
            'total_debits': float(total_debits),
            'total_credits': float(total_credits),
            'net_balance': float(net_balance),
            'debit_count': debit_count,
            'credit_count': credit_count,
            'total_transactions': total_transactions,
            'currency': currency,
            'debit_credit_ratio': float(total_debits / total_credits) if total_credits > 0 else None,
            'credit_debit_ratio': float(total_credits / total_debits) if total_debits > 0 else None,
            'average_debit_amount': float(total_debits / debit_count) if debit_count > 0 else 0,
            'average_credit_amount': float(total_credits / credit_count) if credit_count > 0 else 0
        }
    
    def _generate_gl_account_summaries(self, transactions):
        """Generate detailed GL account summaries with debits, credits, and balances"""
        if not transactions:
            return []
        
        # Group transactions by GL account
        account_data = {}
        
        for transaction in transactions:
            account_id = transaction.gl_account
            
            if account_id not in account_data:
                account_data[account_id] = {
                    'account_id': account_id,
                    'total_debits': Decimal('0.00'),
                    'total_credits': Decimal('0.00'),
                    'debit_count': 0,
                    'credit_count': 0,
                    'transactions': []
                }
            
            account_data[account_id]['transactions'].append(transaction)
            
            if transaction.transaction_type == 'DEBIT':
                account_data[account_id]['total_debits'] += transaction.amount_local_currency
                account_data[account_id]['debit_count'] += 1
            else:
                account_data[account_id]['total_credits'] += transaction.amount_local_currency
                account_data[account_id]['credit_count'] += 1
        
        # Generate summaries for each account
        gl_account_summaries = []
        
        for account_id, data in account_data.items():
            # Get account details from GLAccount model
            try:
                gl_account = GLAccount.objects.get(account_id=account_id)
                account_name = gl_account.account_name
                account_type = gl_account.account_type
                account_category = gl_account.account_category
                normal_balance = gl_account.normal_balance
            except GLAccount.DoesNotExist:
                account_name = f"Account {account_id}"
                account_type = "Unknown"
                account_category = "Unknown"
                normal_balance = "DEBIT"
            
            # Calculate trial balance
            trial_balance = data['total_debits'] - data['total_credits']
            
            # Calculate trading equity (for equity accounts)
            if account_id.startswith('3'):  # Equity accounts
                trading_equity = data['total_credits'] - data['total_debits']
            else:
                trading_equity = trial_balance
            
            # Calculate additional statistics
            amounts = [t.amount_local_currency for t in data['transactions']]
            avg_amount = sum(amounts) / len(amounts) if amounts else Decimal('0.00')
            
            # Determine balance type
            if trial_balance > 0:
                balance_type = 'DEBIT'
            elif trial_balance < 0:
                balance_type = 'CREDIT'
            else:
                balance_type = 'ZERO'
            
            # Check if balance is normal
            is_normal_balance = (
                (normal_balance == 'DEBIT' and trial_balance >= 0) or
                (normal_balance == 'CREDIT' and trial_balance <= 0) or
                trial_balance == 0
            )
            
            account_summary = {
                'account_id': account_id,
                'account_name': account_name,
                'account_type': account_type,
                'account_category': account_category,
                'normal_balance': normal_balance,
                'currency': transactions[0].local_currency,
                
                # Balance information
                'trial_balance': float(trial_balance),
                'trading_equity': float(trading_equity),
                'total_debits': float(data['total_debits']),
                'total_credits': float(data['total_credits']),
                'balance_type': balance_type,
                'is_normal_balance': is_normal_balance,
                
                # Transaction statistics
                'transaction_count': len(data['transactions']),
                'debit_count': data['debit_count'],
                'credit_count': data['credit_count'],
                'avg_amount': float(avg_amount),
                
                # Additional metrics
                'debit_credit_ratio': float(data['total_debits'] / data['total_credits']) if data['total_credits'] > 0 else None,
                'avg_debit_amount': float(data['total_debits'] / data['debit_count']) if data['debit_count'] > 0 else 0,
                'avg_credit_amount': float(data['total_credits'] / data['credit_count']) if data['credit_count'] > 0 else 0,
                'credit_debit_ratio': float(data['total_credits'] / data['total_debits']) if data['total_debits'] > 0 else None
            }
            
            gl_account_summaries.append(account_summary)
        
        # Sort by absolute trial balance (highest first)
        gl_account_summaries.sort(key=lambda x: abs(x['trial_balance']), reverse=True)
        
        return gl_account_summaries
    
    def _generate_user_summaries(self, transactions):
        """Generate user activity summaries per GL account"""
        if not transactions:
            return []
        
        # Group by user and account
        user_account_data = {}
        
        for transaction in transactions:
            user_name = transaction.user_name
            account_id = transaction.gl_account
            
            key = f"{user_name}_{account_id}"
            
            if key not in user_account_data:
                user_account_data[key] = {
                    'user_name': user_name,
                    'account_id': account_id,
                    'total_debits': Decimal('0.00'),
                    'total_credits': Decimal('0.00'),
                    'debit_count': 0,
                    'credit_count': 0,
                    'transactions': []
                }
            
            user_account_data[key]['transactions'].append(transaction)
            
            if transaction.transaction_type == 'DEBIT':
                user_account_data[key]['total_debits'] += transaction.amount_local_currency
                user_account_data[key]['debit_count'] += 1
            else:
                user_account_data[key]['total_credits'] += transaction.amount_local_currency
                user_account_data[key]['credit_count'] += 1
        
        # Generate user summaries
        user_summaries = []
        
        for key, data in user_account_data.items():
            # Get account details
            try:
                gl_account = GLAccount.objects.get(account_id=data['account_id'])
                account_name = gl_account.account_name
                account_type = gl_account.account_type
            except GLAccount.DoesNotExist:
                account_name = f"Account {data['account_id']}"
                account_type = "Unknown"
            
            # Calculate trial balance
            trial_balance = data['total_debits'] - data['total_credits']
            
            # Calculate average amounts
            avg_debit = float(data['total_debits'] / data['debit_count']) if data['debit_count'] > 0 else 0
            avg_credit = float(data['total_credits'] / data['credit_count']) if data['credit_count'] > 0 else 0
            
            user_summary = {
                'user_name': data['user_name'],
                'account_id': data['account_id'],
                'account_name': account_name,
                'account_type': account_type,
                'currency': transactions[0].local_currency,
                
                # Activity summary
                'total_debits': float(data['total_debits']),
                'total_credits': float(data['total_credits']),
                'trial_balance': float(trial_balance),
                'debit_count': data['debit_count'],
                'credit_count': data['credit_count'],
                'total_transactions': len(data['transactions']),
                
                # Average amounts
                'avg_debit_amount': avg_debit,
                'avg_credit_amount': avg_credit,
                'avg_transaction_amount': float((data['total_debits'] + data['total_credits']) / len(data['transactions'])) if data['transactions'] else 0,
                
                # Ratios
                'debit_credit_ratio': float(data['total_debits'] / data['total_credits']) if data['total_credits'] > 0 else None,
                'credit_debit_ratio': float(data['total_credits'] / data['total_debits']) if data['total_debits'] > 0 else None
            }
            
            user_summaries.append(user_summary)
        
        # Sort by total transaction amount (highest first)
        user_summaries.sort(key=lambda x: x['total_debits'] + x['total_credits'], reverse=True)
        
        return user_summaries
    
    def _calculate_statistics(self, transactions):
        """Calculate comprehensive statistical measures"""
        if not transactions:
            return {
                'amount_statistics': {},
                'transaction_statistics': {},
                'account_statistics': {},
                'user_statistics': {}
            }
        
        # Amount statistics
        amounts = [float(t.amount_local_currency) for t in transactions]
        amounts_array = np.array(amounts)
        
        amount_statistics = {
            'count': len(amounts),
            'mean': float(np.mean(amounts_array)),
            'median': float(np.median(amounts_array)),
            'std': float(np.std(amounts_array)),
            'min': float(np.min(amounts_array)),
            'max': float(np.max(amounts_array)),
            'q1': float(np.percentile(amounts_array, 25)),
            'q3': float(np.percentile(amounts_array, 75)),
            'iqr': float(np.percentile(amounts_array, 75) - np.percentile(amounts_array, 25)),
            'skewness': float(self._calculate_skewness(amounts_array)),
            'kurtosis': float(self._calculate_kurtosis(amounts_array))
        }
        
        # Transaction statistics
        debit_amounts = [float(t.amount_local_currency) for t in transactions if t.transaction_type == 'DEBIT']
        credit_amounts = [float(t.amount_local_currency) for t in transactions if t.transaction_type == 'CREDIT']
        
        transaction_statistics = {
            'total_transactions': len(transactions),
            'debit_transactions': len(debit_amounts),
            'credit_transactions': len(credit_amounts),
            'debit_amounts': {
                'count': len(debit_amounts),
                'total': sum(debit_amounts),
                'mean': float(np.mean(debit_amounts)) if debit_amounts else 0,
                'median': float(np.median(debit_amounts)) if debit_amounts else 0,
                'std': float(np.std(debit_amounts)) if debit_amounts else 0,
                'min': float(np.min(debit_amounts)) if debit_amounts else 0,
                'max': float(np.max(debit_amounts)) if debit_amounts else 0
            },
            'credit_amounts': {
                'count': len(credit_amounts),
                'total': sum(credit_amounts),
                'mean': float(np.mean(credit_amounts)) if credit_amounts else 0,
                'median': float(np.median(credit_amounts)) if credit_amounts else 0,
                'std': float(np.std(credit_amounts)) if credit_amounts else 0,
                'min': float(np.min(credit_amounts)) if credit_amounts else 0,
                'max': float(np.max(credit_amounts)) if credit_amounts else 0
            }
        }
        
        # Account statistics
        unique_accounts = len(set(t.gl_account for t in transactions))
        unique_users = len(set(t.user_name for t in transactions))
        
        account_statistics = {
            'unique_accounts': unique_accounts,
            'unique_users': unique_users,
            'transactions_per_account': len(transactions) / unique_accounts if unique_accounts > 0 else 0,
            'transactions_per_user': len(transactions) / unique_users if unique_users > 0 else 0,
            'accounts_per_user': unique_accounts / unique_users if unique_users > 0 else 0
        }
        
        # User statistics
        user_transaction_counts = {}
        for transaction in transactions:
            user_name = transaction.user_name
            user_transaction_counts[user_name] = user_transaction_counts.get(user_name, 0) + 1
        
        user_transaction_counts_list = list(user_transaction_counts.values())
        
        user_statistics = {
            'total_users': unique_users,
            'active_users': len([count for count in user_transaction_counts_list if count > 1]),
            'single_transaction_users': len([count for count in user_transaction_counts_list if count == 1]),
            'transactions_per_user': {
                'mean': float(np.mean(user_transaction_counts_list)) if user_transaction_counts_list else 0,
                'median': float(np.median(user_transaction_counts_list)) if user_transaction_counts_list else 0,
                'std': float(np.std(user_transaction_counts_list)) if user_transaction_counts_list else 0,
                'min': float(np.min(user_transaction_counts_list)) if user_transaction_counts_list else 0,
                'max': float(np.max(user_transaction_counts_list)) if user_transaction_counts_list else 0
            }
        }
        
        return {
            'amount_statistics': amount_statistics,
            'transaction_statistics': transaction_statistics,
            'account_statistics': account_statistics,
            'user_statistics': user_statistics
        }
    
    def _calculate_skewness(self, data):
        """Calculate skewness of the data"""
        if len(data) < 3:
            return 0.0
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        skewness = np.mean(((data - mean) / std) ** 3)
        return skewness
    
    def _calculate_kurtosis(self, data):
        """Calculate kurtosis of the data"""
        if len(data) < 4:
            return 0.0
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        kurtosis = np.mean(((data - mean) / std) ** 4) - 3
        return kurtosis
    
    def _generate_chart_data(self, transactions, trial_balance_summary, gl_account_summaries):
        """Generate chart data for visualizations"""
        if not transactions:
            return {}
        
        # Trial Balance Chart
        trial_balance_chart = {
            'labels': ['Total Debits', 'Total Credits', 'Net Balance'],
            'data': [
                trial_balance_summary['total_debits'],
                trial_balance_summary['total_credits'],
                trial_balance_summary['net_balance']
            ],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56']
        }
        
        # Account Distribution Chart (Top 10 accounts by trial balance)
        top_accounts = gl_account_summaries[:10]
        account_distribution_chart = {
            'labels': [f"{acc['account_id']} - {acc['account_name'][:20]}" for acc in top_accounts],
            'data': [abs(acc['trial_balance']) for acc in top_accounts],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # User Activity Chart (Top 10 users by transaction count)
        user_transaction_counts = {}
        for transaction in transactions:
            user_name = transaction.user_name
            user_transaction_counts[user_name] = user_transaction_counts.get(user_name, 0) + 1
        
        top_users = sorted(user_transaction_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        user_activity_chart = {
            'labels': [user[0] for user in top_users],
            'data': [user[1] for user in top_users],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # Amount Distribution Chart (Histogram data)
        amounts = [float(t.amount_local_currency) for t in transactions]
        amount_ranges = [
            (0, 1000),
            (1000, 10000),
            (10000, 100000),
            (100000, 1000000),
            (1000000, float('inf'))
        ]
        
        amount_distribution = []
        for min_val, max_val in amount_ranges:
            if max_val == float('inf'):
                count = len([a for a in amounts if a >= min_val])
                label = f"{min_val:,}+"
            else:
                count = len([a for a in amounts if min_val <= a < max_val])
                label = f"{min_val:,}-{max_val:,}"
            amount_distribution.append({'label': label, 'count': count})
        
        amount_distribution_chart = {
            'labels': [item['label'] for item in amount_distribution],
            'data': [item['count'] for item in amount_distribution],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']
        }
        
        return {
            'trial_balance_chart': trial_balance_chart,
            'account_distribution_chart': account_distribution_chart,
            'user_activity_chart': user_activity_chart,
            'amount_distribution_chart': amount_distribution_chart
        }
    
    def _generate_export_data(self, transactions, trial_balance_summary, gl_account_summaries):
        """Generate export-ready data"""
        export_data = {
            'trial_balance_summary': trial_balance_summary,
            'gl_account_summaries': gl_account_summaries,
            'export_timestamp': timezone.now().isoformat(),
            'total_records': len(transactions)
        }
        
        return export_data 