"""
Enhanced Duplicate Analysis Model
Comprehensive duplicate detection and analysis with consolidated data structure
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Q, Count, Sum, Avg, Min, Max
from django.utils import timezone
import logging
from calendar import monthrange
import json

logger = logging.getLogger(__name__)

class EnhancedDuplicateAnalyzer:
    """
    Enhanced Duplicate Analysis with comprehensive data consolidation
    
    This test identifies Journal Lines which has the identical characteristics. 
    The classification for Duplicates are categorized as below:
    
    Type 1 Duplicate - Account Number + Amount
    Type 2 Duplicate - Account Number + Source + Amount
    Type 3 Duplicate - Account Number + User + Amount
    Type 4 Duplicate - Account Number + Posted Date + Amount
    Type 5 Duplicate - Account Number + Effective Date + Amount
    Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount
    """
    
    def __init__(self):
        self.duplicate_threshold = 2
        self.duplicate_types = [
            {
                'type': 'Type 1 Duplicate',
                'criteria': 'Account Number + Amount',
                'groupby_cols': ['gl_account', 'amount'],
                'risk_multiplier': 10,
                'description': 'Identical account number and amount'
            },
            {
                'type': 'Type 2 Duplicate',
                'criteria': 'Account Number + Source + Amount',
                'groupby_cols': ['gl_account', 'source', 'amount'],
                'risk_multiplier': 12,
                'description': 'Identical account number, source document type, and amount'
            },
            {
                'type': 'Type 3 Duplicate',
                'criteria': 'Account Number + User + Amount',
                'groupby_cols': ['gl_account', 'user_name', 'amount'],
                'risk_multiplier': 15,
                'description': 'Identical account number, user, and amount'
            },
            {
                'type': 'Type 4 Duplicate',
                'criteria': 'Account Number + Posted Date + Amount',
                'groupby_cols': ['gl_account', 'posting_date', 'amount'],
                'risk_multiplier': 18,
                'description': 'Identical account number, posting date, and amount'
            },
            {
                'type': 'Type 5 Duplicate',
                'criteria': 'Account Number + Effective Date + Amount',
                'groupby_cols': ['gl_account', 'document_date', 'amount'],
                'risk_multiplier': 20,
                'description': 'Identical account number, effective date, and amount'
            },
            {
                'type': 'Type 6 Duplicate',
                'criteria': 'Account Number + Effective Date + Posted Date + User + Source + Amount',
                'groupby_cols': ['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount'],
                'risk_multiplier': 25,
                'description': 'Identical account number, effective date, posting date, user, source, and amount'
            }
        ]
    
    def analyze_duplicates(self, transactions):
        """
        Comprehensive duplicate analysis with consolidated data structure
        
        Returns a single object containing:
        - List of Duplicate Analysis expense
        - Chart data
        - Breakdown of Duplicate Flags
        - Debit, Credit Amts and Journal Line Count per Duplicate and Month
        - Breakdown of Duplicates per Impacted User
        - Breakdown of Duplicates per Impacted FS Line
        - Slicer filters and dynamic counts
        - Summary Table for final test selections
        """
        
        if not transactions:
            return self._get_empty_analysis()
        
        # Convert transactions to DataFrame
        df = self._prepare_dataframe(transactions)
        
        # Detect duplicates by type
        duplicate_groups = self._detect_duplicate_groups(df)
        
        # Generate comprehensive analysis
        analysis = {
            'analysis_info': self._get_analysis_info(df),
            'duplicate_list': self._generate_duplicate_list(duplicate_groups, df),
            'chart_data': self._generate_chart_data(duplicate_groups, df),
            'breakdowns': self._generate_breakdowns(duplicate_groups, df),
            'slicer_filters': self._generate_slicer_filters(duplicate_groups, df),
            'summary_table': self._generate_summary_table(duplicate_groups, df),
            'export_data': self._generate_export_data(duplicate_groups, df),
            'detailed_insights': self._generate_detailed_insights(duplicate_groups, df)
        }
        
        return analysis
    
    def _get_empty_analysis(self):
        """Return empty analysis structure"""
        return {
            'analysis_info': {
                'total_transactions': 0,
                'total_duplicate_groups': 0,
                'total_duplicate_transactions': 0,
                'total_amount_involved': 0.0,
                'analysis_date': datetime.now().isoformat()
            },
            'duplicate_list': [],
            'chart_data': {
                'duplicate_type_chart': [],
                'monthly_trend_chart': [],
                'user_breakdown_chart': [],
                'fs_line_chart': [],
                'amount_distribution_chart': [],
                'risk_level_chart': []
            },
            'breakdowns': {
                'duplicate_flags': {},
                'debit_credit_monthly': {},
                'user_breakdown': {},
                'fs_line_breakdown': {},
                'type_breakdown': {},
                'risk_breakdown': {}
            },
            'slicer_filters': {
                'duplicate_types': [],
                'users': [],
                'gl_accounts': [],
                'date_ranges': [],
                'amount_ranges': [],
                'risk_levels': []
            },
            'summary_table': [],
            'export_data': [],
            'detailed_insights': {}
        }
    
    def _prepare_dataframe(self, transactions):
        """Prepare DataFrame from transactions"""
        data = []
        for t in transactions:
            data.append({
                'id': str(t.id),
                'gl_account': t.gl_account or 'UNKNOWN',
                'amount': float(t.amount_local_currency),
                'user_name': t.user_name,
                'posting_date': t.posting_date,
                'document_date': t.document_date,
                'document_number': t.document_number,
                'document_type': t.document_type,
                'source': t.document_type,
                'transaction_type': t.transaction_type,
                'text': t.text or '',
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period,
                'profit_center': t.profit_center or '',
                'cost_center': t.cost_center or '',
                'local_currency': t.local_currency or 'SAR'
            })
        
        df = pd.DataFrame(data)
        
        # Handle date columns
        for date_col in ['posting_date', 'document_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[f'{date_col}_str'] = df[date_col].dt.strftime('%Y-%m-%d')
        
        return df
    
    def _detect_duplicate_groups(self, df):
        """Detect duplicate groups by type"""
        duplicate_groups = []
        
        for dup_type in self.duplicate_types:
            groupby_cols = []
            for col in dup_type['groupby_cols']:
                if col in ['posting_date', 'document_date']:
                    groupby_cols.append(f'{col}_str')
                else:
                    groupby_cols.append(col)
            
            # Find groups with duplicates
            grouped = df.groupby(groupby_cols).filter(lambda x: len(x) >= self.duplicate_threshold)
            
            for _, group in grouped.groupby(groupby_cols):
                if len(group) >= self.duplicate_threshold:
                    duplicate_group = {
                        'type': dup_type['type'],
                        'criteria': dup_type['criteria'],
                        'description': dup_type['description'],
                        'group_key': groupby_cols,
                        'group_values': {col: group.iloc[0][col] for col in groupby_cols},
                        'transactions': group.to_dict('records'),
                        'count': len(group),
                        'amount': float(group.iloc[0]['amount']),
                        'total_amount': float(group['amount'].sum()),
                        'risk_score': min(len(group) * dup_type['risk_multiplier'], 100),
                        'debit_count': len(group[group['transaction_type'] == 'DEBIT']),
                        'credit_count': len(group[group['transaction_type'] == 'CREDIT']),
                        'debit_amount': float(group[group['transaction_type'] == 'DEBIT']['amount'].sum()),
                        'credit_amount': float(group[group['transaction_type'] == 'CREDIT']['amount'].sum()),
                        'unique_users': group['user_name'].nunique(),
                        'unique_documents': group['document_number'].nunique(),
                        'date_range': {
                            'min_date': group['posting_date'].min().isoformat() if pd.notna(group['posting_date'].min()) else None,
                            'max_date': group['posting_date'].max().isoformat() if pd.notna(group['posting_date'].max()) else None
                        }
                    }
                    duplicate_groups.append(duplicate_group)
        
        return duplicate_groups
    
    def _generate_duplicate_list(self, duplicate_groups, df):
        """Generate list of duplicate analysis expense"""
        duplicate_list = []
        
        for group in duplicate_groups:
            for transaction in group['transactions']:
                duplicate_entry = {
                    'duplicate_type': group['type'],
                    'duplicate_criteria': group['criteria'],
                    'gl_account': transaction['gl_account'],
                    'amount': transaction['amount'],
                    'duplicate_count': group['count'],
                    'risk_score': group['risk_score'],
                    'transaction_id': transaction['id'],
                    'document_number': transaction['document_number'],
                                    'posting_date': str(transaction['posting_date']) if transaction['posting_date'] else None,
                'document_date': str(transaction['document_date']) if transaction['document_date'] else None,
                    'user_name': transaction['user_name'],
                    'document_type': transaction['document_type'],
                    'transaction_type': transaction['transaction_type'],
                    'text': transaction['text'],
                    'fiscal_year': transaction['fiscal_year'],
                    'posting_period': transaction['posting_period'],
                    'profit_center': transaction['profit_center'],
                    'cost_center': transaction['cost_center'],
                    'local_currency': transaction['local_currency'],
                    'debit_count': group['debit_count'],
                    'credit_count': group['credit_count'],
                    'debit_amount': group['debit_amount'],
                    'credit_amount': group['credit_amount'],
                    'group_total_amount': group['total_amount'],
                    'unique_users_in_group': group['unique_users'],
                    'unique_documents_in_group': group['unique_documents']
                }
                duplicate_list.append(duplicate_entry)
        
        return duplicate_list
    
    def _generate_chart_data(self, duplicate_groups, df):
        """Generate comprehensive chart data"""
        
        # 1. Duplicate Type Chart
        type_counts = {}
        for group in duplicate_groups:
            dup_type = group['type']
            if dup_type not in type_counts:
                type_counts[dup_type] = {
                    'count': 0,
                    'transactions': 0,
                    'amount': 0.0
                }
            type_counts[dup_type]['count'] += 1
            type_counts[dup_type]['transactions'] += group['count']
            type_counts[dup_type]['amount'] += group['total_amount']
        
        duplicate_type_chart = [
            {
                'type': dup_type,
                'groups': data['count'],
                'transactions': data['transactions'],
                'total_amount': data['amount']
            }
            for dup_type, data in type_counts.items()
        ]
        
        # 2. Monthly Trend Chart
        monthly_data = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                # Handle posting_date safely - it could be a string, datetime, or timestamp
                posting_date = transaction['posting_date']
                if posting_date:
                    if hasattr(posting_date, 'strftime'):
                        month_key = posting_date.strftime('%Y-%m')
                    elif isinstance(posting_date, str):
                        month_key = posting_date[:7] if len(posting_date) >= 7 else 'Unknown'
                    else:
                        month_key = str(posting_date)[:7] if len(str(posting_date)) >= 7 else 'Unknown'
                else:
                    month_key = 'Unknown'
                
                if month_key not in monthly_data:
                    monthly_data[month_key] = {
                        'duplicate_groups': 0,
                        'transactions': 0,
                        'amount': 0.0,
                        'debit_amount': 0.0,
                        'credit_amount': 0.0
                    }
                monthly_data[month_key]['transactions'] += 1
                monthly_data[month_key]['amount'] += transaction['amount']
                if transaction['transaction_type'] == 'DEBIT':
                    monthly_data[month_key]['debit_amount'] += transaction['amount']
                else:
                    monthly_data[month_key]['credit_amount'] += transaction['amount']
        
        # Count groups per month
        for group in duplicate_groups:
            for transaction in group['transactions']:
                # Handle posting_date safely
                posting_date = transaction['posting_date']
                if posting_date:
                    if hasattr(posting_date, 'strftime'):
                        month_key = posting_date.strftime('%Y-%m')
                    elif isinstance(posting_date, str):
                        month_key = posting_date[:7] if len(posting_date) >= 7 else 'Unknown'
                    else:
                        month_key = str(posting_date)[:7] if len(str(posting_date)) >= 7 else 'Unknown'
                else:
                    month_key = 'Unknown'
                
                monthly_data[month_key]['duplicate_groups'] += 1
                break  # Count group only once per month
        
        monthly_trend_chart = [
            {
                'month': month,
                'duplicate_groups': data['duplicate_groups'],
                'transactions': data['transactions'],
                'total_amount': data['amount'],
                'debit_amount': data['debit_amount'],
                'credit_amount': data['credit_amount']
            }
            for month, data in sorted(monthly_data.items())
        ]
        
        # 3. User Breakdown Chart
        user_data = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                user = transaction['user_name']
                if user not in user_data:
                    user_data[user] = {
                        'duplicate_groups': 0,
                        'transactions': 0,
                        'amount': 0.0
                    }
                user_data[user]['transactions'] += 1
                user_data[user]['amount'] += transaction['amount']
        
        # Count groups per user
        for group in duplicate_groups:
            users_in_group = set(t['user_name'] for t in group['transactions'])
            for user in users_in_group:
                user_data[user]['duplicate_groups'] += 1
        
        user_breakdown_chart = [
            {
                'user': user,
                'duplicate_groups': data['duplicate_groups'],
                'transactions': data['transactions'],
                'total_amount': data['amount']
            }
            for user, data in sorted(user_data.items(), key=lambda x: x[1]['amount'], reverse=True)
        ]
        
        # 4. FS Line Chart
        fs_line_data = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                account = transaction['gl_account']
                if account not in fs_line_data:
                    fs_line_data[account] = {
                        'duplicate_groups': 0,
                        'transactions': 0,
                        'amount': 0.0,
                        'debit_amount': 0.0,
                        'credit_amount': 0.0
                    }
                fs_line_data[account]['transactions'] += 1
                fs_line_data[account]['amount'] += transaction['amount']
                if transaction['transaction_type'] == 'DEBIT':
                    fs_line_data[account]['debit_amount'] += transaction['amount']
                else:
                    fs_line_data[account]['credit_amount'] += transaction['amount']
        
        # Count groups per account
        for group in duplicate_groups:
            account = group['transactions'][0]['gl_account']
            fs_line_data[account]['duplicate_groups'] += 1
        
        fs_line_chart = [
            {
                'gl_account': account,
                'duplicate_groups': data['duplicate_groups'],
                'transactions': data['transactions'],
                'total_amount': data['amount'],
                'debit_amount': data['debit_amount'],
                'credit_amount': data['credit_amount']
            }
            for account, data in sorted(fs_line_data.items(), key=lambda x: x[1]['amount'], reverse=True)
        ]
        
        # 5. Amount Distribution Chart
        amount_ranges = [
            (0, 1000, '0-1K'),
            (1000, 10000, '1K-10K'),
            (10000, 100000, '10K-100K'),
            (100000, 1000000, '100K-1M'),
            (1000000, float('inf'), '1M+')
        ]
        
        amount_distribution = {}
        for range_min, range_max, label in amount_ranges:
            amount_distribution[label] = {
                'duplicate_groups': 0,
                'transactions': 0,
                'amount': 0.0
            }
        
        for group in duplicate_groups:
            amount = group['amount']
            for range_min, range_max, label in amount_ranges:
                if range_min <= amount < range_max:
                    amount_distribution[label]['duplicate_groups'] += 1
                    amount_distribution[label]['transactions'] += group['count']
                    amount_distribution[label]['amount'] += group['total_amount']
                    break
        
        amount_distribution_chart = [
            {
                'range': label,
                'duplicate_groups': data['duplicate_groups'],
                'transactions': data['transactions'],
                'total_amount': data['amount']
            }
            for label, data in amount_distribution.items()
        ]
        
        # 6. Risk Level Chart
        risk_levels = {
            'LOW': {'min': 0, 'max': 39, 'groups': 0, 'transactions': 0, 'amount': 0.0},
            'MEDIUM': {'min': 40, 'max': 69, 'groups': 0, 'transactions': 0, 'amount': 0.0},
            'HIGH': {'min': 70, 'max': 89, 'groups': 0, 'transactions': 0, 'amount': 0.0},
            'CRITICAL': {'min': 90, 'max': 100, 'groups': 0, 'transactions': 0, 'amount': 0.0}
        }
        
        for group in duplicate_groups:
            risk_score = group['risk_score']
            for level, range_data in risk_levels.items():
                if range_data['min'] <= risk_score <= range_data['max']:
                    risk_levels[level]['groups'] += 1
                    risk_levels[level]['transactions'] += group['count']
                    risk_levels[level]['amount'] += group['total_amount']
                    break
        
        risk_level_chart = [
            {
                'risk_level': level,
                'duplicate_groups': data['groups'],
                'transactions': data['transactions'],
                'total_amount': data['amount']
            }
            for level, data in risk_levels.items()
        ]
        
        return {
            'duplicate_type_chart': duplicate_type_chart,
            'monthly_trend_chart': monthly_trend_chart,
            'user_breakdown_chart': user_breakdown_chart,
            'fs_line_chart': fs_line_chart,
            'amount_distribution_chart': amount_distribution_chart,
            'risk_level_chart': risk_level_chart
        }
    
    def _generate_breakdowns(self, duplicate_groups, df):
        """Generate detailed breakdowns"""
        
        # 1. Duplicate Flags Breakdown
        duplicate_flags = {}
        for group in duplicate_groups:
            dup_type = group['type']
            if dup_type not in duplicate_flags:
                duplicate_flags[dup_type] = {
                    'count': 0,
                    'transactions': 0,
                    'amount': 0.0,
                    'debit_count': 0,
                    'credit_count': 0,
                    'debit_amount': 0.0,
                    'credit_amount': 0.0
                }
            duplicate_flags[dup_type]['count'] += 1
            duplicate_flags[dup_type]['transactions'] += group['count']
            duplicate_flags[dup_type]['amount'] += group['total_amount']
            duplicate_flags[dup_type]['debit_count'] += group['debit_count']
            duplicate_flags[dup_type]['credit_count'] += group['credit_count']
            duplicate_flags[dup_type]['debit_amount'] += group['debit_amount']
            duplicate_flags[dup_type]['credit_amount'] += group['credit_amount']
        
        # 2. Debit/Credit Monthly Breakdown
        debit_credit_monthly = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                # Handle posting_date safely
                posting_date = transaction['posting_date']
                if posting_date:
                    if hasattr(posting_date, 'strftime'):
                        month_key = posting_date.strftime('%Y-%m')
                    elif isinstance(posting_date, str):
                        month_key = posting_date[:7] if len(posting_date) >= 7 else 'Unknown'
                    else:
                        month_key = str(posting_date)[:7] if len(str(posting_date)) >= 7 else 'Unknown'
                else:
                    month_key = 'Unknown'
                
                if month_key not in debit_credit_monthly:
                    debit_credit_monthly[month_key] = {
                        'debit_count': 0,
                        'credit_count': 0,
                        'debit_amount': 0.0,
                        'credit_amount': 0.0,
                        'journal_lines': 0
                    }
                debit_credit_monthly[month_key]['journal_lines'] += 1
                if transaction['transaction_type'] == 'DEBIT':
                    debit_credit_monthly[month_key]['debit_count'] += 1
                    debit_credit_monthly[month_key]['debit_amount'] += transaction['amount']
                else:
                    debit_credit_monthly[month_key]['credit_count'] += 1
                    debit_credit_monthly[month_key]['credit_amount'] += transaction['amount']
        
        # 3. User Breakdown
        user_breakdown = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                user = transaction['user_name']
                if user not in user_breakdown:
                    user_breakdown[user] = {
                        'duplicate_groups': 0,
                        'transactions': 0,
                        'amount': 0.0,
                        'unique_accounts': set(),
                        'unique_documents': set()
                    }
                user_breakdown[user]['transactions'] += 1
                user_breakdown[user]['amount'] += transaction['amount']
                user_breakdown[user]['unique_accounts'].add(transaction['gl_account'])
                user_breakdown[user]['unique_documents'].add(transaction['document_number'])
        
        # Convert sets to counts
        for user_data in user_breakdown.values():
            user_data['unique_accounts'] = len(user_data['unique_accounts'])
            user_data['unique_documents'] = len(user_data['unique_documents'])
        
        # 4. FS Line Breakdown
        fs_line_breakdown = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                account = transaction['gl_account']
                if account not in fs_line_breakdown:
                    fs_line_breakdown[account] = {
                        'duplicate_groups': 0,
                        'transactions': 0,
                        'amount': 0.0,
                        'debit_count': 0,
                        'credit_count': 0,
                        'debit_amount': 0.0,
                        'credit_amount': 0.0
                    }
                fs_line_breakdown[account]['transactions'] += 1
                fs_line_breakdown[account]['amount'] += transaction['amount']
                if transaction['transaction_type'] == 'DEBIT':
                    fs_line_breakdown[account]['debit_count'] += 1
                    fs_line_breakdown[account]['debit_amount'] += transaction['amount']
                else:
                    fs_line_breakdown[account]['credit_count'] += 1
                    fs_line_breakdown[account]['credit_amount'] += transaction['amount']
        
        # Count groups per account
        for group in duplicate_groups:
            account = group['transactions'][0]['gl_account']
            fs_line_breakdown[account]['duplicate_groups'] += 1
        
        return {
            'duplicate_flags': duplicate_flags,
            'debit_credit_monthly': debit_credit_monthly,
            'user_breakdown': user_breakdown,
            'fs_line_breakdown': fs_line_breakdown,
            'type_breakdown': duplicate_flags,  # Same as duplicate_flags
            'risk_breakdown': self._generate_risk_breakdown(duplicate_groups)
        }
    
    def _generate_risk_breakdown(self, duplicate_groups):
        """Generate risk level breakdown"""
        risk_breakdown = {
            'LOW': {'groups': 0, 'transactions': 0, 'amount': 0.0},
            'MEDIUM': {'groups': 0, 'transactions': 0, 'amount': 0.0},
            'HIGH': {'groups': 0, 'transactions': 0, 'amount': 0.0},
            'CRITICAL': {'groups': 0, 'transactions': 0, 'amount': 0.0}
        }
        
        for group in duplicate_groups:
            risk_score = group['risk_score']
            if risk_score < 40:
                level = 'LOW'
            elif risk_score < 70:
                level = 'MEDIUM'
            elif risk_score < 90:
                level = 'HIGH'
            else:
                level = 'CRITICAL'
            
            risk_breakdown[level]['groups'] += 1
            risk_breakdown[level]['transactions'] += group['count']
            risk_breakdown[level]['amount'] += group['total_amount']
        
        return risk_breakdown
    
    def _generate_slicer_filters(self, duplicate_groups, df):
        """Generate slicer filters for dynamic filtering"""
        
        # Collect unique values
        duplicate_types = list(set(group['type'] for group in duplicate_groups))
        users = list(set(t['user_name'] for group in duplicate_groups for t in group['transactions']))
        gl_accounts = list(set(t['gl_account'] for group in duplicate_groups for t in group['transactions']))
        
        # Date ranges
        all_dates = []
        for group in duplicate_groups:
            for t in group['transactions']:
                posting_date = t['posting_date']
                if posting_date:
                    # Convert to datetime if it's a string
                    if isinstance(posting_date, str):
                        try:
                            import pandas as pd
                            posting_date = pd.to_datetime(posting_date)
                        except:
                            continue
                    all_dates.append(posting_date)
        
        if all_dates:
            try:
                min_date = min(all_dates)
                max_date = max(all_dates)
            except:
                min_date = max_date = None
            date_ranges = [
                {'label': 'Last 7 days', 'value': '7d'},
                {'label': 'Last 30 days', 'value': '30d'},
                {'label': 'Last 90 days', 'value': '90d'},
                {'label': 'All', 'value': 'all'}
            ]
        else:
            date_ranges = []
        
        # Amount ranges
        all_amounts = [t['amount'] for group in duplicate_groups for t in group['transactions']]
        if all_amounts:
            min_amount = min(all_amounts)
            max_amount = max(all_amounts)
            amount_ranges = [
                {'label': '0-1K', 'value': '0-1000'},
                {'label': '1K-10K', 'value': '1000-10000'},
                {'label': '10K-100K', 'value': '10000-100000'},
                {'label': '100K-1M', 'value': '100000-1000000'},
                {'label': '1M+', 'value': '1000000+'}
            ]
        else:
            amount_ranges = []
        
        # Risk levels
        risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        
        return {
            'duplicate_types': duplicate_types,
            'users': users,
            'gl_accounts': gl_accounts,
            'date_ranges': date_ranges,
            'amount_ranges': amount_ranges,
            'risk_levels': risk_levels
        }
    
    def _generate_summary_table(self, duplicate_groups, df):
        """Generate summary table for final test selections"""
        summary_table = []
        
        for group in duplicate_groups:
            summary_entry = {
                'duplicate_type': group['type'],
                'criteria': group['criteria'],
                'gl_account': group['transactions'][0]['gl_account'],
                'amount': group['amount'],
                'duplicate_count': group['count'],
                'total_amount': group['total_amount'],
                'risk_score': group['risk_score'],
                'risk_level': self._get_risk_level(group['risk_score']),
                'debit_count': group['debit_count'],
                'credit_count': group['credit_count'],
                'debit_amount': group['debit_amount'],
                'credit_amount': group['credit_amount'],
                'unique_users': group['unique_users'],
                'unique_documents': group['unique_documents'],
                'date_range': group['date_range'],
                'transactions': [
                    {
                        'id': t['id'],
                        'document_number': t['document_number'],
                        'posting_date': str(t['posting_date']) if t['posting_date'] else None,
                        'user_name': t['user_name'],
                        'amount': t['amount'],
                        'transaction_type': t['transaction_type']
                    }
                    for t in group['transactions']
                ]
            }
            summary_table.append(summary_entry)
        
        return summary_table
    
    def _generate_export_data(self, duplicate_groups, df):
        """Generate export-ready data for CSV"""
        export_data = []
        
        for group in duplicate_groups:
            for transaction in group['transactions']:
                export_row = {
                    'Duplicate_Type': group['type'],
                    'Duplicate_Criteria': group['criteria'],
                    'GL_Account': transaction['gl_account'],
                    'Amount': transaction['amount'],
                    'Duplicate_Count': group['count'],
                    'Risk_Score': group['risk_score'],
                    'Risk_Level': self._get_risk_level(group['risk_score']),
                    'Transaction_ID': transaction['id'],
                    'Document_Number': transaction['document_number'],
                    'Posting_Date': str(transaction['posting_date']) if transaction['posting_date'] else None,
                    'Document_Date': str(transaction['document_date']) if transaction['document_date'] else None,
                    'User_Name': transaction['user_name'],
                    'Document_Type': transaction['document_type'],
                    'Transaction_Type': transaction['transaction_type'],
                    'Text': transaction['text'],
                    'Fiscal_Year': transaction['fiscal_year'],
                    'Posting_Period': transaction['posting_period'],
                    'Profit_Center': transaction['profit_center'],
                    'Cost_Center': transaction['cost_center'],
                    'Local_Currency': transaction['local_currency'],
                    'Group_Total_Amount': group['total_amount'],
                    'Group_Debit_Count': group['debit_count'],
                    'Group_Credit_Count': group['credit_count'],
                    'Group_Debit_Amount': group['debit_amount'],
                    'Group_Credit_Amount': group['credit_amount'],
                    'Unique_Users_In_Group': group['unique_users'],
                    'Unique_Documents_In_Group': group['unique_documents']
                }
                export_data.append(export_row)
        
        return export_data
    
    def _get_analysis_info(self, df):
        """Get basic analysis information"""
        total_transactions = len(df)
        duplicate_groups = self._detect_duplicate_groups(df)
        total_duplicate_groups = len(duplicate_groups)
        total_duplicate_transactions = sum(group['count'] for group in duplicate_groups)
        total_amount_involved = sum(group['total_amount'] for group in duplicate_groups)
        
        return {
            'total_transactions': total_transactions,
            'total_duplicate_groups': total_duplicate_groups,
            'total_duplicate_transactions': total_duplicate_transactions,
            'total_amount_involved': total_amount_involved,
            'analysis_date': datetime.now().isoformat(),
            'duplicate_threshold': self.duplicate_threshold
        }
    
    def _get_risk_level(self, risk_score):
        """Get risk level from score"""
        if risk_score < 40:
            return 'LOW'
        elif risk_score < 70:
            return 'MEDIUM'
        elif risk_score < 90:
            return 'HIGH'
        else:
            return 'CRITICAL'
    
    def _generate_detailed_insights(self, duplicate_groups, df):
        """Generate detailed insights and analysis"""
        if not duplicate_groups:
            return {}
        
        insights = {
            'duplicate_patterns': self._analyze_duplicate_patterns(duplicate_groups),
            'anomaly_indicators': self._identify_anomaly_indicators(duplicate_groups),
            'risk_assessment': self._assess_risk_factors(duplicate_groups),
            'audit_recommendations': self._generate_audit_recommendations(duplicate_groups),
            'trend_analysis': self._analyze_trends(duplicate_groups),
            'comparative_analysis': self._perform_comparative_analysis(duplicate_groups, df)
        }
        
        return insights
    
    def _analyze_duplicate_patterns(self, duplicate_groups):
        """Analyze patterns in duplicate transactions"""
        patterns = {
            'most_common_patterns': [],
            'unusual_patterns': [],
            'pattern_frequency': {}
        }
        
        # Analyze duplicate type patterns
        type_counts = {}
        for group in duplicate_groups:
            dup_type = group['type']
            type_counts[dup_type] = type_counts.get(dup_type, 0) + 1
        
        # Most common patterns
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        patterns['most_common_patterns'] = [
            {
                'pattern': dup_type,
                'frequency': count,
                'percentage': (count / len(duplicate_groups)) * 100
            }
            for dup_type, count in sorted_types[:3]
        ]
        
        # Unusual patterns (low frequency but high risk)
        unusual_patterns = []
        for group in duplicate_groups:
            if group['risk_score'] > 50 and type_counts.get(group['type'], 0) <= 1:
                unusual_patterns.append({
                    'pattern': group['type'],
                    'risk_score': group['risk_score'],
                    'amount': group['total_amount'],
                    'count': group['count']
                })
        
        patterns['unusual_patterns'] = sorted(unusual_patterns, key=lambda x: x['risk_score'], reverse=True)[:5]
        patterns['pattern_frequency'] = type_counts
        
        return patterns
    
    def _identify_anomaly_indicators(self, duplicate_groups):
        """Identify specific anomaly indicators"""
        indicators = {
            'high_value_duplicates': [],
            'frequent_duplicates': [],
            'time_based_anomalies': [],
            'user_based_anomalies': [],
            'account_based_anomalies': []
        }
        
        # High value duplicates
        high_value_threshold = 1000000  # 1M
        for group in duplicate_groups:
            if group['total_amount'] > high_value_threshold:
                indicators['high_value_duplicates'].append({
                    'type': group['type'],
                    'amount': group['total_amount'],
                    'count': group['count'],
                    'risk_score': group['risk_score']
                })
        
        # Frequent duplicates (same user/account combinations)
        user_account_counts = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                key = f"{transaction['user_name']}_{transaction['gl_account']}"
                user_account_counts[key] = user_account_counts.get(key, 0) + 1
        
        frequent_combinations = [(k, v) for k, v in user_account_counts.items() if v > 2]
        indicators['frequent_duplicates'] = [
            {
                'user_account': combo[0],
                'frequency': combo[1]
            }
            for combo in sorted(frequent_combinations, key=lambda x: x[1], reverse=True)[:5]
        ]
        
        # Time-based anomalies (same day duplicates)
        same_day_duplicates = []
        for group in duplicate_groups:
            if len(group['transactions']) > 1:
                dates = [t['posting_date'] for t in group['transactions'] if t['posting_date']]
                if len(set(dates)) == 1:  # All same date
                    same_day_duplicates.append({
                        'type': group['type'],
                        'date': dates[0],
                        'count': group['count'],
                        'amount': group['total_amount']
                    })
        
        indicators['time_based_anomalies'] = sorted(same_day_duplicates, key=lambda x: x['amount'], reverse=True)[:5]
        
        return indicators
    
    def _assess_risk_factors(self, duplicate_groups):
        """Assess risk factors for duplicate transactions"""
        risk_factors = {
            'high_risk_groups': [],
            'risk_distribution': {},
            'risk_trends': {},
            'mitigation_suggestions': []
        }
        
        # High risk groups
        high_risk_groups = [group for group in duplicate_groups if group['risk_score'] > 70]
        risk_factors['high_risk_groups'] = [
            {
                'type': group['type'],
                'risk_score': group['risk_score'],
                'amount': group['total_amount'],
                'count': group['count'],
                'users': list(set(t['user_name'] for t in group['transactions'])),
                'accounts': list(set(t['gl_account'] for t in group['transactions']))
            }
            for group in sorted(high_risk_groups, key=lambda x: x['risk_score'], reverse=True)
        ]
        
        # Risk distribution
        risk_levels = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        for group in duplicate_groups:
            risk_level = self._get_risk_level(group['risk_score'])
            risk_levels[risk_level] += 1
        
        risk_factors['risk_distribution'] = risk_levels
        
        # Mitigation suggestions
        suggestions = []
        if len(high_risk_groups) > 0:
            suggestions.append("Review high-risk duplicate groups for potential fraud or errors")
        if any(group['total_amount'] > 1000000 for group in duplicate_groups):
            suggestions.append("Investigate high-value duplicate transactions")
        if len(set(t['user_name'] for group in duplicate_groups for t in group['transactions'])) < 3:
            suggestions.append("Limited user diversity in duplicates - consider user training")
        
        risk_factors['mitigation_suggestions'] = suggestions
        
        return risk_factors
    
    def _generate_audit_recommendations(self, duplicate_groups):
        """Generate audit recommendations based on duplicate analysis"""
        recommendations = {
            'immediate_actions': [],
            'investigation_priorities': [],
            'control_improvements': [],
            'monitoring_suggestions': []
        }
        
        # Immediate actions
        critical_duplicates = [g for g in duplicate_groups if g['risk_score'] >= 90]
        if critical_duplicates:
            recommendations['immediate_actions'].append({
                'action': 'Immediate investigation required',
                'reason': f'{len(critical_duplicates)} critical risk duplicate groups found',
                'priority': 'HIGH'
            })
        
        # Investigation priorities
        high_value_duplicates = sorted(duplicate_groups, key=lambda x: x['total_amount'], reverse=True)[:3]
        recommendations['investigation_priorities'] = [
            {
                'priority': i + 1,
                'type': group['type'],
                'amount': group['total_amount'],
                'risk_score': group['risk_score'],
                'reason': 'High value duplicate transaction'
            }
            for i, group in enumerate(high_value_duplicates)
        ]
        
        # Control improvements
        if len(duplicate_groups) > 10:
            recommendations['control_improvements'].append({
                'improvement': 'Implement duplicate detection controls',
                'reason': 'High number of duplicate transactions detected'
            })
        
        # Monitoring suggestions
        recommendations['monitoring_suggestions'] = [
            'Monitor transactions from users with high duplicate frequency',
            'Set up alerts for duplicate transactions above threshold amounts',
            'Regular review of duplicate patterns and trends',
            'Implement automated duplicate prevention controls'
        ]
        
        return recommendations
    
    def _analyze_trends(self, duplicate_groups):
        """Analyze trends in duplicate transactions"""
        trends = {
            'temporal_trends': {},
            'amount_trends': {},
            'user_trends': {},
            'pattern_trends': {}
        }
        
        # Temporal trends
        monthly_data = {}
        for group in duplicate_groups:
            for transaction in group['transactions']:
                if transaction['posting_date']:
                    try:
                        if hasattr(transaction['posting_date'], 'strftime'):
                            month_key = transaction['posting_date'].strftime('%Y-%m')
                        elif isinstance(transaction['posting_date'], str):
                            month_key = transaction['posting_date'][:7]
                        else:
                            month_key = str(transaction['posting_date'])[:7]
                        
                        if month_key not in monthly_data:
                            monthly_data[month_key] = {'count': 0, 'amount': 0}
                        monthly_data[month_key]['count'] += 1
                        monthly_data[month_key]['amount'] += transaction['amount']
                    except:
                        pass
        
        trends['temporal_trends'] = monthly_data
        
        # Amount trends
        amounts = [group['total_amount'] for group in duplicate_groups]
        if amounts:
            trends['amount_trends'] = {
                'min_amount': min(amounts),
                'max_amount': max(amounts),
                'average_amount': sum(amounts) / len(amounts),
                'amount_distribution': {
                    'low': len([a for a in amounts if a < 10000]),
                    'medium': len([a for a in amounts if 10000 <= a < 100000]),
                    'high': len([a for a in amounts if 100000 <= a < 1000000]),
                    'very_high': len([a for a in amounts if a >= 1000000])
                }
            }
        
        return trends
    
    def _perform_comparative_analysis(self, duplicate_groups, df):
        """Perform comparative analysis against overall transaction data"""
        if df.empty:
            return {}
        
        total_transactions = len(df)
        total_amount = df['amount'].sum()
        duplicate_transactions = sum(group['count'] for group in duplicate_groups)
        duplicate_amount = sum(group['total_amount'] for group in duplicate_groups)
        
        comparative = {
            'duplicate_percentage': {
                'transaction_count': (duplicate_transactions / total_transactions) * 100 if total_transactions > 0 else 0,
                'amount': (duplicate_amount / total_amount) * 100 if total_amount > 0 else 0
            },
            'concentration_analysis': {
                'top_users_duplicate_percentage': {},
                'top_accounts_duplicate_percentage': {}
            },
            'benchmark_comparison': {
                'industry_average_duplicate_rate': 2.5,  # Example benchmark
                'current_duplicate_rate': (duplicate_transactions / total_transactions) * 100 if total_transactions > 0 else 0,
                'status': 'Above Average' if (duplicate_transactions / total_transactions) * 100 > 2.5 else 'Below Average'
            }
        }
        
        return comparative 