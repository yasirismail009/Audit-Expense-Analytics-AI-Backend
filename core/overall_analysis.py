"""
Overall Analysis Module for SAP GL Posting Analysis System

This module handles the final overall analysis that combines:
- All flagged transactions from duplicate and backdated analysis
- Flag summaries by type
- Expense data analysis
- Overall risk assessment and scoring
- Chart data generation for visualizations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Q, Count, Sum, Avg, Min, Max
from django.utils import timezone
import logging
from calendar import monthrange
import holidays
from django.db import models
from .models import (
    SAPGLPosting, OverallAnalysisResult, DuplicateAnalysisResult, 
    BackdatedAnalysisResult, GeneralAnalysisResult, RiskScoringDocument
)

logger = logging.getLogger(__name__)

class OverallAnalyzer:
    """Main analyzer for overall analysis combining all analysis types with risk calculations"""
    
    def __init__(self):
        self.analysis_config = {
            'currency': 'SAR',
            'decimal_places': 2,
            'risk_thresholds': {
                'low': 30,
                'medium': 60,
                'high': 80
            },
            'flag_types': ['duplicate', 'backdated', 'anomaly', 'high_value', 'unusual_pattern']
        }
    
    def run_overall_analysis(self, data_file, processing_job=None):
        """
        Run comprehensive overall analysis combining all analysis types
        
        Args:
            data_file: DataFile object
            processing_job: FileProcessingJob object (optional)
            
        Returns:
            dict: Complete overall analysis results
        """
        try:
            logger.info(f"Starting overall analysis for file: {data_file.file_name}")
            start_time = timezone.now()
            
            # Get all transactions for this file
            transactions = SAPGLPosting.objects.filter(
                document_number__in=data_file.get_transaction_document_numbers()
            )
            
            # 1. Transaction Summary
            transaction_summary = self._generate_transaction_summary(transactions)
            
            # 2. Get flagged transactions from other analyses
            flagged_transactions = self._get_flagged_transactions(data_file)
            
            # 3. Generate flag summary
            flag_summary = self._generate_flag_summary(flagged_transactions, transactions)
            
            # 4. Expense Analysis
            expense_analysis = self._analyze_expense_data(transactions)
            
            # 5. Risk Assessment
            risk_assessment = self._calculate_overall_risk_assessment(flagged_transactions, transactions)
            
            # 6. Chart Data
            chart_data = self._generate_chart_data(flagged_transactions, flag_summary, risk_assessment, transactions)
            
            # 7. Export Data
            export_data = self._generate_export_data(flagged_transactions, flag_summary, risk_assessment)
            
            # Calculate processing duration
            processing_duration = (timezone.now() - start_time).total_seconds()
            
            # Create analysis result
            analysis_result = {
                'transaction_summary': transaction_summary,
                'flagged_transactions': flagged_transactions,
                'flag_summary': flag_summary,
                'expense_analysis': expense_analysis,
                'risk_assessment': risk_assessment,
                'chart_data': chart_data,
                'export_data': export_data
            }
            
            # Save to database
            overall_analysis = OverallAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                processing_duration=processing_duration,
                **analysis_result
            )
            
            # Generate risk scoring document
            risk_document = self._generate_risk_scoring_document(
                data_file, flagged_transactions, risk_assessment, processing_job
            )
            
            logger.info(f"Overall analysis completed for file: {data_file.file_name} in {processing_duration:.2f} seconds")
            
            return {
                'analysis_id': str(overall_analysis.id),
                'risk_document_id': str(risk_document.id),
                'processing_duration': processing_duration,
                'results': analysis_result
            }
            
        except Exception as e:
            logger.error(f"Error in overall analysis: {e}")
            raise
    
    def _generate_transaction_summary(self, transactions):
        """Generate overall transaction summary statistics"""
        if not transactions:
            return {
                'total_transactions': 0,
                'total_amount': 0.0,
                'unique_accounts': 0,
                'unique_users': 0,
                'date_range': {},
                'currency': self.analysis_config['currency']
            }
        
        # Basic statistics
        total_transactions = transactions.count()
        total_amount = transactions.aggregate(total=Sum('amount_local_currency'))['total'] or Decimal('0.00')
        unique_accounts = transactions.values('gl_account').distinct().count()
        unique_users = transactions.values('user_name').distinct().count()
        
        # Date range
        date_range = transactions.aggregate(
            min_date=Min('posting_date'),
            max_date=Max('posting_date')
        )
        
        # Currency
        currency = transactions.first().local_currency if transactions.exists() else self.analysis_config['currency']
        
        # Amount statistics
        amounts = [float(t.amount_local_currency) for t in transactions]
        amount_stats = {
            'mean': float(np.mean(amounts)) if amounts else 0,
            'median': float(np.median(amounts)) if amounts else 0,
            'std': float(np.std(amounts)) if amounts else 0,
            'min': float(np.min(amounts)) if amounts else 0,
            'max': float(np.max(amounts)) if amounts else 0
        }
        
        return {
            'total_transactions': total_transactions,
            'total_amount': float(total_amount),
            'unique_accounts': unique_accounts,
            'unique_users': unique_users,
            'date_range': {
                'min_date': date_range['min_date'].isoformat() if date_range['min_date'] else None,
                'max_date': date_range['max_date'].isoformat() if date_range['max_date'] else None
            },
            'currency': currency,
            'amount_statistics': amount_stats
        }
    
    def _get_flagged_transactions(self, data_file):
        """Get all flagged transactions from duplicate and backdated analyses"""
        flagged_transactions = []
        
        # Get duplicate analysis results
        try:
            duplicate_results = DuplicateAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).latest('analysis_date')
            
            if duplicate_results.duplicate_list:
                # Deduplicate the duplicate list first
                seen_duplicate_ids = set()
                for duplicate in duplicate_results.duplicate_list:
                    dup_id = duplicate.get('id')
                    if dup_id not in seen_duplicate_ids:
                        flagged_transactions.append({
                            'transaction_id': dup_id,
                            'document_number': duplicate.get('document_number'),
                            'gl_account': duplicate.get('gl_account'),
                            'amount': duplicate.get('amount', 0),
                            'user_name': duplicate.get('user_name'),
                            'posting_date': duplicate.get('posting_date'),
                            'flag_type': 'duplicate',
                            'flag_details': duplicate.get('duplicate_type', 'Unknown'),
                            'risk_score': duplicate.get('risk_score', 0),
                            'source_analysis': 'duplicate_analysis'
                        })
                        seen_duplicate_ids.add(dup_id)
        except DuplicateAnalysisResult.DoesNotExist:
            logger.warning(f"No duplicate analysis results found for file: {data_file.file_name}")
        
        # Get backdated analysis results
        try:
            backdated_results = BackdatedAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).latest('analysis_date')
            
            if backdated_results.backdated_entries:
                # Deduplicate the backdated list first
                seen_backdated_ids = set()
                for backdated in backdated_results.backdated_entries:
                    back_id = backdated.get('id')
                    if back_id not in seen_backdated_ids:
                        flagged_transactions.append({
                            'transaction_id': back_id,
                            'document_number': backdated.get('document_number'),
                            'gl_account': backdated.get('gl_account'),
                            'amount': backdated.get('amount', 0),
                            'user_name': backdated.get('user_name'),
                            'posting_date': backdated.get('posting_date'),
                            'flag_type': 'backdated',
                            'flag_details': f"Days difference: {backdated.get('days_difference', 0)}",
                            'risk_score': backdated.get('risk_score', 0),
                            'source_analysis': 'backdated_analysis'
                        })
                        seen_backdated_ids.add(back_id)
        except BackdatedAnalysisResult.DoesNotExist:
            logger.warning(f"No backdated analysis results found for file: {data_file.file_name}")
        
        # Get user analysis results
        try:
            user_results = UserAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).latest('analysis_date')
            
            if user_results.user_anomalies:
                # Add user anomalies to flagged transactions
                seen_user_anomaly_ids = set()
                for user_anomaly in user_results.user_anomalies:
                    anomaly_id = user_anomaly.get('transaction_id')
                    if anomaly_id not in seen_user_anomaly_ids:
                        flagged_transactions.append({
                            'transaction_id': anomaly_id,
                            'document_number': user_anomaly.get('document_number'),
                            'gl_account': user_anomaly.get('gl_account'),
                            'amount': user_anomaly.get('amount', 0),
                            'user_name': user_anomaly.get('user_name'),
                            'posting_date': user_anomaly.get('posting_date'),
                            'flag_type': 'user_anomaly',
                            'flag_details': f"User anomaly: {user_anomaly.get('anomaly_type', 'Unknown')}",
                            'risk_score': user_anomaly.get('risk_score', 0),
                            'source_analysis': 'user_analysis'
                        })
                        seen_user_anomaly_ids.add(anomaly_id)
        except UserAnalysisResult.DoesNotExist:
            logger.warning(f"No user analysis results found for file: {data_file.file_name}")
        
        # Get unusual days analysis results
        try:
            unusual_days_results = UnusualDaysAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).latest('analysis_date')
            
            if unusual_days_results.weekend_postings:
                # Add weekend postings to flagged transactions
                seen_weekend_ids = set()
                for weekend_posting in unusual_days_results.weekend_postings:
                    posting_id = weekend_posting.get('transaction_id')
                    if posting_id not in seen_weekend_ids:
                        flagged_transactions.append({
                            'transaction_id': posting_id,
                            'document_number': weekend_posting.get('document_number'),
                            'gl_account': weekend_posting.get('gl_account'),
                            'amount': weekend_posting.get('amount', 0),
                            'user_name': weekend_posting.get('user_name'),
                            'posting_date': weekend_posting.get('posting_date'),
                            'flag_type': 'weekend_posting',
                            'flag_details': f"Weekend posting: {weekend_posting.get('day_name', 'Unknown')}",
                            'risk_score': weekend_posting.get('risk_score', 0),
                            'source_analysis': 'unusual_days_analysis'
                        })
                        seen_weekend_ids.add(posting_id)
        except UnusualDaysAnalysisResult.DoesNotExist:
            logger.warning(f"No unusual days analysis results found for file: {data_file.file_name}")
        
        # Get closing entries analysis results
        try:
            closing_entries_results = ClosingEntriesAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).latest('analysis_date')
            
            if closing_entries_results.closing_entries:
                # Add closing entries to flagged transactions
                seen_closing_ids = set()
                for closing_entry in closing_entries_results.closing_entries:
                    entry_id = closing_entry.get('transaction_id')
                    if entry_id not in seen_closing_ids:
                        flagged_transactions.append({
                            'transaction_id': entry_id,
                            'document_number': closing_entry.get('document_number'),
                            'gl_account': closing_entry.get('gl_account'),
                            'amount': closing_entry.get('amount', 0),
                            'user_name': closing_entry.get('user_name'),
                            'posting_date': closing_entry.get('posting_date'),
                            'flag_type': 'closing_entry',
                            'flag_details': f"Closing entry: {closing_entry.get('closing_window_type', 'Unknown')} - {closing_entry.get('days_from_month_end', 0)} days from month end",
                            'risk_score': closing_entry.get('risk_score', 0),
                            'source_analysis': 'closing_entries_analysis'
                        })
                        seen_closing_ids.add(entry_id)
        except ClosingEntriesAnalysisResult.DoesNotExist:
            logger.warning(f"No closing entries analysis results found for file: {data_file.file_name}")
        
        # Add fiscal year validation flags
        try:
            # Get all transactions for this file
            all_transactions = SAPGLPosting.objects.filter(data_file=data_file)
            
            for transaction in all_transactions:
                fiscal_year_issues = []
                risk_score = 0
                
                # Check if transaction fiscal year matches file fiscal year
                if transaction.fiscal_year != data_file.fiscal_year:
                    fiscal_year_issues.append(f"Fiscal year mismatch: {transaction.fiscal_year} vs {data_file.fiscal_year}")
                    risk_score += 15.0
                
                # Check if transaction date is within audit period
                if data_file.audit_start_date and data_file.audit_end_date and transaction.posting_date:
                    if transaction.posting_date < data_file.audit_start_date or transaction.posting_date > data_file.audit_end_date:
                        fiscal_year_issues.append(f"Transaction outside audit period: {transaction.posting_date}")
                        risk_score += 20.0
                
                # Add to flagged transactions if there are fiscal year issues
                if fiscal_year_issues:
                    flagged_transactions.append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'gl_account': transaction.gl_account,
                        'amount': float(transaction.amount_local_currency),
                        'user_name': transaction.user_name,
                        'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
                        'flag_type': 'fiscal_year_validation',
                        'flag_details': '; '.join(fiscal_year_issues),
                        'risk_score': risk_score,
                        'source_analysis': 'fiscal_year_validation'
                    })
        except Exception as e:
            logger.warning(f"Error in fiscal year validation: {e}")
        
        # Remove duplicates and create clean flag types (same transaction might be flagged in multiple analyses)
        unique_flagged = {}
        for transaction in flagged_transactions:
            key = transaction['transaction_id']
            if key not in unique_flagged:
                unique_flagged[key] = transaction
            else:
                # Merge flag types if same transaction is flagged multiple times
                existing = unique_flagged[key]
                
                # Create a set of unique flag types to avoid duplicates
                existing_flags = set(existing['flag_type'].split(', '))
                new_flags = set(transaction['flag_type'].split(', '))
                all_flags = existing_flags.union(new_flags)
                
                # Create a clean flag type label
                if len(all_flags) == 1:
                    flag_type_label = list(all_flags)[0]
                elif len(all_flags) == 2:
                    flag_type_label = ' & '.join(sorted(all_flags))
                else:
                    flag_type_label = 'Multiple Issues'
                
                existing['flag_type'] = flag_type_label
                existing['flag_details'] = f"{existing['flag_details']}; {transaction['flag_details']}"
                existing['risk_score'] = max(existing['risk_score'], transaction['risk_score'])
        
        # Clean up any remaining concatenated flag types and remove duplicates
        cleaned_transactions = []
        seen_flag_types = set()
        
        for transaction in unique_flagged.values():
            flag_type = transaction['flag_type']
            
            # Clean up concatenated flag types
            if ',' in flag_type:
                # Split by comma and clean up
                flags = [f.strip() for f in flag_type.split(',')]
                unique_flags = list(set(flags))  # Remove duplicates
                
                # Create clean label
                if len(unique_flags) == 1:
                    flag_type = unique_flags[0]
                elif len(unique_flags) == 2:
                    flag_type = ' & '.join(sorted(unique_flags))
                else:
                    flag_type = 'Multiple Issues'
                
                transaction['flag_type'] = flag_type
            
            # Only add if we haven't seen this flag type before (to prevent duplicates)
            if flag_type not in seen_flag_types:
                cleaned_transactions.append(transaction)
                seen_flag_types.add(flag_type)
        
        return cleaned_transactions
    
    def _generate_flag_summary(self, flagged_transactions, all_transactions=None):
        """Generate summary of flags by type with real transaction data"""
        if not flagged_transactions:
            return {
                'total_flagged': 0,
                'flag_types': {},
                'risk_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0},
                'amount_summary': {'avg_amount': 0, 'max_amount': 0, 'min_amount': 0, 'total_amount': 0},
                'anomaly_rate': 0
            }
        
        # Count by flag type (prevent duplicates)
        flag_types = {}
        risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        amounts = []
        
        # Use set to track unique transaction IDs to prevent duplicate counting
        unique_transaction_ids = set()
        
        for transaction in flagged_transactions:
            transaction_id = transaction['transaction_id']
            flag_type = transaction['flag_type']
            risk_score = transaction['risk_score']
            amount = transaction['amount']
            
            # Only count each transaction once per flag type
            if flag_type not in flag_types:
                flag_types[flag_type] = {
                    'count': 0,
                    'total_amount': 0,
                    'avg_risk_score': 0,
                    'risk_scores': [],
                    'unique_transactions': set()
                }
            
            # Only count if this transaction hasn't been counted for this flag type
            if transaction_id not in flag_types[flag_type]['unique_transactions']:
                flag_types[flag_type]['count'] += 1
                flag_types[flag_type]['total_amount'] += amount
                flag_types[flag_type]['risk_scores'].append(risk_score)
                flag_types[flag_type]['unique_transactions'].add(transaction_id)
                unique_transaction_ids.add(transaction_id)
            
            # Risk distribution (only count once per transaction)
            if transaction_id not in unique_transaction_ids or transaction_id in flag_types[flag_type]['unique_transactions']:
                if risk_score >= 80:
                    risk_distribution['critical'] += 1
                elif risk_score >= 60:
                    risk_distribution['high'] += 1
                elif risk_score >= 30:
                    risk_distribution['medium'] += 1
                else:
                    risk_distribution['low'] += 1
            
            amounts.append(amount)
        
        # Calculate averages for flag types and clean up data
        for flag_type, data in flag_types.items():
            data['avg_risk_score'] = sum(data['risk_scores']) / len(data['risk_scores']) if data['risk_scores'] else 0
            data['total_amount'] = float(data['total_amount'])
            data['avg_risk_score'] = float(data['avg_risk_score'])
            # Remove the set from the final output
            data.pop('unique_transactions', None)
        
        # Amount summary
        amount_summary = {
            'total_amount': sum(amounts) if amounts else 0,
            'avg_amount': sum(amounts) / len(amounts) if amounts else 0,
            'min_amount': min(amounts) if amounts else 0,
            'max_amount': max(amounts) if amounts else 0
        }
        
        # Calculate anomaly rate based on all transactions if available
        total_transactions = len(all_transactions) if all_transactions else len(flagged_transactions)
        anomaly_rate = (len(unique_transaction_ids) / total_transactions * 100) if total_transactions > 0 else 0
        
        return {
            'total_flagged': len(unique_transaction_ids),
            'flag_types': flag_types,
            'risk_distribution': risk_distribution,
            'amount_summary': amount_summary,
            'anomaly_rate': anomaly_rate
        }
    
    def _analyze_expense_data(self, transactions):
        """Analyze expense data and categorize transactions"""
        if not transactions:
            return {
                'expense_categories': {},
                'expense_summary': {},
                'top_expense_accounts': []
            }
        
        # Group by GL account for expense analysis
        account_expenses = {}
        
        for transaction in transactions:
            account_id = transaction.gl_account
            
            if account_id not in account_expenses:
                account_expenses[account_id] = {
                    'account_id': account_id,
                    'total_amount': 0,
                    'transaction_count': 0,
                    'debit_amount': 0,
                    'credit_amount': 0
                }
            
            amount = float(transaction.amount_local_currency)
            account_expenses[account_id]['total_amount'] += amount
            account_expenses[account_id]['transaction_count'] += 1
            
            if transaction.transaction_type == 'DEBIT':
                account_expenses[account_id]['debit_amount'] += amount
            else:
                account_expenses[account_id]['credit_amount'] += amount
        
        # Categorize accounts (simplified categorization)
        expense_categories = {
            'operating_expenses': [],
            'cost_of_goods_sold': [],
            'administrative_expenses': [],
            'financial_expenses': [],
            'other_expenses': []
        }
        
        for account_id, data in account_expenses.items():
            # Simple categorization based on account number patterns
            if account_id.startswith('5'):  # Cost of goods sold
                category = 'cost_of_goods_sold'
            elif account_id.startswith('6'):  # Operating expenses
                category = 'operating_expenses'
            elif account_id.startswith('7'):  # Administrative expenses
                category = 'administrative_expenses'
            elif account_id.startswith('8'):  # Financial expenses
                category = 'financial_expenses'
            else:
                category = 'other_expenses'
            
            expense_categories[category].append({
                'account_id': account_id,
                'total_amount': data['total_amount'],
                'transaction_count': data['transaction_count'],
                'debit_amount': data['debit_amount'],
                'credit_amount': data['credit_amount']
            })
        
        # Calculate expense summary
        total_expenses = sum(data['total_amount'] for data in account_expenses.values())
        expense_summary = {
            'total_expenses': total_expenses,
            'total_accounts': len(account_expenses),
            'avg_expense_per_account': total_expenses / len(account_expenses) if account_expenses else 0
        }
        
        # Top expense accounts
        top_expense_accounts = sorted(
            account_expenses.values(),
            key=lambda x: x['total_amount'],
            reverse=True
        )[:10]
        
        return {
            'expense_categories': expense_categories,
            'expense_summary': expense_summary,
            'top_expense_accounts': top_expense_accounts
        }
    
    def _calculate_overall_risk_assessment(self, flagged_transactions, all_transactions):
        """Calculate overall risk assessment and scoring"""
        if not flagged_transactions:
            return {
                'overall_risk_score': 0,
                'risk_level': 'LOW',
                'risk_factors': [],
                'risk_distribution': {},
                'recommendations': []
            }
        
        # Calculate overall risk score
        total_risk_score = sum(t['risk_score'] for t in flagged_transactions)
        overall_risk_score = total_risk_score / len(flagged_transactions) if flagged_transactions else 0
        
        # Get user analysis risk factors if available
        user_risk_factors = self._get_user_analysis_risk_factors(all_transactions)
        
        # Add user risk factors to overall score
        if user_risk_factors:
            user_risk_score = user_risk_factors.get('average_user_risk_score', 0)
            # Weight user risk at 20% of overall risk
            overall_risk_score = (overall_risk_score * 0.8) + (user_risk_score * 0.2)
        
        # Determine risk level
        if overall_risk_score >= 80:
            risk_level = 'CRITICAL'
        elif overall_risk_score >= 60:
            risk_level = 'HIGH'
        elif overall_risk_score >= 30:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        # Identify risk factors
        risk_factors = []
        
        # High-value transactions
        high_value_count = len([t for t in flagged_transactions if t['amount'] > 1000000])
        if high_value_count > 0:
            risk_factors.append({
                'factor': 'High-value flagged transactions',
                'count': high_value_count,
                'impact': 'HIGH'
            })
        
        # Multiple flag types
        flag_type_counts = {}
        for transaction in flagged_transactions:
            flag_type = transaction['flag_type']
            flag_type_counts[flag_type] = flag_type_counts.get(flag_type, 0) + 1
        
        for flag_type, count in flag_type_counts.items():
            if count > 10:  # Threshold for significant flag type
                risk_factors.append({
                    'factor': f'Multiple {flag_type} flags',
                    'count': count,
                    'impact': 'MEDIUM'
                })
        
        # Add user risk factors
        if user_risk_factors:
            high_risk_users = user_risk_factors.get('high_risk_users_count', 0)
            if high_risk_users > 0:
                risk_factors.append({
                    'factor': 'High-risk user activity',
                    'count': high_risk_users,
                    'impact': 'HIGH'
                })
            
            user_anomalies = user_risk_factors.get('user_anomalies_count', 0)
            if user_anomalies > 0:
                risk_factors.append({
                    'factor': 'User activity anomalies',
                    'count': user_anomalies,
                    'impact': 'MEDIUM'
                })
        
        # Risk distribution
        risk_distribution = {
            'low_risk': len([t for t in flagged_transactions if t['risk_score'] < 30]),
            'medium_risk': len([t for t in flagged_transactions if 30 <= t['risk_score'] < 60]),
            'high_risk': len([t for t in flagged_transactions if 60 <= t['risk_score'] < 80]),
            'critical_risk': len([t for t in flagged_transactions if t['risk_score'] >= 80])
        }
        
        # Generate recommendations
        recommendations = []
        
        if risk_level in ['HIGH', 'CRITICAL']:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Immediate review of all flagged transactions',
                'description': 'High-risk transactions require immediate attention'
            })
        
        if high_value_count > 0:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Review high-value flagged transactions',
                'description': f'{high_value_count} high-value transactions flagged'
            })
        
        if risk_distribution['critical_risk'] > 0:
            recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Investigate critical risk transactions',
                'description': f'{risk_distribution["critical_risk"]} critical risk transactions found'
            })
        
        # Add user-specific recommendations
        if user_risk_factors:
            high_risk_users = user_risk_factors.get('high_risk_users_count', 0)
            if high_risk_users > 0:
                recommendations.append({
                    'priority': 'HIGH',
                    'action': 'Review high-risk user activities',
                    'description': f'{high_risk_users} users identified with high-risk activity patterns'
                })
        
        return {
            'overall_risk_score': float(overall_risk_score),
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'risk_distribution': risk_distribution,
            'recommendations': recommendations,
            'user_risk_factors': user_risk_factors
        }
    
    def _get_user_analysis_risk_factors(self, all_transactions):
        """Get user analysis risk factors for risk calculation"""
        try:
            if not all_transactions:
                return {}
            
            # Get the data file from transactions
            data_file = all_transactions.first().data_file if all_transactions.exists() else None
            if not data_file:
                return {}
            
            # Check if user analysis exists
            from .models import UserAnalysisResult
            user_analysis = UserAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not user_analysis:
                return {}
            
            # Extract risk factors from user analysis
            user_risk_assessment = user_analysis.user_risk_assessment
            user_anomalies = user_analysis.user_anomalies
            
            risk_factors = {
                'high_risk_users_count': len(user_risk_assessment.get('high_risk_users', [])),
                'user_anomalies_count': len(user_anomalies),
                'average_user_risk_score': 0,
                'critical_risk_users': 0,
                'high_risk_users': 0
            }
            
            # Calculate average risk score
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            if user_risk_scores:
                risk_factors['average_user_risk_score'] = sum(
                    user['risk_score'] for user in user_risk_scores
                ) / len(user_risk_scores)
                
                # Count risk levels
                for user in user_risk_scores:
                    risk_level = user.get('risk_level', 'low')
                    if risk_level == 'critical':
                        risk_factors['critical_risk_users'] += 1
                    elif risk_level == 'high':
                        risk_factors['high_risk_users'] += 1
            
            return risk_factors
            
        except Exception as e:
            logger.warning(f"Error getting user analysis risk factors: {e}")
            return {}
    
    def _generate_chart_data(self, flagged_transactions, flag_summary, risk_assessment, all_transactions=None):
        """Generate chart data for visualizations"""
        if not flagged_transactions and not all_transactions:
            return {}
        
        # Flag Type Distribution Chart with improved labels
        # Calculate individual anomaly counts directly from transaction data
        flag_labels = []
        flag_data = []
        
        if all_transactions:
            # Calculate individual anomaly counts from transaction data
            duplicate_count = all_transactions.filter(is_duplicate=True).count()
            backdated_count = all_transactions.filter(is_backdated=True).count()
            high_value_count = len([t for t in all_transactions if t.is_high_value])
            
            if duplicate_count > 0:
                flag_labels.append('Duplicate Transactions')
                flag_data.append(duplicate_count)
            
            if backdated_count > 0:
                flag_labels.append('Backdated Entries')
                flag_data.append(backdated_count)
            
            if high_value_count > 0:
                flag_labels.append('High Value Transactions')
                flag_data.append(high_value_count)
            
        else:
            # Fallback to flag summary data if no transaction data available
            if 'duplicate_anomalies' in flag_summary and 'backdated_anomalies' in flag_summary:
                # Use enhanced counts for individual anomaly types
                duplicate_count = flag_summary.get('duplicate_anomalies', 0)
                backdated_count = flag_summary.get('backdated_anomalies', 0)
                high_value_count = flag_summary.get('high_value_anomalies', 0)
                
                if duplicate_count > 0:
                    flag_labels.append('Duplicate Transactions')
                    flag_data.append(duplicate_count)
                
                if backdated_count > 0:
                    flag_labels.append('Backdated Entries')
                    flag_data.append(backdated_count)
                
                if high_value_count > 0:
                    flag_labels.append('High Value Transactions')
                    flag_data.append(high_value_count)
                
            else:
                # Fallback to original flag types data
                flag_types = flag_summary.get('flag_types', {})
                
                for flag_type, data in flag_types.items():
                    # Clean up flag type labels
                    if flag_type == 'duplicate':
                        label = 'Duplicate Transactions'
                    elif flag_type == 'backdated':
                        label = 'Backdated Entries'
                    elif flag_type == 'duplicate & backdated':
                        label = 'Duplicate & Backdated'
                    elif flag_type == 'Multiple Issues':
                        label = 'Multiple Issues'
                    else:
                        label = flag_type.replace('_', ' ').title()
                    
                    flag_labels.append(label)
                    flag_data.append(data['count'])
        
        # If no flag types, create a default chart
        if not flag_labels:
            flag_labels = ['No Issues Detected']
            flag_data = [0]
        
        flag_type_chart = {
            'labels': flag_labels,
            'data': flag_data,
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']
        }
        
        # Chart data removed to prevent duplication - will be handled by API endpoint
        
        # Amount Distribution Chart with correct 1M threshold
        # Use all transactions for amount distribution, not just flagged ones
        if all_transactions:
            amounts = [float(t.amount_local_currency) for t in all_transactions]
        else:
            amounts = [t['amount'] for t in flagged_transactions]
        
        amount_ranges = [
            (0, 100000),
            (100000, 1000000),
            (1000000, 10000000),
            (10000000, float('inf'))
        ]
        
        amount_distribution = []
        for min_val, max_val in amount_ranges:
            if max_val == float('inf'):
                count = len([a for a in amounts if a >= min_val])
                label = f"{min_val/1000000:.0f}M+"
            else:
                count = len([a for a in amounts if min_val <= a < max_val])
                if min_val >= 1000000:
                    label = f"{min_val/1000000:.0f}M-{max_val/1000000:.0f}M"
                elif min_val >= 100000:
                    label = f"{min_val/100000:.0f}K-{max_val/1000000:.0f}M"
                else:
                    label = f"{min_val/1000:.0f}K-{max_val/1000:.0f}K"
            amount_distribution.append({'label': label, 'count': count})
        
        # Fix the labels to use correct format
        fixed_labels = []
        for item in amount_distribution:
            label = item['label']
            if label == '0K-100K':
                fixed_labels.append('0-100K')
            elif label == '1K-1M':
                fixed_labels.append('100K-1M')
            else:
                fixed_labels.append(label)
        
        # Update the distribution with fixed labels
        for i, item in enumerate(amount_distribution):
            item['label'] = fixed_labels[i]
        
        amount_distribution_chart = {
            'labels': [item['label'] for item in amount_distribution],
            'data': [item['count'] for item in amount_distribution],
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384']
        }
        
        return {
            'flag_type_chart': flag_type_chart,
            'amount_distribution_chart': amount_distribution_chart
        }
    
    def _generate_export_data(self, flagged_transactions, flag_summary, risk_assessment):
        """Generate export-ready data"""
        export_data = {
            'flagged_transactions': flagged_transactions,
            'flag_summary': flag_summary,
            'risk_assessment': risk_assessment,
            'export_timestamp': timezone.now().isoformat(),
            'total_flagged': len(flagged_transactions)
        }
        
        return export_data
    
    def _generate_risk_scoring_document(self, data_file, flagged_transactions, risk_assessment, processing_job):
        """Generate comprehensive risk scoring document"""
        try:
            # Calculate risk statistics
            total_transactions = SAPGLPosting.objects.filter(
                document_number__in=data_file.get_transaction_document_numbers()
            ).count()
            
            risk_dist = risk_assessment['risk_distribution']
            
            # Create risk scoring document
            risk_document = RiskScoringDocument.objects.create(
                data_file=data_file,
                processing_job=processing_job,
                total_transactions=total_transactions,
                high_risk_transactions=risk_dist['high_risk'],
                medium_risk_transactions=risk_dist['medium_risk'],
                low_risk_transactions=risk_dist['low_risk'],
                overall_risk_score=risk_assessment['overall_risk_score'],
                methodology_overview={
                    'description': 'Comprehensive risk scoring methodology combining duplicate and backdated analysis',
                    'version': '1.0.0',
                    'last_updated': timezone.now().isoformat()
                },
                risk_factors={
                    'duplicate_risk': {
                        'weight': 0.4,
                        'description': 'Risk associated with duplicate transactions'
                    },
                    'backdated_risk': {
                        'weight': 0.4,
                        'description': 'Risk associated with backdated entries'
                    },
                    'amount_risk': {
                        'weight': 0.2,
                        'description': 'Risk associated with high-value transactions'
                    }
                },
                scoring_criteria={
                    'low_risk': {'min': 0, 'max': 29, 'description': 'Normal transactions'},
                    'medium_risk': {'min': 30, 'max': 59, 'description': 'Some concerns'},
                    'high_risk': {'min': 60, 'max': 79, 'description': 'Significant risk'},
                    'critical_risk': {'min': 80, 'max': 100, 'description': 'High risk'}
                },
                risk_calculations={
                    'total_flagged': len(flagged_transactions),
                    'overall_risk_score': risk_assessment['overall_risk_score'],
                    'risk_level': risk_assessment['risk_level']
                },
                risk_distributions=risk_assessment['risk_distribution'],
                recommendations=risk_assessment['recommendations'],
                audit_implications={
                    'immediate_actions': [
                        'Review all critical and high-risk transactions',
                        'Investigate duplicate transactions',
                        'Verify backdated entries'
                    ],
                    'follow_up_actions': [
                        'Implement controls to prevent future issues',
                        'Regular monitoring of flagged patterns',
                        'Staff training on proper posting procedures'
                    ]
                }
            )
            
            return risk_document
            
        except Exception as e:
            logger.error(f"Error generating risk scoring document: {e}")
            raise 