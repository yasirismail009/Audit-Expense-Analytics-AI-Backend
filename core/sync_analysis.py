#!/usr/bin/env python3
"""
Synchronous Analysis Functions
Run full analysis pipeline without Celery and save to database tables
"""

import os
import django
from django.utils import timezone
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def run_general_analysis_sync(job_id):
    """Run General Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, GeneralAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running General Analysis for {len(transactions)} transactions")
        
        # Run basic analysis without ML orchestrator
        total_amount = float(sum(t.amount_local_currency for t in transactions))
        total_debits = float(sum(t.amount_local_currency for t in transactions if hasattr(t, 'transaction_type') and t.transaction_type == 'DEBIT'))
        total_credits = float(sum(t.amount_local_currency for t in transactions if hasattr(t, 'transaction_type') and t.transaction_type == 'CREDIT'))
        trial_balance = total_debits - total_credits
        
        # Generate GL account summaries
        gl_accounts = {}
        for t in transactions:
            account = t.gl_account
            if account not in gl_accounts:
                gl_accounts[account] = {
                    'account': account,
                    'total_amount': 0.0,
                    'transaction_count': 0,
                    'users': set()
                }
            gl_accounts[account]['total_amount'] += float(t.amount_local_currency)
            gl_accounts[account]['transaction_count'] += 1
            gl_accounts[account]['users'].add(t.user_name)
        
        # Convert sets to lists for JSON serialization
        for account_data in gl_accounts.values():
            account_data['users'] = list(account_data['users'])
        
        # Generate user summaries
        users = {}
        for t in transactions:
            user = t.user_name
            if user not in users:
                users[user] = {
                    'user': user,
                    'total_amount': 0.0,
                    'transaction_count': 0,
                    'accounts': set()
                }
            users[user]['total_amount'] += float(t.amount_local_currency)
            users[user]['transaction_count'] += 1
            users[user]['accounts'].add(t.gl_account)
        
        # Convert sets to lists for JSON serialization
        for user_data in users.values():
            user_data['accounts'] = list(user_data['accounts'])
        
        analysis_results = {
            'trial_balance_summary': {
                'total_debits': total_debits,
                'total_credits': total_credits,
                'balance': trial_balance,
                'is_balanced': abs(trial_balance) < 0.01,
                'balance_percentage': (abs(trial_balance) / float(total_amount) * 100) if total_amount > 0 else 0.0,
                'total_amount': total_amount
            },
            'gl_account_summaries': list(gl_accounts.values()),
            'user_summaries': list(users.values()),
            'statistical_calculations': {
                'average_transaction_amount': float(total_amount) / len(transactions) if transactions else 0.0,
                'total_transactions': len(transactions),
                'unique_users': len(set(t.user_name for t in transactions)),
                'unique_accounts': len(set(t.gl_account for t in transactions)),
                'min_transaction_amount': float(min(t.amount_local_currency for t in transactions)) if transactions else 0.0,
                'max_transaction_amount': float(max(t.amount_local_currency for t in transactions)) if transactions else 0.0
            },
            'chart_data': {
                'gl_account_distribution': list(gl_accounts.values()),
                'user_distribution': list(users.values())
            },
            'export_data': []
        }
        
        general_results = analysis_results
        general_results['processing_duration'] = (timezone.now() - start_time).total_seconds()
        
        # Create comprehensive statistics for general analysis
        general_stats = create_general_analysis_statistics(
            general_results, transactions, data_file
        )
        
        # Save to GeneralAnalysisResult table
        general_analysis_result = GeneralAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='general_analysis',
            analysis_version='1.0.0',
            trial_balance_summary=general_results['trial_balance_summary'],
            gl_account_summaries=general_results['gl_account_summaries'],
            user_summaries=general_results['user_summaries'],
            statistical_calculations=general_results['statistical_calculations'],
            chart_data=general_results['chart_data'],
            export_data=general_results['export_data'],
            processing_duration=general_results['processing_duration'],
            analysis_summary={
                'total_transactions': len(transactions),
                'total_users': len(set(t.user_name for t in transactions)),
                'total_accounts': len(set(t.gl_account for t in transactions)),
                'statistics': general_stats  # Add comprehensive statistics
            },
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"General Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(general_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'GeneralAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in General Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database
        try:
            GeneralAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='general_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def create_duplicate_analysis_statistics(duplicate_results, transactions, data_file, duplicate_pairs):
    """Create comprehensive statistics for duplicate analysis"""
    from decimal import Decimal
    
    stats = {
        'analysis_type': 'duplicate_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'financial_metrics': {},
        'compliance_metrics': {},
        'ml_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    duplicates_found = len(duplicate_pairs)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'duplicates_found': duplicates_found,
        'duplicate_percentage': (duplicates_found / total_transactions * 100) if total_transactions > 0 else 0,
        'unique_duplicate_transactions': len(set(
            dup['transaction1']['id'] for dup in duplicate_pairs
        ).union(set(
            dup['transaction2']['id'] for dup in duplicate_pairs
        )))
    }
    
    # Financial metrics
    total_duplicate_amount = sum(
        abs(dup['transaction1']['amount']) for dup in duplicate_pairs
    )
    
    stats['financial_metrics'] = {
        'total_amount_involved': total_duplicate_amount,
        'average_duplicate_amount': total_duplicate_amount / duplicates_found if duplicates_found > 0 else 0,
        'material_impact': total_duplicate_amount > 1000000,  # Over 1M SAR
        'impact_percentage': (total_duplicate_amount / abs(float(sum(t.amount_local_currency for t in transactions))) * 100) if transactions else 0,
        'high_value_duplicates': len([dup for dup in duplicate_pairs if abs(dup['transaction1']['amount']) > 100000])
    }
    
    # Risk metrics
    high_risk_duplicates = len([dup for dup in duplicate_pairs if dup.get('risk_level') == 'HIGH'])
    critical_risk_duplicates = len([dup for dup in duplicate_pairs if dup.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_duplicates,
        'critical_risk_count': critical_risk_duplicates,
        'overall_risk_score': min(100, (high_risk_duplicates * 10 + critical_risk_duplicates * 20)),
        'risk_distribution': {
            'low': len([dup for dup in duplicate_pairs if dup.get('risk_level') == 'LOW']),
            'medium': len([dup for dup in duplicate_pairs if dup.get('risk_level') == 'HIGH']),
            'high': high_risk_duplicates,
            'critical': critical_risk_duplicates
        }
    }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (duplicates_found * 5)),
        'regulatory_concerns': ['Duplicate transaction controls', 'Data integrity', 'Transaction monitoring'] if duplicates_found > 0 else [],
        'control_deficiencies': duplicates_found > 10,
        'audit_implications': 'High' if duplicates_found > 20 else 'Medium' if duplicates_found > 5 else 'Low'
    }
    
    # ML metrics (if available)
    if duplicate_results.get('ml_insights'):
        ml_insights = duplicate_results['ml_insights']
        stats['ml_metrics'] = {
            'ml_available': ml_insights.get('ml_available', False),
            'detection_method': ml_insights.get('detection_method', 'rule_based'),
            'ml_accuracy': ml_insights.get('ml_model_accuracy', 0.0),
            'ml_enhanced_duplicates': ml_insights.get('ml_enhanced_duplicates', 0),
            'rule_based_duplicates': ml_insights.get('rule_based_duplicates', 0)
        }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if duplicates_found < 10 else 'Medium' if duplicates_found < 50 else 'Low',
        'data_quality_score': max(0, 100 - (duplicates_found * 2)),
        'recommendation_priority': 'High' if duplicates_found > 20 else 'Medium' if duplicates_found > 5 else 'Low'
    }
    
    return stats

def create_backdated_analysis_statistics(backdated_results, transactions, data_file, backdated_entries):
    """Create comprehensive statistics for backdated analysis"""
    
    stats = {
        'analysis_type': 'backdated_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'financial_metrics': {},
        'compliance_metrics': {},
        'temporal_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    backdated_found = len(backdated_entries)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'backdated_found': backdated_found,
        'backdated_percentage': (backdated_found / total_transactions * 100) if total_transactions > 0 else 0,
        'unique_backdated_transactions': len(set(
            entry.get('transaction_id') for entry in backdated_entries if entry.get('transaction_id')
        ))
    }
    
    # Financial metrics
    total_backdated_amount = sum(
        abs(entry.get('amount', 0)) for entry in backdated_entries
    )
    
    stats['financial_metrics'] = {
        'total_amount_involved': total_backdated_amount,
        'average_backdated_amount': total_backdated_amount / backdated_found if backdated_found > 0 else 0,
        'material_impact': total_backdated_amount > 1000000,  # Over 1M SAR
        'impact_percentage': (total_backdated_amount / abs(float(sum(t.amount_local_currency for t in transactions))) * 100) if transactions else 0,
        'high_value_backdated': len([entry for entry in backdated_entries if abs(entry.get('amount', 0)) > 100000])
    }
    
    # Risk metrics
    high_risk_backdated = len([entry for entry in backdated_entries if entry.get('risk_level') == 'HIGH'])
    critical_risk_backdated = len([entry for entry in backdated_entries if entry.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_backdated,
        'critical_risk_count': critical_risk_backdated,
        'overall_risk_score': min(100, (high_risk_backdated * 15 + critical_risk_backdated * 25)),
        'risk_distribution': {
            'low': len([entry for entry in backdated_entries if entry.get('risk_level') == 'LOW']),
            'medium': len([entry for entry in backdated_entries if entry.get('risk_level') == 'MEDIUM']),
            'high': high_risk_backdated,
            'critical': critical_risk_backdated
        }
    }
    
    # Temporal metrics
    if backdated_entries:
        backdated_days = [entry.get('backdated_days', 0) for entry in backdated_entries]
        stats['temporal_metrics'] = {
            'average_backdated_days': sum(backdated_days) / len(backdated_days) if backdated_days else 0,
            'max_backdated_days': max(backdated_days) if backdated_days else 0,
            'min_backdated_days': min(backdated_days) if backdated_days else 0,
            'critical_backdated_entries': len([days for days in backdated_days if days > 30])
        }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (backdated_found * 8)),
        'regulatory_concerns': ['Timely posting controls', 'Document date validation', 'Audit trail integrity'] if backdated_found > 0 else [],
        'control_deficiencies': backdated_found > 5,
        'audit_implications': 'High' if backdated_found > 15 else 'Medium' if backdated_found > 3 else 'Low'
    }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if backdated_found < 5 else 'Medium' if backdated_found < 20 else 'Low',
        'data_quality_score': max(0, 100 - (backdated_found * 3)),
        'recommendation_priority': 'High' if backdated_found > 15 else 'Medium' if backdated_found > 3 else 'Low'
    }
    
    return stats

def create_user_analysis_statistics(user_results, transactions, data_file, user_anomalies):
    """Create comprehensive statistics for user analysis"""
    
    stats = {
        'analysis_type': 'user_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'user_metrics': {},
        'compliance_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    total_users = user_results.get('total_users', 0)
    anomalies_count = len(user_anomalies)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'total_users': total_users,
        'anomalies_count': anomalies_count,
        'anomaly_percentage': (anomalies_count / total_users * 100) if total_users > 0 else 0,
        'average_transactions_per_user': total_transactions / total_users if total_users > 0 else 0
    }
    
    # User metrics
    user_summary = user_results.get('user_transaction_summary', [])
    high_activity_users = len([u for u in user_summary if u.get('total_amount', 0) > 1000000 or u.get('transaction_count', 0) > 100])
    
    stats['user_metrics'] = {
        'high_activity_users': high_activity_users,
        'high_activity_percentage': (high_activity_users / total_users * 100) if total_users > 0 else 0,
        'users_with_anomalies': anomalies_count,
        'average_user_amount': sum(u.get('total_amount', 0) for u in user_summary) / total_users if total_users > 0 else 0,
        'max_user_amount': max((u.get('total_amount', 0) for u in user_summary), default=0),
        'min_user_amount': min((u.get('total_amount', 0) for u in user_summary), default=0)
    }
    
    # Risk metrics
    high_risk_anomalies = len([a for a in user_anomalies if a.get('risk_level') == 'HIGH'])
    critical_risk_anomalies = len([a for a in user_anomalies if a.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_anomalies,
        'critical_risk_count': critical_risk_anomalies,
        'overall_risk_score': min(100, (high_risk_anomalies * 15 + critical_risk_anomalies * 25)),
        'risk_distribution': {
            'low': len([a for a in user_anomalies if a.get('risk_level') == 'LOW']),
            'medium': len([a for a in user_anomalies if a.get('risk_level') == 'MEDIUM']),
            'high': high_risk_anomalies,
            'critical': critical_risk_anomalies
        }
    }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (anomalies_count * 5)),
        'regulatory_concerns': ['User access controls', 'Segregation of duties', 'Transaction monitoring'] if anomalies_count > 0 else [],
        'control_deficiencies': anomalies_count > 5,
        'audit_implications': 'High' if anomalies_count > 10 else 'Medium' if anomalies_count > 3 else 'Low'
    }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if anomalies_count < 5 else 'Medium' if anomalies_count < 15 else 'Low',
        'data_quality_score': max(0, 100 - (anomalies_count * 3)),
        'recommendation_priority': 'High' if anomalies_count > 10 else 'Medium' if anomalies_count > 3 else 'Low'
    }
    
    return stats

def create_unusual_days_analysis_statistics(unusual_days_results, transactions, data_file, unusual_days_transactions):
    """Create comprehensive statistics for unusual days analysis"""
    
    stats = {
        'analysis_type': 'unusual_days_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'temporal_metrics': {},
        'compliance_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    unusual_days_count = len(unusual_days_transactions)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'unusual_days_count': unusual_days_count,
        'unusual_days_percentage': (unusual_days_count / total_transactions * 100) if total_transactions > 0 else 0,
        'weekend_transactions_count': len([t for t in unusual_days_transactions if t.get('day_of_week') in ['Friday', 'Saturday']])
    }
    
    # Temporal metrics
    if unusual_days_transactions:
        weekend_count = len([t for t in unusual_days_transactions if t.get('day_of_week') in ['Friday', 'Saturday']])
        stats['temporal_metrics'] = {
            'weekend_transactions': weekend_count,
            'weekend_percentage': (weekend_count / unusual_days_count * 100) if unusual_days_count > 0 else 0,
            'other_unusual_days': unusual_days_count - weekend_count,
            'most_unusual_day': max(set(t.get('day_of_week') for t in unusual_days_transactions), key=lambda x: len([t for t in unusual_days_transactions if t.get('day_of_week') == x])) if unusual_days_transactions else None
        }
    
    # Risk metrics
    high_risk_unusual = len([t for t in unusual_days_transactions if t.get('risk_level') == 'HIGH'])
    critical_risk_unusual = len([t for t in unusual_days_transactions if t.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_unusual,
        'critical_risk_count': critical_risk_unusual,
        'overall_risk_score': min(100, (high_risk_unusual * 15 + critical_risk_unusual * 25)),
        'risk_distribution': {
            'low': len([t for t in unusual_days_transactions if t.get('risk_level') == 'LOW']),
            'medium': len([t for t in unusual_days_transactions if t.get('risk_level') == 'MEDIUM']),
            'high': high_risk_unusual,
            'critical': critical_risk_unusual
        }
    }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (unusual_days_count * 8)),
        'regulatory_concerns': ['Weekend posting controls', 'Business hour validation', 'Transaction timing controls'] if unusual_days_count > 0 else [],
        'control_deficiencies': unusual_days_count > 5,
        'audit_implications': 'High' if unusual_days_count > 15 else 'Medium' if unusual_days_count > 3 else 'Low'
    }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if unusual_days_count < 5 else 'Medium' if unusual_days_count < 20 else 'Low',
        'data_quality_score': max(0, 100 - (unusual_days_count * 3)),
        'recommendation_priority': 'High' if unusual_days_count > 15 else 'Medium' if unusual_days_count > 3 else 'Low'
    }
    
    return stats

def create_closing_entries_analysis_statistics(closing_entries_results, transactions, data_file, closing_entries_transactions):
    """Create comprehensive statistics for closing entries analysis"""
    
    stats = {
        'analysis_type': 'closing_entries_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'temporal_metrics': {},
        'compliance_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    closing_entries_count = len(closing_entries_transactions)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'closing_entries_count': closing_entries_count,
        'closing_entries_percentage': (closing_entries_count / total_transactions * 100) if total_transactions > 0 else 0,
        'post_close_entries_count': len([t for t in closing_entries_transactions if t.get('days_from_month_end', 0) <= 1])
    }
    
    # Temporal metrics
    if closing_entries_transactions:
        days_from_month_end = [t.get('days_from_month_end', 0) for t in closing_entries_transactions]
        stats['temporal_metrics'] = {
            'average_days_from_month_end': sum(days_from_month_end) / len(days_from_month_end) if days_from_month_end else 0,
            'max_days_from_month_end': max(days_from_month_end) if days_from_month_end else 0,
            'min_days_from_month_end': min(days_from_month_end) if days_from_month_end else 0,
            'critical_closing_entries': len([days for days in days_from_month_end if days <= 1])
        }
    
    # Risk metrics
    high_risk_closing = len([t for t in closing_entries_transactions if t.get('risk_level') == 'HIGH'])
    critical_risk_closing = len([t for t in closing_entries_transactions if t.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_closing,
        'critical_risk_count': critical_risk_closing,
        'overall_risk_score': min(100, (high_risk_closing * 15 + critical_risk_closing * 25)),
        'risk_distribution': {
            'low': len([t for t in closing_entries_transactions if t.get('risk_level') == 'LOW']),
            'medium': len([t for t in closing_entries_transactions if t.get('risk_level') == 'MEDIUM']),
            'high': high_risk_closing,
            'critical': critical_risk_closing
        }
    }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (closing_entries_count * 6)),
        'regulatory_concerns': ['Month-end closing controls', 'Financial statement integrity', 'Closing entry documentation'] if closing_entries_count > 0 else [],
        'control_deficiencies': closing_entries_count > 3,
        'audit_implications': 'High' if closing_entries_count > 10 else 'Medium' if closing_entries_count > 3 else 'Low'
    }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if closing_entries_count < 3 else 'Medium' if closing_entries_count < 10 else 'Low',
        'data_quality_score': max(0, 100 - (closing_entries_count * 4)),
        'recommendation_priority': 'High' if closing_entries_count > 10 else 'Medium' if closing_entries_count > 3 else 'Low'
    }
    
    return stats

def create_holiday_analysis_statistics(holiday_results, transactions, data_file, holiday_transactions):
    """Create comprehensive statistics for holiday analysis"""
    
    stats = {
        'analysis_type': 'holiday_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'risk_metrics': {},
        'temporal_metrics': {},
        'compliance_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    holiday_count = len(holiday_transactions)
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'holiday_transactions_count': holiday_count,
        'holiday_percentage': (holiday_count / total_transactions * 100) if total_transactions > 0 else 0,
        'unique_holidays': len(set(t.get('holiday_name') for t in holiday_transactions if t.get('holiday_name')))
    }
    
    # Financial metrics
    total_holiday_amount = sum(abs(t.get('amount', 0)) for t in holiday_transactions)
    
    stats['financial_metrics'] = {
        'total_amount_involved': total_holiday_amount,
        'average_holiday_amount': total_holiday_amount / holiday_count if holiday_count > 0 else 0,
        'material_impact': total_holiday_amount > 1000000,  # Over 1M SAR
        'impact_percentage': (total_holiday_amount / abs(float(sum(t.amount_local_currency for t in transactions))) * 100) if transactions else 0,
        'high_value_holiday_transactions': len([t for t in holiday_transactions if abs(t.get('amount', 0)) > 100000])
    }
    
    # Risk metrics
    high_risk_holiday = len([t for t in holiday_transactions if t.get('risk_level') == 'HIGH'])
    critical_risk_holiday = len([t for t in holiday_transactions if t.get('risk_level') == 'CRITICAL'])
    
    stats['risk_metrics'] = {
        'high_risk_count': high_risk_holiday,
        'critical_risk_count': critical_risk_holiday,
        'overall_risk_score': min(100, (high_risk_holiday * 15 + critical_risk_holiday * 25)),
        'risk_distribution': {
            'low': len([t for t in holiday_transactions if t.get('risk_level') == 'LOW']),
            'medium': len([t for t in holiday_transactions if t.get('risk_level') == 'MEDIUM']),
            'high': high_risk_holiday,
            'critical': critical_risk_holiday
        }
    }
    
    # Compliance metrics
    stats['compliance_metrics'] = {
        'compliance_score': max(0, 100 - (holiday_count * 8)),
        'regulatory_concerns': ['Holiday posting controls', 'Business day validation', 'Transaction timing controls'] if holiday_count > 0 else [],
        'control_deficiencies': holiday_count > 5,
        'audit_implications': 'High' if holiday_count > 15 else 'Medium' if holiday_count > 3 else 'Low'
    }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'processing_efficiency': 'High' if holiday_count < 5 else 'Medium' if holiday_count < 20 else 'Low',
        'data_quality_score': max(0, 100 - (holiday_count * 3)),
        'recommendation_priority': 'High' if holiday_count > 15 else 'Medium' if holiday_count > 3 else 'Low'
    }
    
    return stats

def create_general_analysis_statistics(general_results, transactions, data_file):
    """Create comprehensive statistics for general analysis"""
    
    stats = {
        'analysis_type': 'general_analysis',
        'timestamp': timezone.now().isoformat(),
        'summary_metrics': {},
        'financial_metrics': {},
        'account_metrics': {},
        'user_metrics': {},
        'performance_metrics': {}
    }
    
    # Basic summary metrics
    total_transactions = len(transactions)
    unique_users = len(set(t.user_name for t in transactions))
    unique_accounts = len(set(t.gl_account for t in transactions))
    
    stats['summary_metrics'] = {
        'total_transactions': total_transactions,
        'unique_users': unique_users,
        'unique_accounts': unique_accounts,
        'average_transactions_per_user': total_transactions / unique_users if unique_users > 0 else 0,
        'average_transactions_per_account': total_transactions / unique_accounts if unique_accounts > 0 else 0
    }
    
    # Financial metrics
    trial_balance = general_results.get('trial_balance_summary', {})
    total_amount = trial_balance.get('total_amount', 0)
    
    stats['financial_metrics'] = {
        'total_amount': total_amount,
        'total_debits': trial_balance.get('total_debits', 0),
        'total_credits': trial_balance.get('total_credits', 0),
        'balance': trial_balance.get('balance', 0),
        'is_balanced': trial_balance.get('is_balanced', False),
        'balance_percentage': trial_balance.get('balance_percentage', 0),
        'average_transaction_amount': total_amount / total_transactions if total_transactions > 0 else 0
    }
    
    # Account metrics
    gl_account_summaries = general_results.get('gl_account_summaries', [])
    if gl_account_summaries:
        stats['account_metrics'] = {
            'total_gl_accounts': len(gl_account_summaries),
            'high_volume_accounts': len([a for a in gl_account_summaries if a.get('transaction_count', 0) > 50]),
            'high_value_accounts': len([a for a in gl_account_summaries if abs(a.get('total_amount', 0)) > 1000000]),
            'average_account_balance': sum(abs(a.get('total_amount', 0)) for a in gl_account_summaries) / len(gl_account_summaries) if gl_account_summaries else 0
        }
    
    # User metrics
    user_summaries = general_results.get('user_summaries', [])
    if user_summaries:
        stats['user_metrics'] = {
            'total_users': len(user_summaries),
            'high_activity_users': len([u for u in user_summaries if u.get('transaction_count', 0) > 100]),
            'high_value_users': len([u for u in user_summaries if abs(u.get('total_amount', 0)) > 1000000]),
            'average_user_activity': sum(u.get('transaction_count', 0) for u in user_summaries) / len(user_summaries) if user_summaries else 0
        }
    
    # Performance metrics
    stats['performance_metrics'] = {
        'data_completeness': 'High' if unique_accounts > 0 and unique_users > 0 else 'Medium' if unique_accounts > 0 or unique_users > 0 else 'Low',
        'data_quality_score': min(100, (unique_accounts * 2 + unique_users * 2)),
        'recommendation_priority': 'High' if not trial_balance.get('is_balanced', True) else 'Medium' if unique_accounts < 10 else 'Low'
    }
    
    return stats

def run_duplicate_analysis_sync(job_id):
    """Run Duplicate Analysis synchronously and save to database with ML integration"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, DuplicateAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Duplicate Analysis for {len(transactions)} transactions")
        
        # Run basic duplicate analysis
        duplicate_pairs = []
        duplicates_found = 0
        
        # Enhanced duplicate detection based on amount, date, account, and credit/debit validation
        transaction_dict = {}
        for t in transactions:
            # Create a more comprehensive key that includes transaction type for credit/debit validation
            key = (float(t.amount_local_currency), t.posting_date, t.gl_account, t.transaction_type)
            if key in transaction_dict:
                # Found a potential duplicate - validate both transactions exist
                transaction1 = transaction_dict[key]
                transaction2 = t
                
                # Ensure both transactions are valid and not None
                if transaction1 and transaction2:
                    # Validate credit/debit consistency
                    if transaction1.transaction_type == transaction2.transaction_type:
                        # Same transaction type - this is a true duplicate
                        duplicate_pairs.append({
                            'transaction1': {
                                'id': str(transaction1.id),
                                'document_number': transaction1.document_number,
                                'amount': float(transaction1.amount_local_currency),
                                'posting_date': transaction1.posting_date.isoformat() if transaction1.posting_date else None,
                                'user': transaction1.user_name,
                                'account': transaction1.gl_account,
                                'transaction_type': transaction1.transaction_type,
                                'debit_credit': 'DEBIT' if transaction1.transaction_type == 'DEBIT' else 'CREDIT'
                            },
                            'transaction2': {
                                'id': str(transaction2.id),
                                'document_number': transaction2.document_number,
                                'amount': float(transaction2.amount_local_currency),
                                'posting_date': transaction2.posting_date.isoformat() if transaction2.posting_date else None,
                                'user': transaction2.user_name,
                                'account': transaction2.gl_account,
                                'transaction_type': transaction2.transaction_type,
                                'debit_credit': 'DEBIT' if transaction2.transaction_type == 'DEBIT' else 'CREDIT'
                            },
                            'similarity_score': 1.0,
                            'risk_level': 'HIGH',
                            'duplicate_type': 'exact_match',
                            'credit_debit_consistent': True,
                            'validation_notes': 'Both transactions are same type (DEBIT/CREDIT)'
                        })
                        duplicates_found += 1
                    else:
                        # Different transaction types - this might be a legitimate offsetting entry
                        # Log as potential anomaly but not as duplicate
                        logger.info(f"Potential offsetting entries found: {transaction1.transaction_type} vs {transaction2.transaction_type} for account {t.gl_account}")
                else:
                    logger.warning(f"Invalid transaction found during duplicate detection: transaction1={transaction1}, transaction2={transaction2}")
            else:
                transaction_dict[key] = t
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_confidence_scores = []
        ml_detection_method = 'rule_based'
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully")
            
            # Run ML duplicate prediction
            ml_results = ml_trainer.predict_duplicates(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML confidence scores
                if 'confidence_scores' in ml_results:
                    ml_confidence_scores = ml_results['confidence_scores']
                
                # Update duplicate pairs with ML insights
                if 'duplicate_transactions' in ml_results and ml_results['duplicate_transactions']:
                    ml_duplicates = ml_results['duplicate_transactions']
                    
                    # Merge ML predictions with rule-based results
                    for ml_dup in ml_duplicates:
                        # Check if this ML duplicate is already in our rule-based results
                        existing = False
                        for existing_dup in duplicate_pairs:
                            if (existing_dup['transaction1']['id'] == ml_dup['transaction_id'] or 
                                existing_dup['transaction2']['id'] == ml_dup['transaction_id']):
                                existing = True
                                # Enhance existing duplicate with ML confidence
                                existing_dup['ml_confidence'] = ml_dup.get('confidence', 0.8)
                                existing_dup['ml_detected'] = True
                                break
                        
                        if not existing:
                            # For ML-detected duplicates, we need to find a matching transaction
                            # Look for transactions with similar characteristics
                            matching_transaction = None
                            for t in transactions:
                                if (str(t.id) != ml_dup['transaction_id'] and 
                                    abs(float(t.amount_local_currency or 0) - ml_dup['amount']) < 0.01 and
                                    t.gl_account == ml_dup['gl_account'] and
                                    t.posting_date and ml_dup['posting_date'] and
                                    t.posting_date.strftime('%Y-%m-%d') == ml_dup['posting_date']):
                                    matching_transaction = t
                                    break
                            
                            if matching_transaction:
                                # Add new ML-detected duplicate with both transactions
                                # Validate credit/debit consistency
                                transaction1_type = ml_dup.get('transaction_type', 'DEBIT')  # Default to DEBIT if not specified
                                transaction2_type = matching_transaction.transaction_type
                                
                                if transaction1_type == transaction2_type:
                                    # Same transaction type - this is a true duplicate
                                    duplicate_pairs.append({
                                        'transaction1': {
                                            'id': ml_dup['transaction_id'],
                                            'amount': ml_dup['amount'],
                                            'gl_account': ml_dup['gl_account'],
                                            'user_name': ml_dup['user_name'],
                                            'posting_date': ml_dup['posting_date'],
                                            'transaction_type': transaction1_type,
                                            'debit_credit': 'DEBIT' if transaction1_type == 'DEBIT' else 'CREDIT'
                                        },
                                        'transaction2': {
                                            'id': str(matching_transaction.id),
                                            'amount': float(matching_transaction.amount_local_currency or 0),
                                            'gl_account': matching_transaction.gl_account,
                                            'user_name': matching_transaction.user_name,
                                            'posting_date': matching_transaction.posting_date.isoformat() if matching_transaction.posting_date else None,
                                            'transaction_type': transaction2_type,
                                            'debit_credit': 'DEBIT' if transaction2_type == 'DEBIT' else 'CREDIT'
                                        },
                                        'similarity_score': ml_dup.get('confidence', 0.8),
                                        'risk_level': 'MEDIUM' if ml_dup.get('confidence', 0.8) < 0.9 else 'HIGH',
                                        'ml_detected': True,
                                        'ml_confidence': ml_dup.get('confidence', 0.8),
                                        'duplicate_type': 'ml_detected',
                                        'credit_debit_consistent': True,
                                        'validation_notes': 'ML-detected duplicate with consistent credit/debit types'
                                    })
                                    duplicates_found += 1
                                else:
                                    # Different transaction types - log as potential anomaly but not as duplicate
                                    logger.info(f"ML detected potential offsetting entries: {transaction1_type} vs {transaction2_type} for account {ml_dup['gl_account']}")
                            else:
                                # If no matching transaction found, log this as a potential anomaly but don't add to duplicates
                                logger.warning(f"ML detected potential duplicate for transaction {ml_dup['transaction_id']} but no matching transaction found")
                
                logger.info(f"ML duplicate detection completed: {ml_results.get('duplicate_count', 0)} ML-detected duplicates")
                
            else:
                logger.warning("ML duplicate detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML duplicate detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        # Validate all duplicate pairs before finalizing
        validated_duplicate_pairs = []
        for dup in duplicate_pairs:
            # Ensure both transactions exist and are not None
            if (dup.get('transaction1') and dup.get('transaction2') and 
                dup['transaction1'] is not None and dup['transaction2'] is not None):
                
                # Validate credit/debit consistency
                transaction1_type = dup['transaction1'].get('transaction_type', 'DEBIT')
                transaction2_type = dup['transaction2'].get('transaction_type', 'DEBIT')
                
                if transaction1_type == transaction2_type:
                    # Valid duplicate - add to validated list
                    validated_duplicate_pairs.append(dup)
                else:
                    # Different transaction types - log as potential offsetting entry
                    logger.info(f"Filtering out potential offsetting entry: {transaction1_type} vs {transaction2_type}")
            else:
                # Invalid duplicate - log warning
                logger.warning(f"Filtering out invalid duplicate with missing transactions: {dup}")
        
        # Update duplicates_found count with validated pairs
        duplicates_found = len(validated_duplicate_pairs)
        duplicate_pairs = validated_duplicate_pairs
        
        duplicate_results = {
            'duplicates_found': duplicates_found,
            'duplicate_pairs': duplicate_pairs,
            'duplicate_by_amount': [],
            'duplicate_by_account': [],
            'duplicate_by_user': [],
            'audit_recommendations': [
                'Review duplicate transactions for potential errors',
                'Verify if duplicates are intentional or data entry errors'
            ],
            'compliance_assessment': {
                'duplicate_percentage': (duplicates_found / len(transactions) * 100) if transactions else 0.0,
                'risk_level': 'HIGH' if duplicates_found > 0 else 'LOW'
            },
            'financial_statement_impact': {
                'potential_overstatement': duplicates_found * 2,  # Rough estimate
                'impact_level': 'HIGH' if duplicates_found > 0 else 'LOW'
            },
            'chart_data': {
                'duplicate_summary': {
                    'total_duplicates': duplicates_found,
                    'total_transactions': len(transactions)
                }
            },
            'export_data': duplicate_pairs,
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'confidence_scores': ml_confidence_scores,
                'ml_enhanced_duplicates': len([d for d in duplicate_pairs if d.get('ml_detected', False)]),
                'rule_based_duplicates': len([d for d in duplicate_pairs if not d.get('ml_detected', False)]),
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'false_positive_indicators': ml_predictions.get('false_positive_indicators', []) if ml_predictions else [],
            'confidence_scores': ml_confidence_scores if ml_confidence_scores else [1.0] * duplicates_found,
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Create comprehensive statistics for duplicate analysis
        duplicate_stats = create_duplicate_analysis_statistics(
            duplicate_results, transactions, data_file, duplicate_pairs
        )
        
        # Save to DuplicateAnalysisResult table using new unified structure
        duplicate_analysis_result = DuplicateAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_duplicate',
            analysis_version='2.0.0',
            analysis_summary={
                'total_transactions': len(transactions),
                'duplicates_found': duplicate_results.get('duplicates_found', 0),
                'duplicate_percentage': duplicate_results.get('compliance_assessment', {}).get('duplicate_percentage', 0),
                'ml_enhanced': duplicate_results.get('ml_insights', {}).get('ml_available', False),
                'detection_method': duplicate_results.get('ml_insights', {}).get('detection_method', 'rule_based'),
                'statistics': duplicate_stats  # Add comprehensive statistics
            },
            anomaly_list=duplicate_results.get('duplicate_pairs', []),
            duplicate_list=duplicate_results.get('duplicate_pairs', []),
            breakdowns={
                'duplicate_by_document': duplicate_results.get('duplicate_by_amount', []),
                'duplicate_by_account': duplicate_results.get('duplicate_by_account', []),
                'duplicate_by_user': duplicate_results.get('duplicate_by_user', []),
                'audit_recommendations': duplicate_results.get('audit_recommendations', []),
                'compliance_assessment': duplicate_results.get('compliance_assessment', {}),
                'financial_statement_impact': duplicate_results.get('financial_statement_impact', {}),
                'ml_insights': duplicate_results.get('ml_insights', {}),
                'detection_methods': duplicate_results.get('detection_methods', {}),
                'statistics': duplicate_stats  # Add statistics to breakdowns as well
            },
            chart_data=duplicate_results.get('chart_data', {}),
            export_data=duplicate_results.get('export_data', []),
            processing_duration=duplicate_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        # Update SAPGLPosting records with duplicate flags and anomaly analysis
        duplicate_transaction_ids = set()
        for dup in duplicate_pairs:
            if dup.get('transaction1', {}).get('id'):
                duplicate_transaction_ids.add(dup['transaction1']['id'])
            if dup.get('transaction2', {}).get('id'):
                duplicate_transaction_ids.add(dup['transaction2']['id'])
        
        # Update SAPGLPosting records
        updated_count = 0
        for transaction_id in duplicate_transaction_ids:
            try:
                posting = SAPGLPosting.objects.get(id=transaction_id)
                posting.is_duplicate = True
                posting.duplicate_type = 'exact_match'
                posting.duplicate_risk_score = 85.0  # High risk for duplicates
                
                # Update anomaly analysis summary
                if not posting.anomaly_analysis_summary:
                    posting.anomaly_analysis_summary = {}
                
                posting.anomaly_analysis_summary.update({
                    'duplicate_detected': True,
                    'duplicate_analysis_id': str(duplicate_analysis_result.id),
                    'duplicate_risk_level': 'HIGH',
                    'duplicate_confidence': 1.0,
                    'credit_debit_consistent': True,
                    'last_updated': timezone.now().isoformat()
                })
                
                # Add to anomaly types if not already present
                if 'duplicate' not in posting.anomaly_types:
                    posting.anomaly_types.append('duplicate')
                
                # Update overall risk score
                posting.overall_risk_score = min(100.0, posting.overall_risk_score + 25.0)
                
                posting.save()
                updated_count += 1
                
            except SAPGLPosting.DoesNotExist:
                logger.warning(f"SAPGLPosting with ID {transaction_id} not found for duplicate update")
            except Exception as e:
                logger.error(f"Error updating SAPGLPosting {transaction_id}: {e}")
        
        logger.info(f"Updated {updated_count} SAPGLPosting records with duplicate flags")
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Duplicate Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(duplicate_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'duplicates_found': duplicate_results.get('duplicates_found', 0),
            'table': 'DuplicateAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Duplicate Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            DuplicateAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_duplicate',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_backdated_analysis_sync(job_id):
    """Run Backdated Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, BackdatedAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Backdated Analysis for {len(transactions)} transactions")
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_detection_method = 'rule_based'
        ml_detected_backdated = []
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully for backdated analysis")
            
            # Run ML backdated prediction
            ml_results = ml_trainer.predict_backdated(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML detected backdated transactions
                if 'backdated_transactions' in ml_results:
                    ml_backdated = ml_results['backdated_transactions']
                    
                    # Process ML detected backdated transactions
                    for ml_back in ml_backdated:
                        ml_detected_backdated.append({
                            'transaction_id': ml_back['transaction_id'],
                            'document_number': ml_back.get('document_number', ''),
                            'amount': ml_back.get('amount', 0),
                            'document_date': ml_back.get('document_date', ''),
                            'posting_date': ml_back.get('posting_date', ''),
                            'days_difference': ml_back.get('delay_days', 0),
                            'user': ml_back.get('user_name', ''),
                            'account': ml_back.get('gl_account', ''),
                            'risk_level': 'HIGH' if ml_back.get('confidence', 0) > 0.8 else 'MEDIUM',
                            'ml_detected': True,
                            'ml_confidence': ml_back.get('confidence', 0.8)
                        })
                    
                    logger.info(f"ML backdated detection completed: {len(ml_detected_backdated)} ML-detected backdated transactions")
                    
            else:
                logger.warning("ML backdated detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML backdated detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        # Run basic backdated analysis (rule-based)
        from datetime import timedelta
        
        backdated_transactions = []
        backdated_by_user = {}
        backdated_by_account = {}
        
        # Define what constitutes "backdated" (transactions posted more than 7 days after the posting date)
        backdated_threshold = timedelta(days=7)
        
        for t in transactions:
            if t.posting_date and t.document_date:
                days_difference = (t.posting_date - t.document_date).days
                if days_difference > 0:  # Backdated transaction (posting date after document date)
                    backdated_transactions.append({
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'amount': float(t.amount_local_currency),
                        'document_date': t.document_date.isoformat(),
                        'posting_date': t.posting_date.isoformat(),
                        'days_difference': days_difference,
                        'user': t.user_name,
                        'account': t.gl_account,
                        'risk_level': 'HIGH' if days_difference > 30 else 'MEDIUM'
                    })
                    
                    # Group by user
                    if t.user_name not in backdated_by_user:
                        backdated_by_user[t.user_name] = []
                    backdated_by_user[t.user_name].append(t)
                    
                    # Group by account
                    if t.gl_account not in backdated_by_account:
                        backdated_by_account[t.gl_account] = []
                    backdated_by_account[t.gl_account].append(t)
        
        # Merge ML and rule-based results
        backdated_transactions.extend(ml_detected_backdated)
        
        # Create backdated_results first
        backdated_results = {
            'backdated_transactions': backdated_transactions,
            'backdated_by_user': {user: len(transactions) for user, transactions in backdated_by_user.items()},
            'backdated_by_account': {account: len(transactions) for account, transactions in backdated_by_account.items()},
            'audit_recommendations': [
                'Review backdated transactions for proper authorization',
                'Verify if backdating is allowed by company policy',
                'Check for potential fraud or manipulation'
            ],
            'compliance_assessment': {
                'backdated_percentage': (len(backdated_transactions) / len(transactions) * 100) if transactions else 0.0,
                'risk_level': 'HIGH' if len(backdated_transactions) > 0 else 'LOW'
            },
            'financial_statement_impact': {
                'potential_misstatement': len(backdated_transactions),
                'impact_level': 'HIGH' if len(backdated_transactions) > 0 else 'LOW'
            },
            'chart_data': {
                'backdated_summary': {
                    'total_backdated': len(backdated_transactions),
                    'total_transactions': len(transactions)
                }
            },
            'export_data': backdated_transactions,
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'ml_detected_backdated': len(ml_detected_backdated),
                'rule_based_backdated': len(backdated_transactions) - len(ml_detected_backdated),
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Create comprehensive statistics for backdated analysis
        backdated_stats = create_backdated_analysis_statistics(
            backdated_results, transactions, data_file, backdated_transactions
        )
        
        # Save to BackdatedAnalysisResult table using new unified structure
        backdated_analysis_result = BackdatedAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_backdated',
            analysis_version='2.0.0',
            analysis_summary={
                'total_transactions': len(transactions),
                'backdated_entries_found': len(backdated_transactions),
                'backdated_percentage': backdated_results.get('compliance_assessment', {}).get('backdated_percentage', 0),
                'statistics': backdated_stats  # Add comprehensive statistics
            },
            anomaly_list=backdated_results.get('backdated_transactions', []),
            backdated_entries=backdated_results.get('backdated_transactions', []),
            backdated_by_document=backdated_results.get('backdated_by_document', []),
            backdated_by_account=backdated_results.get('backdated_by_account', []),
            backdated_by_user=backdated_results.get('backdated_by_user', []),
            audit_recommendations=backdated_results.get('audit_recommendations', []),
            compliance_assessment=backdated_results.get('compliance_assessment', {}),
            financial_statement_impact=backdated_results.get('financial_statement_impact', {}),
            chart_data=backdated_results.get('chart_data', {}),
            export_data=backdated_results.get('export_data', []),
            processing_duration=backdated_results.get('processing_duration', 0),
            breakdowns={
                'ml_insights': backdated_results.get('ml_insights', {}),
                'detection_methods': backdated_results.get('detection_methods', {}),
                'statistics': backdated_stats  # Add statistics to breakdowns
            },
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Backdated Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(backdated_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'backdated_entries_found': backdated_results.get('backdated_entries_found', 0),
            'table': 'BackdatedAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Backdated Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            BackdatedAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_backdated',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_manual_entry_analysis_sync(job_id):
    """Run Manual Entry Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, ManualEntryAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Manual Entry Analysis for file: {data_file.file_name}")
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Manual Entry Analysis for {len(transactions)} transactions")
        
        # Initialize analysis results
        manual_entries = []
        period_end_adjustments = []
        management_override_indicators = []
        
        # Analyze each transaction for manual entry indicators
        for transaction in transactions:
            manual_entry_data = {}
            
            # Check if it's a manual entry
            if transaction.is_manual_entry:
                manual_entry_data = {
                    'transaction_id': str(transaction.id),
                    'document_number': transaction.document_number,
                    'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
                    'document_date': transaction.document_date.isoformat() if transaction.document_date else None,
                    'gl_account': transaction.gl_account,
                    'amount': float(transaction.amount_local_currency),
                    'user_name': transaction.user_name,
                    'text': transaction.text,
                    'document_type': transaction.document_type,
                    'manual_entry_type': _determine_manual_entry_type(transaction),
                    'risk_score': _calculate_manual_entry_risk_score(transaction),
                    'risk_level': _determine_risk_level(_calculate_manual_entry_risk_score(transaction)),
                    'management_override_indicators': _identify_management_override_indicators(transaction)
                }
                
                manual_entries.append(manual_entry_data)
                
                # Check for period-end adjustments
                if transaction.is_period_end_adjustment:
                    period_end_adjustments.append(manual_entry_data)
                
                # Check for management override indicators
                override_indicators = _identify_management_override_indicators(transaction)
                if override_indicators:
                    management_override_indicators.append({
                        'transaction_id': str(transaction.id),
                        'indicators': override_indicators,
                        'risk_score': manual_entry_data['risk_score']
                    })
        
        logger.info(f"Found {len(manual_entries)} manual entries")
        logger.info(f"Found {len(period_end_adjustments)} period-end adjustments")
        logger.info(f"Found {len(management_override_indicators)} management override indicators")
        
        # Calculate risk distribution
        risk_distribution = _calculate_manual_entry_risk_distribution(manual_entries)
        
        # Calculate amount analysis
        amount_analysis = _calculate_manual_entry_amount_analysis(manual_entries)
        
        # Calculate user analysis
        user_analysis = _calculate_manual_entry_user_analysis(manual_entries)
        
        # Calculate account analysis
        account_analysis = _calculate_manual_entry_account_analysis(manual_entries)
        
        # Create analysis result
        manual_entry_analysis = ManualEntryAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_summary={
                'total_transactions': len(transactions),
                'manual_entries_count': len(manual_entries),
                'period_end_adjustments_count': len(period_end_adjustments),
                'management_override_indicators_count': len(management_override_indicators),
                'high_risk_manual_entries': len([e for e in manual_entries if e['risk_level'] in ['HIGH', 'CRITICAL']])
            },
            anomaly_list=manual_entries,
            chart_data={
                'risk_distribution': risk_distribution,
                'amount_analysis': amount_analysis,
                'user_analysis': user_analysis,
                'account_analysis': account_analysis
            },
            risk_assessment={
                'overall_risk_score': _calculate_overall_manual_entry_risk_score(manual_entries),
                'risk_distribution': risk_distribution,
                'high_risk_entries': len([e for e in manual_entries if e['risk_level'] in ['HIGH', 'CRITICAL']])
            },
            audit_recommendations={
                'high_priority': [e for e in manual_entries if e['risk_level'] == 'CRITICAL'],
                'medium_priority': [e for e in manual_entries if e['risk_level'] == 'HIGH'],
                'low_priority': [e for e in manual_entries if e['risk_level'] in ['MEDIUM', 'LOW']]
            },
            compliance_assessment={
                'management_override_risk': 'HIGH' if management_override_indicators else 'LOW',
                'period_end_adjustment_risk': 'HIGH' if period_end_adjustments else 'LOW',
                'manual_entry_compliance': _assess_manual_entry_compliance(manual_entries)
            },
            export_data=manual_entries,
            # Manual entry specific fields
            manual_entries=manual_entries,
            manual_entry_risk_distribution=risk_distribution,
            manual_entry_amount_analysis=amount_analysis,
            manual_entry_user_analysis=user_analysis,
            manual_entry_account_analysis=account_analysis,
            period_end_adjustments=period_end_adjustments,
            management_override_indicators=management_override_indicators,
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Manual Entry Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(manual_entry_analysis.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'manual_entries_found': len(manual_entries),
            'table': 'ManualEntryAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Manual Entry Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database
        try:
            ManualEntryAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='manual_entry_analysis',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def _determine_manual_entry_type(transaction):
    """Determine the type of manual entry"""
    if transaction.document_type:
        doc_type = transaction.document_type.upper()
        if 'MANUAL' in doc_type:
            return 'MANUAL'
        elif 'ADJUSTMENT' in doc_type:
            return 'ADJUSTMENT'
        elif 'CORRECTION' in doc_type:
            return 'CORRECTION'
        elif 'REVERSAL' in doc_type:
            return 'REVERSAL'
    
    if transaction.text:
        text = transaction.text.lower()
        if 'manual' in text:
            return 'MANUAL'
        elif 'adjustment' in text:
            return 'ADJUSTMENT'
        elif 'correction' in text:
            return 'CORRECTION'
        elif 'reversal' in text:
            return 'REVERSAL'
    
    return 'UNKNOWN'

def _calculate_manual_entry_risk_score(transaction):
    """Calculate risk score for manual entry"""
    risk_score = 50  # Base score for manual entries
    
    # Increase risk for high amounts
    if abs(transaction.amount_local_currency) > 1000000:
        risk_score += 30
    elif abs(transaction.amount_local_currency) > 100000:
        risk_score += 20
    
    # Increase risk for period-end adjustments
    if transaction.is_period_end_adjustment:
        risk_score += 25
    
    # Increase risk for specific account types
    if transaction.gl_account in ['999999', '888888', '777777', '666666']:
        risk_score += 20
    
    return min(risk_score, 100)

def _determine_risk_level(risk_score):
    """Determine risk level based on score"""
    if risk_score >= 80:
        return 'CRITICAL'
    elif risk_score >= 60:
        return 'HIGH'
    elif risk_score >= 40:
        return 'MEDIUM'
    else:
        return 'LOW'

def _identify_management_override_indicators(transaction):
    """Identify management override indicators"""
    indicators = []
    
    # High amount manual entries
    if abs(transaction.amount_local_currency) > 1000000:
        indicators.append('HIGH_AMOUNT_MANUAL_ENTRY')
    
    # Period-end adjustments
    if transaction.is_period_end_adjustment:
        indicators.append('PERIOD_END_ADJUSTMENT')
    
    # Specific manual entry accounts
    if transaction.gl_account in ['999999', '888888', '777777', '666666']:
        indicators.append('SUSPICIOUS_ACCOUNT')
    
    # Manual entries without proper documentation
    if not transaction.text or len(transaction.text.strip()) < 10:
        indicators.append('INSUFFICIENT_DOCUMENTATION')
    
    return indicators

def _calculate_manual_entry_risk_distribution(manual_entries):
    """Calculate risk distribution for manual entries"""
    distribution = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
    
    for entry in manual_entries:
        risk_level = entry.get('risk_level', 'LOW')
        distribution[risk_level] += 1
    
    return distribution

def _calculate_manual_entry_amount_analysis(manual_entries):
    """Calculate amount analysis for manual entries"""
    if not manual_entries:
        return {'total_amount': 0, 'average_amount': 0, 'max_amount': 0, 'min_amount': 0}
    
    amounts = [abs(entry['amount']) for entry in manual_entries]
    
    return {
        'total_amount': sum(amounts),
        'average_amount': sum(amounts) / len(amounts),
        'max_amount': max(amounts),
        'min_amount': min(amounts)
    }

def _calculate_manual_entry_user_analysis(manual_entries):
    """Calculate user analysis for manual entries"""
    user_counts = {}
    
    for entry in manual_entries:
        user = entry.get('user_name', 'Unknown')
        user_counts[user] = user_counts.get(user, 0) + 1
    
    return user_counts

def _calculate_manual_entry_account_analysis(manual_entries):
    """Calculate account analysis for manual entries"""
    account_counts = {}
    
    for entry in manual_entries:
        account = entry.get('gl_account', 'Unknown')
        account_counts[account] = account_counts.get(account, 0) + 1
    
    return account_counts

def _calculate_overall_manual_entry_risk_score(manual_entries):
    """Calculate overall risk score for manual entries"""
    if not manual_entries:
        return 0
    
    total_risk = sum(entry.get('risk_score', 0) for entry in manual_entries)
    return min(total_risk / len(manual_entries), 100)

def _assess_manual_entry_compliance(manual_entries):
    """Assess compliance for manual entries"""
    if not manual_entries:
        return 'COMPLIANT'
    
    high_risk_count = len([e for e in manual_entries if e.get('risk_level') in ['HIGH', 'CRITICAL']])
    total_count = len(manual_entries)
    
    if high_risk_count / total_count > 0.3:
        return 'NON_COMPLIANT'
    elif high_risk_count / total_count > 0.1:
        return 'NEEDS_ATTENTION'
    else:
        return 'COMPLIANT'

def run_user_analysis_sync(job_id):
    """Run User Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, UserAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running User Analysis for {len(transactions)} transactions")
        
        # Run basic user analysis without ML orchestrator
        user_summary = {}
        user_anomalies = []
        
        # Group transactions by user
        for t in transactions:
            user = t.user_name
            if user not in user_summary:
                user_summary[user] = {
                    'user': user,
                    'total_amount': 0.0,
                    'transaction_count': 0,
                    'accounts': set(),
                    'avg_amount': 0.0
                }
            user_summary[user]['total_amount'] += float(t.amount_local_currency)
            user_summary[user]['transaction_count'] += 1
            user_summary[user]['accounts'].add(t.gl_account)
        
        # Calculate averages and identify anomalies
        for user, data in user_summary.items():
            data['avg_amount'] = data['total_amount'] / data['transaction_count']
            data['accounts'] = list(data['accounts'])
            
            # Simple anomaly detection: users with very high amounts or many transactions
            if data['total_amount'] > 1000000 or data['transaction_count'] > 100:
                user_anomalies.append({
                    'user': user,
                    'anomaly_type': 'HIGH_ACTIVITY',
                    'risk_level': 'HIGH',
                    'details': f"User has {data['transaction_count']} transactions totaling {data['total_amount']}"
                })
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_detection_method = 'rule_based'
        ml_detected_anomalies = []
        anomaly_severity_breakdown = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully for user analysis")
            
            # Run ML user anomaly prediction
            ml_results = ml_trainer.predict_user_anomalies(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML detected anomalies
                if 'user_anomalies' in ml_results:
                    ml_user_anomalies = ml_results['user_anomalies']
                    
                    # Process ML detected anomalies
                    for user, ml_data in ml_user_anomalies.items():
                        if ml_data.get('anomaly_score', 0) > 0.7:  # High confidence anomalies
                            ml_detected_anomalies.append({
                                'user': user,
                                'anomaly_type': 'ML_DETECTED',
                                'risk_level': 'HIGH' if ml_data.get('anomaly_score', 0) > 0.8 else 'MEDIUM',
                                'details': f"ML detected anomaly: {ml_data.get('transaction_count', 0)} transactions, {ml_data.get('total_amount', 0)} total",
                                'ml_confidence': ml_data.get('anomaly_score', 0),
                                'ml_detected': True
                            })
                            
                            # Update severity breakdown
                            if ml_data.get('anomaly_score', 0) > 0.9:
                                anomaly_severity_breakdown['HIGH'] += 1
                            elif ml_data.get('anomaly_score', 0) > 0.7:
                                anomaly_severity_breakdown['MEDIUM'] += 1
                            else:
                                anomaly_severity_breakdown['LOW'] += 1
                
                # Merge ML anomalies with rule-based anomalies
                user_anomalies.extend(ml_detected_anomalies)
                
                logger.info(f"ML user anomaly detection completed: {len(ml_detected_anomalies)} ML-detected anomalies")
                
            else:
                logger.warning("ML user anomaly detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML user anomaly detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        user_results = {
            'total_users': len(user_summary),
            'user_transaction_summary': list(user_summary.values()),
            'user_debit_analysis': [],
            'user_account_distribution': [],
            'user_anomalies': user_anomalies,
            'user_risk_assessment': [
                {
                    'user': user,
                    'risk_level': 'HIGH' if user in [a['user'] for a in user_anomalies] else 'LOW',
                    'risk_factors': [a['anomaly_type'] for a in user_anomalies if a['user'] == user]
                }
                for user in user_summary.keys()
            ],
            'statistical_summary': {
                'total_users': len(user_summary),
                'high_risk_users': len(user_anomalies),
                'users_with_anomalies': len(user_anomalies)
            },
            'chart_data': {
                'user_summary': list(user_summary.values())
            },
            'export_data': list(user_summary.values()),
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'ml_detected_anomalies': ml_detected_anomalies,
                'anomaly_severity_breakdown': anomaly_severity_breakdown,
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'ml_detected_anomalies': ml_detected_anomalies,
            'anomaly_severity_breakdown': anomaly_severity_breakdown,
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Create comprehensive statistics for user analysis
        user_stats = create_user_analysis_statistics(
            user_results, transactions, data_file, user_anomalies
        )
        
        # Save to UserAnalysisResult table using new unified structure
        user_analysis_result = UserAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='user_analysis',
            analysis_version='2.0.0',
            analysis_summary={
                'total_users': user_results.get('total_users', 0),
                'total_transactions': len(transactions),
                'high_risk_users': user_results.get('statistical_summary', {}).get('high_risk_users', 0),
                'users_with_anomalies': user_results.get('statistical_summary', {}).get('users_with_anomalies', 0),
                'ml_enhanced': user_results.get('ml_insights', {}).get('ml_available', False),
                'detection_method': user_results.get('ml_insights', {}).get('detection_method', 'rule_based'),
                'statistics': user_stats  # Add comprehensive statistics
            },
            anomaly_list=user_results.get('user_anomalies', []),
            user_transaction_summary=user_results.get('user_transaction_summary', []),
            user_debit_analysis=user_results.get('user_debit_analysis', []),
            user_account_distribution=user_results.get('user_account_distribution', []),
            user_anomalies=user_results.get('user_anomalies', []),
            user_risk_assessment=user_results.get('user_risk_assessment', []),
            audit_recommendations=user_results.get('audit_recommendations', {}),
            compliance_assessment=user_results.get('compliance_assessment', {}),
            financial_statement_impact=user_results.get('financial_statement_impact', {}),
            chart_data=user_results.get('chart_data', {}),
            export_data=user_results.get('export_data', []),
            processing_duration=user_results.get('processing_duration', 0),
            breakdowns={
                'ml_insights': user_results.get('ml_insights', {}),
                'ml_detected_anomalies': user_results.get('ml_detected_anomalies', []),
                'anomaly_severity_breakdown': user_results.get('anomaly_severity_breakdown', {}),
                'detection_methods': user_results.get('detection_methods', {}),
                'statistics': user_stats  # Add statistics to breakdowns
            },
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"User Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(user_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'total_users': user_results.get('total_users', 0),
            'table': 'UserAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in User Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            UserAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='user_analysis',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_overall_analysis_sync(job_id):
    """Run Overall Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, OverallAnalysisResult
    from core.overall_analysis import OverallAnalyzer
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Overall Analysis for file: {data_file.file_name}")
        
        # Use the real OverallAnalyzer to run comprehensive analysis
        overall_analyzer = OverallAnalyzer()
        analysis_results = overall_analyzer.run_overall_analysis(data_file, job)
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Overall Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': analysis_results['analysis_id'],
            'risk_document_id': analysis_results['risk_document_id'],
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'OverallAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Overall Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database
        try:
            OverallAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='overall_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_risk_analysis_sync(job_id):
    """Run Risk Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, RiskScoringDocument, DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult, HolidayAnalysisResult
    from django.db.models import Avg, Max, Min
    import json
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Risk Analysis for file: {data_file.file_name}")
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        total_transactions = transactions.count()
        
        # Get all analysis results
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
        backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
        user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
        holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
        
        # Calculate anomaly counts
        duplicate_count = duplicate_analysis.get_duplicate_count() if duplicate_analysis else 0
        backdated_count = backdated_analysis.get_backdated_count() if backdated_analysis else 0
        user_anomalies_count = len(user_analysis.user_anomalies) if user_analysis and user_analysis.user_anomalies else 0
        unusual_days_count = unusual_days_analysis.get_weekend_transactions_count() if unusual_days_analysis else 0
        closing_entries_count = closing_entries_analysis.get_closing_entries_count() if closing_entries_analysis else 0
        holiday_count = len(holiday_analysis.holiday_postings) if holiday_analysis and holiday_analysis.holiday_postings else 0
        
        # Calculate total anomalies
        total_anomalies = duplicate_count + backdated_count + user_anomalies_count + unusual_days_count + closing_entries_count + holiday_count
        
        # Calculate risk scores based on anomalies
        risk_scores = []
        high_risk_count = 0
        medium_risk_count = 0
        low_risk_count = 0
        critical_risk_count = 0
        
        for transaction in transactions:
            risk_score = 0.0
            
            # Check for duplicate (high risk)
            if duplicate_analysis and duplicate_analysis.duplicate_list:
                for duplicate in duplicate_analysis.duplicate_list:
                    if (str(transaction.id) in [duplicate.get('transaction1', {}).get('id'), duplicate.get('transaction2', {}).get('id')]):
                        risk_score += 80
                        break
            
            # Check for backdated (high risk)
            if backdated_analysis and backdated_analysis.backdated_entries:
                for backdated in backdated_analysis.backdated_entries:
                    if str(transaction.id) == backdated.get('transaction_id'):
                        risk_score += 70
                        break
            
            # Check for user anomalies (medium risk)
            if user_analysis and user_analysis.user_anomalies:
                for anomaly in user_analysis.user_anomalies:
                    if transaction.user_name == anomaly.get('user_name'):
                        risk_score += 50
                        break
            
            # Check for unusual days (medium risk)
            if unusual_days_analysis and unusual_days_analysis.weekend_postings:
                for unusual in unusual_days_analysis.weekend_postings:
                    if str(transaction.id) == unusual.get('transaction_id'):
                        risk_score += 40
                        break
            
            # Check for closing entries (medium risk)
            if closing_entries_analysis and closing_entries_analysis.closing_entries:
                for closing in closing_entries_analysis.closing_entries:
                    if str(transaction.id) == closing.get('transaction_id'):
                        risk_score += 30
                        break
            
            # Check for holiday postings (high risk)
            if holiday_analysis and holiday_analysis.holiday_postings:
                for holiday in holiday_analysis.holiday_postings:
                    if str(transaction.id) == holiday.get('transaction_id'):
                        risk_score += 60
                        break
            
            # Cap risk score at 100
            risk_score = min(risk_score, 100.0)
            
            # Update transaction risk score and anomaly flags
            transaction.overall_risk_score = risk_score
            
            # Update specific anomaly flags
            anomaly_types = []
            
            # Check for duplicate (high risk)
            if duplicate_analysis and duplicate_analysis.duplicate_list:
                for duplicate in duplicate_analysis.duplicate_list:
                    if (str(transaction.id) in [duplicate.get('transaction1', {}).get('id'), duplicate.get('transaction2', {}).get('id')]):
                        transaction.is_duplicate = True
                        transaction.duplicate_risk_score = 80.0
                        anomaly_types.append('duplicate')
                        break
            
            # Check for backdated (high risk)
            if backdated_analysis and backdated_analysis.backdated_entries:
                for backdated in backdated_analysis.backdated_entries:
                    if str(transaction.id) == backdated.get('transaction_id'):
                        transaction.is_backdated = True
                        transaction.backdated_risk_score = 70.0
                        transaction.backdated_days = backdated.get('days_difference', 0)
                        anomaly_types.append('backdated')
                        break
            
            # Check for user anomalies (medium risk)
            if user_analysis and user_analysis.user_anomalies:
                for anomaly in user_analysis.user_anomalies:
                    if transaction.user_name == anomaly.get('user'):
                        transaction.is_user_anomaly = True
                        transaction.user_anomaly_type = anomaly.get('anomaly_type', 'HIGH_ACTIVITY')
                        transaction.user_anomaly_risk_score = 60.0
                        transaction.user_anomaly_analysis_details = {
                            'anomaly_type': anomaly.get('anomaly_type'),
                            'risk_level': anomaly.get('risk_level'),
                            'details': anomaly.get('details')
                        }
                        anomaly_types.append('user_anomaly')
                        break
            
            # Check for unusual days (medium risk)
            if unusual_days_analysis and unusual_days_analysis.weekend_postings:
                for unusual in unusual_days_analysis.weekend_postings:
                    if str(transaction.id) == unusual.get('transaction_id'):
                        transaction.is_unusual_days_posting = True
                        transaction.unusual_days_type = unusual.get('day_of_week', 'Weekend')
                        transaction.unusual_days_risk_score = 50.0
                        transaction.unusual_days_analysis_details = {
                            'day_of_week': unusual.get('day_of_week'),
                            'risk_level': unusual.get('risk_level'),
                            'posting_date': unusual.get('posting_date')
                        }
                        anomaly_types.append('unusual_days')
                        break
            
            # Check for closing entries (medium risk)
            if closing_entries_analysis and closing_entries_analysis.closing_entries:
                for closing in closing_entries_analysis.closing_entries:
                    if str(transaction.id) == closing.get('transaction_id'):
                        transaction.is_closing_entry = True
                        transaction.closing_entry_type = 'MONTH_END'
                        transaction.closing_entry_risk_score = 40.0
                        transaction.closing_entry_analysis_details = {
                            'days_from_month_end': closing.get('days_from_month_end'),
                            'risk_level': closing.get('risk_level'),
                            'posting_date': closing.get('posting_date')
                        }
                        anomaly_types.append('closing_entries')
                        break
            
            # Check for holiday postings (high risk)
            if holiday_analysis and holiday_analysis.holiday_postings:
                for holiday in holiday_analysis.holiday_postings:
                    if str(transaction.id) == holiday.get('transaction_id'):
                        transaction.is_holiday_posting = True
                        transaction.holiday_name = holiday.get('holiday_name', 'Unknown')
                        transaction.holiday_risk_score = holiday.get('risk_score', 60.0)
                        anomaly_types.append('holiday')
                        break
            
            # Update anomaly types list
            transaction.anomaly_types = anomaly_types
            
            # Update anomaly analysis summary
            transaction.anomaly_analysis_summary = {
                'risk_score': risk_score,
                'anomaly_types': anomaly_types,
                'is_duplicate': transaction.is_duplicate,
                'is_backdated': transaction.is_backdated,
                'is_holiday_posting': transaction.is_holiday_posting,
                'is_unusual_days_posting': transaction.is_unusual_days_posting,
                'is_user_anomaly': transaction.is_user_anomaly,
                'is_closing_entry': transaction.is_closing_entry,
                'holiday_name': transaction.holiday_name,
                'backdated_days': transaction.backdated_days,
                'unusual_days_type': transaction.unusual_days_type,
                'user_anomaly_type': transaction.user_anomaly_type,
                'closing_entry_type': transaction.closing_entry_type
            }
            
            transaction.save()
            
            risk_scores.append(risk_score)
            
            # Categorize risk levels
            if risk_score >= 80:
                critical_risk_count += 1
            elif risk_score >= 60:
                high_risk_count += 1
            elif risk_score >= 30:
                medium_risk_count += 1
            else:
                low_risk_count += 1
        
        # Calculate overall risk score based on risk distribution
        # Weight the risk score based on percentage of high/critical risk transactions
        high_risk_percentage = (high_risk_count + critical_risk_count) / total_transactions * 100 if total_transactions > 0 else 0
        critical_risk_percentage = critical_risk_count / total_transactions * 100 if total_transactions > 0 else 0
        
        # Calculate weighted risk score
        if critical_risk_percentage > 0:
            # If there are critical risk transactions, score should be HIGH
            overall_risk_score = 75.0 + (critical_risk_percentage * 0.5)  # Base 75 + critical risk bonus
        elif high_risk_percentage > 5:
            # If more than 5% are high risk, score should be HIGH
            overall_risk_score = 60.0 + (high_risk_percentage * 0.3)  # Base 60 + high risk bonus
        elif high_risk_percentage > 1:
            # If more than 1% are high risk, score should be MEDIUM
            overall_risk_score = 40.0 + (high_risk_percentage * 0.5)  # Base 40 + high risk bonus
        else:
            # Use average risk score for low risk scenarios
            overall_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
        
        # Cap risk score at 100
        overall_risk_score = min(overall_risk_score, 100.0)
        
        # Risk distribution
        risk_distribution = {
            'low_risk': low_risk_count,
            'medium_risk': medium_risk_count,
            'high_risk': high_risk_count,
            'critical_risk': critical_risk_count
        }
        
        # Risk factors based on anomalies
        risk_factors = {
            'duplicate_risk': {
                'count': duplicate_count,
                'percentage': (duplicate_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with duplicate transactions',
                'weight': 0.25
            },
            'backdated_risk': {
                'count': backdated_count,
                'percentage': (backdated_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with backdated entries',
                'weight': 0.20
            },
            'user_anomaly_risk': {
                'count': user_anomalies_count,
                'percentage': (user_anomalies_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with user behavior anomalies',
                'weight': 0.20
            },
            'unusual_days_risk': {
                'count': unusual_days_count,
                'percentage': (unusual_days_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with weekend/unusual day postings',
                'weight': 0.15
            },
            'closing_entries_risk': {
                'count': closing_entries_count,
                'percentage': (closing_entries_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with closing entries',
                'weight': 0.10
            },
            'holiday_risk': {
                'count': holiday_count,
                'percentage': (holiday_count / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with holiday postings',
                'weight': 0.10
            }
        }
        
        # Methodology overview
        methodology_overview = {
            'description': 'Simplified risk scoring based on anomaly analysis results',
            'version': '2.0.0',
            'analysis_date': timezone.now().isoformat(),
            'total_transactions_analyzed': total_transactions,
            'total_anomalies_found': total_anomalies,
            'anomaly_percentage': (total_anomalies / total_transactions * 100) if total_transactions > 0 else 0,
            'risk_score_range': {
                'min': min(risk_scores) if risk_scores else 0,
                'max': max(risk_scores) if risk_scores else 0,
                'average': overall_risk_score
            },
            'risk_levels': {
                'low_risk': {'min': 0, 'max': 29, 'description': 'Normal transactions'},
                'medium_risk': {'min': 30, 'max': 59, 'description': 'Some concerns'},
                'high_risk': {'min': 60, 'max': 79, 'description': 'Significant risk'},
                'critical_risk': {'min': 80, 'max': 100, 'description': 'High risk'}
            }
        }
        
        # Recommendations based on findings
        recommendations = []
        
        if critical_risk_count > 0:
            recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Immediate review of critical risk transactions',
                'description': f'{critical_risk_count} critical risk transactions require immediate attention',
                'count': critical_risk_count
            })
        
        if high_risk_count > 0:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Review high-risk transactions',
                'description': f'{high_risk_count} high-risk transactions need investigation',
                'count': high_risk_count
            })
        
        if duplicate_count > 0:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Investigate duplicate transactions',
                'description': f'{duplicate_count} duplicate transactions found',
                'count': duplicate_count
            })
        
        if backdated_count > 0:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Review backdated entries',
                'description': f'{backdated_count} backdated entries found',
                'count': backdated_count
            })
        
        if holiday_count > 0:
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Review holiday postings',
                'description': f'{holiday_count} holiday postings found',
                'count': holiday_count
            })
        
        # Create risk scoring document
        risk_document = RiskScoringDocument.objects.create(
            data_file=data_file,
            processing_job=job,
            document_type='simplified_risk_scoring',
            document_version='2.0.0',
            methodology_overview=methodology_overview,
            risk_factors=risk_factors,
            scoring_criteria={
                'duplicate_weight': 80,
                'backdated_weight': 70,
                'user_anomaly_weight': 50,
                'unusual_days_weight': 40,
                'closing_entries_weight': 30,
                'holiday_weight': 60
            },
            risk_calculations={
                'total_transactions': total_transactions,
                'total_anomalies': total_anomalies,
                'anomaly_percentage': (total_anomalies / total_transactions * 100) if total_transactions > 0 else 0
            },
            risk_distributions=risk_distribution,
            recommendations=recommendations,
            audit_implications={
                'high_risk_transactions': high_risk_count + critical_risk_count,
                'requires_immediate_attention': critical_risk_count,
                'sampling_recommendation': min(100, high_risk_count + critical_risk_count)
            },
            total_transactions=total_transactions,
            high_risk_transactions=high_risk_count,
            medium_risk_transactions=medium_risk_count,
            low_risk_transactions=low_risk_count,
            critical_risk_transactions=critical_risk_count,
            overall_risk_score=overall_risk_score,
            processing_duration=(timezone.now() - start_time).total_seconds(),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Risk Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(risk_document.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'risk_score': overall_risk_score,
            'total_anomalies': total_anomalies,
            'critical_risk': critical_risk_count,
            'high_risk': high_risk_count,
            'medium_risk': medium_risk_count,
            'low_risk': low_risk_count,
            'table': 'RiskScoringDocument'
        }
        
    except Exception as e:
        error_msg = f"Error in Risk Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database
        try:
            RiskScoringDocument.objects.create(
                data_file=data_file,
                processing_job=job,
                document_type='simplified_risk_scoring',
                document_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_unusual_days_analysis_sync(job_id):
    """Run Unusual Days Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, UnusualDaysAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Unusual Days Analysis for file: {data_file.file_name}")
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Unusual Days Analysis for {len(transactions)} transactions")
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_detection_method = 'rule_based'
        ml_detected_unusual = []
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully for unusual days analysis")
            
            # Run ML unusual days prediction
            ml_results = ml_trainer.predict_unusual_days(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML detected unusual day transactions
                if 'unusual_transactions' in ml_results:
                    ml_unusual = ml_results['unusual_transactions']
                    
                    # Process ML detected unusual transactions
                    for ml_unusual_tx in ml_unusual:
                        ml_detected_unusual.append({
                            'transaction_id': ml_unusual_tx['transaction_id'],
                            'document_number': ml_unusual_tx.get('document_number', ''),
                            'amount': ml_unusual_tx.get('amount', 0),
                            'posting_date': ml_unusual_tx.get('posting_date', ''),
                            'day_of_week': ml_unusual_tx.get('day_of_week', ''),
                            'user': ml_unusual_tx.get('user', ''),
                            'account': ml_unusual_tx.get('account', ''),
                            'risk_level': ml_unusual_tx.get('risk_level', 'MEDIUM'),
                            'ml_detected': True,
                            'ml_confidence': ml_unusual_tx.get('confidence', 0.8)
                        })
                    
                    logger.info(f"ML unusual days detection completed: {len(ml_detected_unusual)} ML-detected unusual transactions")
                    
            else:
                logger.warning("ML unusual days detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML unusual days detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        # Run basic unusual days analysis (rule-based)
        from datetime import datetime, timedelta
        
        unusual_days_transactions = []
        unusual_days_by_user = {}
        unusual_days_by_account = {}
        
        for t in transactions:
            if t.posting_date:
                # Check if posting is on weekend (Friday = 4, Saturday = 5)
                if t.posting_date.weekday() in [4, 5]:
                    unusual_days_transactions.append({
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'amount': float(t.amount_local_currency),
                        'posting_date': t.posting_date.isoformat(),
                        'day_of_week': t.posting_date.strftime('%A'),
                        'user': t.user_name,
                        'account': t.gl_account,
                        'risk_level': 'HIGH'
                    })
                    
                    # Group by user
                    if t.user_name not in unusual_days_by_user:
                        unusual_days_by_user[t.user_name] = []
                    unusual_days_by_user[t.user_name].append(t)
                    
                    # Group by account
                    if t.gl_account not in unusual_days_by_account:
                        unusual_days_by_account[t.gl_account] = []
                    unusual_days_by_account[t.gl_account].append(t)
        
        # Merge ML and rule-based results
        unusual_days_transactions.extend(ml_detected_unusual)
        
        unusual_days_results = {
            'unusual_days_transactions': unusual_days_transactions,
            'unusual_days_by_user': {user: len(transactions) for user, transactions in unusual_days_by_user.items()},
            'unusual_days_by_account': {account: len(transactions) for account, transactions in unusual_days_by_account.items()},
            'audit_recommendations': [
                'Review weekend transactions for proper authorization',
                'Verify if weekend postings are allowed by company policy',
                'Check for potential fraud or manipulation'
            ],
            'compliance_assessment': {
                'unusual_days_percentage': (len(unusual_days_transactions) / len(transactions) * 100) if transactions else 0.0,
                'risk_level': 'HIGH' if len(unusual_days_transactions) > 0 else 'LOW'
            },
            'financial_statement_impact': {
                'potential_misstatement': len(unusual_days_transactions),
                'impact_level': 'HIGH' if len(unusual_days_transactions) > 0 else 'LOW'
            },
            'chart_data': {
                'unusual_days_summary': {
                    'total_weekend_transactions': len(unusual_days_transactions),
                    'total_transactions': len(transactions),
                    'weekend_percentage': (len(unusual_days_transactions) / len(transactions) * 100) if transactions else 0.0
                },
                'weekend_by_month': _generate_weekend_by_month_chart(unusual_days_transactions),
                'weekend_by_user': _generate_weekend_by_user_chart(unusual_days_by_user),
                'weekend_by_account': _generate_weekend_by_account_chart(unusual_days_by_account),
                'weekend_amount_distribution': _generate_weekend_amount_chart(unusual_days_transactions),
                'weekend_risk_distribution': {
                    'high_risk': len([t for t in unusual_days_transactions if t.get('risk_level') == 'HIGH']),
                    'medium_risk': len([t for t in unusual_days_transactions if t.get('risk_level') == 'MEDIUM']),
                    'low_risk': len([t for t in unusual_days_transactions if t.get('risk_level') == 'LOW'])
                }
            },
            'export_data': unusual_days_transactions,
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'ml_detected_unusual': len(ml_detected_unusual),
                'rule_based_unusual': len(unusual_days_transactions) - len(ml_detected_unusual),
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Save to UnusualDaysAnalysisResult table using new unified structure
        unusual_days_analysis_result = UnusualDaysAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='unusual_days_analysis',
            analysis_version='2.0.0',
            analysis_summary={
                'total_transactions': len(transactions),
                'unusual_days_count': len(unusual_days_transactions)
            },
            anomaly_list=unusual_days_transactions,
            weekend_postings=unusual_days_transactions,
            unusual_days=unusual_days_transactions,
            audit_recommendations=unusual_days_results.get('audit_recommendations', {}),
            compliance_assessment=unusual_days_results.get('compliance_assessment', {}),
            financial_statement_impact=unusual_days_results.get('financial_statement_impact', {}),
            chart_data=unusual_days_results.get('chart_data', {}),
            breakdowns={
                'ml_insights': unusual_days_results.get('ml_insights', {}),
                'detection_methods': unusual_days_results.get('detection_methods', {})
            },
            export_data=unusual_days_results.get('export_data', []),
            processing_duration=unusual_days_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Unusual Days Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(unusual_days_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'unusual_days_found': len(unusual_days_transactions),
            'table': 'UnusualDaysAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Unusual Days Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            UnusualDaysAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='unusual_days_analysis',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_closing_entries_analysis_sync(job_id):
    """Run Closing Entries Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, ClosingEntriesAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Closing Entries Analysis for file: {data_file.file_name}")
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Closing Entries Analysis for {len(transactions)} transactions")
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_detection_method = 'rule_based'
        ml_detected_closing = []
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully for closing entries analysis")
            
            # Run ML closing entries prediction
            ml_results = ml_trainer.predict_closing_entries(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML detected closing entry transactions
                if 'closing_transactions' in ml_results:
                    ml_closing = ml_results['closing_transactions']
                    
                    # Process ML detected closing transactions
                    for ml_closing_tx in ml_closing:
                        ml_detected_closing.append({
                            'transaction_id': ml_closing_tx['transaction_id'],
                            'document_number': ml_closing_tx.get('document_number', ''),
                            'amount': ml_closing_tx.get('amount', 0),
                            'posting_date': ml_closing_tx.get('posting_date', ''),
                            'days_from_month_end': ml_closing_tx.get('days_from_month_end', 0),
                            'user': ml_closing_tx.get('user', ''),
                            'account': ml_closing_tx.get('account', ''),
                            'risk_level': ml_closing_tx.get('risk_level', 'MEDIUM'),
                            'ml_detected': True,
                            'ml_confidence': ml_closing_tx.get('confidence', 0.8)
                        })
                    
                    logger.info(f"ML closing entries detection completed: {len(ml_detected_closing)} ML-detected closing transactions")
                    
            else:
                logger.warning("ML closing entries detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML closing entries detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        # Run basic closing entries analysis (rule-based)
        from datetime import datetime, timedelta
        
        closing_entries_transactions = []
        closing_by_user = {}
        closing_by_account = {}
        
        for t in transactions:
            if t.posting_date:
                # Simple logic: consider transactions in last 3 days of month as potential closing entries
                last_day_of_month = (t.posting_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                days_from_month_end = (last_day_of_month - t.posting_date).days
                
                if days_from_month_end <= 3:
                    closing_entries_transactions.append({
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'amount': float(t.amount_local_currency),
                        'posting_date': t.posting_date.isoformat(),
                        'days_from_month_end': days_from_month_end,
                        'user': t.user_name,
                        'account': t.gl_account,
                        'risk_level': 'MEDIUM' if days_from_month_end <= 1 else 'LOW'
                    })
                    
                    # Group by user
                    if t.user_name not in closing_by_user:
                        closing_by_user[t.user_name] = []
                    closing_by_user[t.user_name].append(t)
                    
                    # Group by account
                    if t.gl_account not in closing_by_account:
                        closing_by_account[t.gl_account] = []
                    closing_by_account[t.gl_account].append(t)
        
        # Merge ML and rule-based results
        closing_entries_transactions.extend(ml_detected_closing)
        
        closing_entries_results = {
            'closing_entries_transactions': closing_entries_transactions,
            'closing_by_user': {user: len(transactions) for user, transactions in closing_by_user.items()},
            'closing_by_account': {account: len(transactions) for account, transactions in closing_by_account.items()},
            'audit_recommendations': [
                'Review closing entries for proper authorization',
                'Verify if closing entries are properly documented',
                'Check for potential manipulation of financial statements'
            ],
            'compliance_assessment': {
                'closing_entries_percentage': (len(closing_entries_transactions) / len(transactions) * 100) if transactions else 0.0,
                'risk_level': 'MEDIUM' if len(closing_entries_transactions) > 0 else 'LOW'
            },
            'financial_statement_impact': {
                'potential_misstatement': len(closing_entries_transactions),
                'impact_level': 'MEDIUM' if len(closing_entries_transactions) > 0 else 'LOW'
            },
            'chart_data': {
                'closing_entries_summary': {
                    'total_closing_entries': len(closing_entries_transactions),
                    'total_transactions': len(transactions)
                }
            },
            'export_data': closing_entries_transactions,
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'ml_detected_closing': len(ml_detected_closing),
                'rule_based_closing': len(closing_entries_transactions) - len(ml_detected_closing),
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Save to ClosingEntriesAnalysisResult table using new unified structure
        closing_entries_analysis_result = ClosingEntriesAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='closing_entries_analysis',
            analysis_version='2.0.0',
            analysis_summary={
                'total_transactions': len(transactions),
                'closing_entries_count': len(closing_entries_transactions)
            },
            anomaly_list=closing_entries_transactions,
            closing_entries=closing_entries_transactions,
            audit_recommendations=closing_entries_results.get('audit_recommendations', {}),
            compliance_assessment=closing_entries_results.get('compliance_assessment', {}),
            financial_statement_impact=closing_entries_results.get('financial_statement_impact', {}),
            chart_data=closing_entries_results.get('chart_data', {}),
            breakdowns={
                'ml_insights': closing_entries_results.get('ml_insights', {}),
                'detection_methods': closing_entries_results.get('detection_methods', {})
            },
            export_data=closing_entries_results.get('export_data', []),
            processing_duration=closing_entries_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Closing Entries Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(closing_entries_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'closing_entries_found': len(closing_entries_transactions),
            'table': 'ClosingEntriesAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Closing Entries Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            ClosingEntriesAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='closing_entries_analysis',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

def run_holiday_analysis_sync(job_id):
    """Run Holiday Analysis synchronously and save to database"""
    
    # Setup Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    django.setup()
    
    from core.models import FileProcessingJob, SAPGLPosting, HolidayAnalysisResult
    
    start_time = timezone.now()
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        logger.info(f"Running Holiday Analysis for file: {data_file.file_name}")
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        logger.info(f"Running Holiday Analysis for {len(transactions)} transactions")
        
        # =============================================================================
        # ML MODEL INTEGRATION - Run ML predictions during analysis
        # =============================================================================
        
        ml_predictions = {}
        ml_detection_method = 'rule_based'
        ml_detected_holidays = []
        
        try:
            # Import ML trainer
            from core.ml_models import MLModelTrainer
            
            # Initialize ML trainer
            ml_trainer = MLModelTrainer()
            logger.info("ML Model Trainer initialized successfully for holiday analysis")
            
            # Run ML holiday prediction
            ml_results = ml_trainer.predict_holidays(transactions)
            
            if ml_results and 'error' not in ml_results:
                ml_predictions = ml_results
                ml_detection_method = 'ml_enhanced'
                
                # Extract ML detected holiday transactions
                if 'holiday_transactions' in ml_results:
                    ml_holidays = ml_results['holiday_transactions']
                    
                    # Process ML detected holiday transactions
                    for ml_holiday_tx in ml_holidays:
                        ml_detected_holidays.append({
                            'transaction_id': ml_holiday_tx['transaction_id'],
                            'document_number': ml_holiday_tx.get('document_number', ''),
                            'amount': ml_holiday_tx.get('amount', 0),
                            'posting_date': ml_holiday_tx.get('posting_date', ''),
                            'holiday_name': ml_holiday_tx.get('holiday_name', 'ML Detected Holiday'),
                            'user': ml_holiday_tx.get('user', ''),
                            'account': ml_holiday_tx.get('account', ''),
                            'risk_level': ml_holiday_tx.get('risk_level', 'HIGH'),
                            'ml_detected': True,
                            'ml_confidence': ml_holiday_tx.get('confidence', 0.8)
                        })
                    
                    logger.info(f"ML holiday detection completed: {len(ml_detected_holidays)} ML-detected holiday transactions")
                    
            else:
                logger.warning("ML holiday detection failed, using rule-based results only")
                
        except Exception as e:
            logger.error(f"ML holiday detection error: {e}")
            # Continue with rule-based results
            ml_detection_method = 'rule_based_fallback'
        
        # Run basic holiday analysis (rule-based)
        from datetime import datetime, timedelta
        
        # Use holiday_utils to get dynamic Saudi Arabian holidays
        # Holiday utilities - simplified implementation since holiday_utils module was removed
        def get_holidays(country_code, start_date, end_date, include_observances=True):
            """Get holidays for a given date range - simplified implementation"""
            return []
        
        def is_holiday(date):
            """Check if a date is a holiday - simplified implementation"""
            return False
        
        # Get date range for holiday detection
        start_date = data_file.audit_start_date if data_file.audit_start_date else datetime(data_file.fiscal_year, 1, 1).date()
        end_date = data_file.audit_end_date if data_file.audit_end_date else datetime(data_file.fiscal_year, 12, 31).date()
        
        # Get Saudi Arabian holidays for the date range
        try:
            holidays = get_holidays('saudiarabian', start_date, end_date, include_observances=True)
            holiday_dates = {h.date for h in holidays}
            holiday_info = {h.date: {'name': h.name, 'type': h.holiday_type} for h in holidays}
            logger.info(f"Retrieved {len(holidays)} Saudi Arabian holidays from {start_date} to {end_date}")
        except Exception as e:
            logger.error(f"Error retrieving holidays: {e}")
            holidays = []
            holiday_dates = set()
            holiday_info = {}
        
        holiday_transactions = []
        holiday_by_user = {}
        holiday_by_account = {}
        
        for t in transactions:
            if t.posting_date:
                # Check if posting is on a Saudi Arabian holiday using pre-fetched holiday data
                posting_date = t.posting_date.date() if hasattr(t.posting_date, 'date') else t.posting_date
                
                # Check if the posting date is in our pre-fetched holiday dates
                if posting_date in holiday_dates:
                    holiday_name = holiday_info.get(posting_date, {}).get('name', 'Saudi Holiday')
                    
                    holiday_transactions.append({
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'amount': float(t.amount_local_currency),
                        'posting_date': t.posting_date.isoformat(),
                        'holiday_name': holiday_name,
                        'user': t.user_name,
                        'account': t.gl_account,
                        'risk_level': 'HIGH'
                    })
                    
                    # Group by user
                    if t.user_name not in holiday_by_user:
                        holiday_by_user[t.user_name] = []
                    holiday_by_user[t.user_name].append(t)
                    
                    # Group by account
                    if t.gl_account not in holiday_by_account:
                        holiday_by_account[t.gl_account] = []
                    holiday_by_account[t.gl_account].append(t)
        
        # Merge ML and rule-based results
        holiday_transactions.extend(ml_detected_holidays)
        
        holiday_results = {
            'holiday_transactions': holiday_transactions,
            'holiday_by_user': {user: len(transactions) for user, transactions in holiday_by_user.items()},
            'holiday_by_account': {account: len(transactions) for account, transactions in holiday_by_account.items()},
            'holiday_breakdown': _generate_holiday_breakdown(holiday_transactions),
            'audit_recommendations': [
                'Review holiday transactions for proper authorization',
                'Verify if holiday postings are allowed by company policy',
                'Check for potential fraud or manipulation',
                'Ensure proper documentation for holiday transactions'
            ],
            'compliance_assessment': {
                'holiday_percentage': (len(holiday_transactions) / len(transactions) * 100) if transactions else 0.0,
                'risk_level': 'HIGH' if len(holiday_transactions) > 0 else 'LOW',
                'total_holidays_detected': len(set(t.get('holiday_name') for t in holiday_transactions))
            },
            'financial_statement_impact': {
                'potential_misstatement': len(holiday_transactions),
                'impact_level': 'HIGH' if len(holiday_transactions) > 0 else 'LOW',
                'total_holiday_amount': sum(float(t.get('amount', 0)) for t in holiday_transactions)
            },
            'chart_data': {
                'holiday_summary': {
                    'total_holiday_transactions': len(holiday_transactions),
                    'total_transactions': len(transactions),
                    'holiday_percentage': (len(holiday_transactions) / len(transactions) * 100) if transactions else 0.0
                },
                'holiday_by_month': _generate_holiday_by_month_chart(holiday_transactions),
                'holiday_by_user': _generate_holiday_by_user_chart(holiday_by_user),
                'holiday_by_account': _generate_holiday_by_account_chart(holiday_by_account),
                'holiday_amount_distribution': _generate_holiday_amount_chart(holiday_transactions),
                'holiday_breakdown_chart': _generate_holiday_breakdown_chart(holiday_transactions),
                'holiday_risk_distribution': {
                    'high_risk': len([t for t in holiday_transactions if t.get('risk_level') == 'HIGH']),
                    'medium_risk': len([t for t in holiday_transactions if t.get('risk_level') == 'MEDIUM']),
                    'low_risk': len([t for t in holiday_transactions if t.get('risk_level') == 'LOW'])
                }
            },
            'export_data': holiday_transactions,
            'processing_duration': (timezone.now() - start_time).total_seconds(),
            # =============================================================================
            # ML ENHANCED FEATURES
            # =============================================================================
            'ml_insights': {
                'detection_method': ml_detection_method,
                'ml_predictions': ml_predictions,
                'ml_detected_holidays': len(ml_detected_holidays),
                'rule_based_holidays': len(holiday_transactions) - len(ml_detected_holidays),
                'ml_model_accuracy': ml_predictions.get('model_accuracy', 0.0) if ml_predictions else 0.0
            },
            'detection_methods': {
                'primary_method': ml_detection_method,
                'ml_available': bool(ml_predictions),
                'rule_based_fallback': ml_detection_method in ['rule_based', 'rule_based_fallback']
            }
        }
        
        # Calculate overall risk score based on holiday transactions
        total_amount = sum(float(t.get('amount', 0)) for t in holiday_transactions)
        holiday_percentage = (len(holiday_transactions) / len(transactions) * 100) if transactions else 0.0
        
        # Risk score calculation: higher score for more holiday transactions and higher amounts
        risk_score = min(100.0, (len(holiday_transactions) * 10) + (holiday_percentage * 2) + (total_amount / 1000000))
        
        # Save to HolidayAnalysisResult table using new unified structure
        holiday_analysis_result = HolidayAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='holiday_analysis',
            analysis_version='2.0.0',
            analysis_summary={
                'total_transactions': len(transactions),
                'holiday_transactions_count': len(holiday_transactions),
                'holiday_breakdown': _generate_holiday_breakdown(holiday_transactions),
                'total_holiday_amount': total_amount,
                'holidays_detected': list(set(t.get('holiday_name') for t in holiday_transactions)),
                'overall_risk_score': risk_score,
                'holiday_percentage': holiday_percentage
            },
            anomaly_list=holiday_transactions,
            holiday_postings=holiday_transactions,
            holiday_by_fs_line=holiday_results.get('holiday_by_fs_line', []),
            holiday_by_account=holiday_results.get('holiday_by_account', []),
            holiday_by_user=holiday_results.get('holiday_by_user', []),
            holiday_by_holiday_type=holiday_results.get('holiday_by_holiday_type', []),
            gl_activity_by_holiday=holiday_results.get('gl_activity_by_holiday', []),
            holiday_breakdown=holiday_results.get('holiday_breakdown', []),
            holiday_patterns=holiday_results.get('holiday_patterns', {}),
            financial_statement_impact=holiday_results.get('financial_statement_impact', {}),
            chart_data=holiday_results.get('chart_data', {}),
            risk_assessment=holiday_results.get('risk_assessment', {}),
            audit_recommendations=holiday_results.get('audit_recommendations', []),
            compliance_assessment=holiday_results.get('compliance_assessment', {}),
            breakdowns={
                'ml_insights': holiday_results.get('ml_insights', {}),
                'detection_methods': holiday_results.get('detection_methods', {})
            },
            export_data=holiday_results.get('export_data', []),
            processing_duration=holiday_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"Holiday Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(holiday_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'holiday_transactions_found': len(holiday_transactions),
            'table': 'HolidayAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Holiday Analysis: {str(e)}"
        logger.error(error_msg)
        
        # Save failed result to database using new unified structure
        try:
            HolidayAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='holiday_analysis',
                analysis_version='2.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg} 

def _generate_holiday_by_month_chart(holiday_transactions):
    """Generate holiday transactions by month chart data"""
    from collections import defaultdict
    import calendar
    
    monthly_data = defaultdict(int)
    for transaction in holiday_transactions:
        posting_date = transaction.get('posting_date', '')
        if posting_date:
            try:
                month = int(posting_date.split('-')[1])
                month_name = calendar.month_name[month]
                monthly_data[month_name] += 1
            except:
                continue
    
    return {
        'labels': list(monthly_data.keys()),
        'data': list(monthly_data.values()),
        'title': 'Holiday Transactions by Month'
    }

def _generate_holiday_by_user_chart(holiday_by_user):
    """Generate holiday transactions by user chart data"""
    # Get top 10 users by holiday transaction count
    sorted_users = sorted(holiday_by_user.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    
    return {
        'labels': [user for user, _ in sorted_users],
        'data': [len(transactions) for _, transactions in sorted_users],
        'title': 'Top Users by Holiday Transactions'
    }

def _generate_holiday_by_account_chart(holiday_by_account):
    """Generate holiday transactions by account chart data"""
    # Get top 10 accounts by holiday transaction count
    sorted_accounts = sorted(holiday_by_account.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    
    return {
        'labels': [account for account, _ in sorted_accounts],
        'data': [len(transactions) for _, transactions in sorted_accounts],
        'title': 'Top Accounts by Holiday Transactions'
    }

def _generate_holiday_amount_chart(holiday_transactions):
    """Generate holiday transaction amount distribution chart data"""
    amounts = [float(t.get('amount', 0)) for t in holiday_transactions]
    
    if not amounts:
        return {'labels': [], 'data': [], 'title': 'Holiday Transaction Amounts'}
    
    # Create amount ranges
    min_amount = min(amounts)
    max_amount = max(amounts)
    
    if min_amount == max_amount:
        ranges = [f"${min_amount:,.0f}"]
        counts = [len(amounts)]
    else:
        # Create 5 ranges
        step = (max_amount - min_amount) / 5
        ranges = []
        counts = []
        
        for i in range(5):
            start = min_amount + (i * step)
            end = min_amount + ((i + 1) * step)
            range_label = f"${start:,.0f} - ${end:,.0f}"
            ranges.append(range_label)
            
            count = len([a for a in amounts if start <= a < end])
            counts.append(count)
    
    return {
        'labels': ranges,
        'data': counts,
        'title': 'Holiday Transaction Amount Distribution'
    } 

def _generate_weekend_by_month_chart(unusual_days_transactions):
    """Generate unusual days transactions by month chart data"""
    from collections import defaultdict
    import calendar
    
    monthly_data = defaultdict(int)
    for transaction in unusual_days_transactions:
        posting_date = transaction.get('posting_date', '')
        if posting_date:
            try:
                month = int(posting_date.split('-')[1])
                month_name = calendar.month_name[month]
                monthly_data[month_name] += 1
            except:
                continue
    
    return {
        'labels': list(monthly_data.keys()),
        'data': list(monthly_data.values()),
        'title': 'Unusual Days Transactions by Month'
    }

def _generate_weekend_by_user_chart(unusual_days_by_user):
    """Generate unusual days transactions by user chart data"""
    # Get top 10 users by unusual day transaction count
    sorted_users = sorted(unusual_days_by_user.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    
    return {
        'labels': [user for user, _ in sorted_users],
        'data': [len(transactions) for _, transactions in sorted_users],
        'title': 'Top Users by Unusual Days Transactions'
    }

def _generate_weekend_by_account_chart(unusual_days_by_account):
    """Generate unusual days transactions by account chart data"""
    # Get top 10 accounts by unusual day transaction count
    sorted_accounts = sorted(unusual_days_by_account.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    
    return {
        'labels': [account for account, _ in sorted_accounts],
        'data': [len(transactions) for _, transactions in sorted_accounts],
        'title': 'Top Accounts by Unusual Days Transactions'
    }

def _generate_weekend_amount_chart(unusual_days_transactions):
    """Generate unusual day transaction amount distribution chart data"""
    amounts = [float(t.get('amount', 0)) for t in unusual_days_transactions]
    
    if not amounts:
        return {'labels': [], 'data': [], 'title': 'Unusual Day Transaction Amounts'}
    
    # Create amount ranges
    min_amount = min(amounts)
    max_amount = max(amounts)
    
    if min_amount == max_amount:
        ranges = [f"${min_amount:,.0f}"]
        counts = [len(amounts)]
    else:
        # Create 5 ranges
        step = (max_amount - min_amount) / 5
        ranges = []
        counts = []
        
        for i in range(5):
            start = min_amount + (i * step)
            end = min_amount + ((i + 1) * step)
            range_label = f"${start:,.0f} - ${end:,.0f}"
            ranges.append(range_label)
            
            count = len([a for a in amounts if start <= a < end])
            counts.append(count)
    
    return {
        'labels': ranges,
        'data': counts,
        'title': 'Unusual Day Transaction Amount Distribution'
    } 

def _generate_holiday_breakdown(holiday_transactions):
    """Generate a breakdown of detected holidays."""
    from collections import defaultdict
    
    holiday_breakdown = defaultdict(int)
    for transaction in holiday_transactions:
        holiday_name = transaction.get('holiday_name')
        if holiday_name:
            holiday_breakdown[holiday_name] += 1
    
    return list(holiday_breakdown.items())

def _generate_holiday_breakdown_chart(holiday_transactions):
    """Generate a chart data for holiday breakdown."""
    from collections import defaultdict
    
    holiday_breakdown = defaultdict(int)
    for transaction in holiday_transactions:
        holiday_name = transaction.get('holiday_name')
        if holiday_name:
            holiday_breakdown[holiday_name] += 1
    
    return {
        'labels': list(holiday_breakdown.keys()),
        'data': list(holiday_breakdown.values()),
        'title': 'Holiday Breakdown'
    } 