"""
Utility functions for tasks
"""

import logging
import traceback
import psutil
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List

# Import notification tasks
try:
    from notifications.tasks import (
        notify_file_processing_status, 
        notify_analysis_status,
        notify_system_status
    )
    NOTIFICATIONS_AVAILABLE = True
except ImportError:
    NOTIFICATIONS_AVAILABLE = False

logger = logging.getLogger(__name__)


def send_notification_if_available(notification_func, *args, **kwargs):
    """
    Send notification if notifications app is available
    
    Args:
        notification_func: The notification function to call
        *args, **kwargs: Arguments to pass to the notification function
    """
    if NOTIFICATIONS_AVAILABLE:
        try:
            notification_func.delay(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Failed to send notification: {e}")


def get_user_from_job(job):
    """
    Extract user from job for notifications
    
    Args:
        job: FileProcessingJob instance
        
    Returns:
        User ID or None
    """
    try:
        if hasattr(job, 'data_file') and job.data_file and hasattr(job.data_file, 'engagement'):
            engagement = job.data_file.engagement
            if hasattr(engagement, 'created_by') and engagement.created_by:
                return engagement.created_by.id
        return None
    except Exception as e:
        logger.warning(f"Could not extract user from job: {e}")
        return None


def _serialize_transaction(transaction):
    """Serialize transaction for JSON storage"""
    return {
        'id': str(transaction.id),
        'document_number': transaction.document_number,
        'document_type': transaction.document_type,
        'amount_local_currency': float(transaction.amount_local_currency) if transaction.amount_local_currency else 0,
        'local_currency': transaction.local_currency,
        'gl_account': transaction.gl_account,
        'profit_center': transaction.profit_center,
        'user_name': transaction.user_name,
        'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
        'document_date': transaction.document_date.isoformat() if transaction.document_date else None,
        'entry_date': transaction.entry_date.isoformat() if transaction.entry_date else None,
        'fiscal_year': transaction.fiscal_year,
        'posting_period': transaction.posting_period,
        'text': transaction.text,
        'segment': transaction.segment,
        'clearing_document': transaction.clearing_document,
        'offsetting_account': transaction.offsetting_account,
        'invoice_reference': transaction.invoice_reference,
        'sales_document': transaction.sales_document,
        'assignment': transaction.assignment,
        'year_month': transaction.year_month,
        'amount_transaction_currency': float(transaction.amount_transaction_currency) if transaction.amount_transaction_currency else None,
        'transaction_currency': transaction.transaction_currency,
        'exchange_rate': float(transaction.exchange_rate) if transaction.exchange_rate else None,
        'posting_key': transaction.posting_key,
        'reference_document': transaction.reference_document,
        'document_header_text': transaction.document_header_text,
        'company_code': transaction.company_code,
        'fiscal_period': transaction.fiscal_period,
        'cost_center': transaction.cost_center,
        'wbs_element': transaction.wbs_element,
        'order_number': transaction.order_number,
        'asset_number': transaction.asset_number,
        'sub_number': transaction.sub_number,
        'business_area': transaction.business_area,
        'partner_business_area': transaction.partner_business_area
    }


def _calculate_holiday_risk_score(transaction, holiday_data):
    """Calculate risk score for holiday transactions"""
    base_score = 30
    
    # Higher risk for higher amounts
    amount = abs(transaction.amount_local_currency or 0)
    if amount > 100000:
        base_score += 40
    elif amount > 50000:
        base_score += 30
    elif amount > 10000:
        base_score += 20
    elif amount > 1000:
        base_score += 10
    
    # Higher risk for certain account types
    gl_account = str(transaction.gl_account or '')
    if gl_account.startswith('1'):  # Asset accounts
        base_score += 15
    elif gl_account.startswith('6'):  # Expense accounts
        base_score += 10
    
    # Higher risk for certain document types
    doc_type = str(transaction.document_type or '')
    if doc_type in ['AB', 'DR', 'CR']:  # Adjustment entries
        base_score += 20
    elif doc_type in ['SA', 'RE']:  # Sales/Revenue entries
        base_score += 15
    
    # Higher risk for certain users
    user_name = str(transaction.user_name or '').lower()
    if 'admin' in user_name or 'manager' in user_name:
        base_score += 15
    elif 'temp' in user_name or 'test' in user_name:
        base_score += 10
    
    return min(base_score, 100)


def _get_holiday_risk_level(transaction, holiday_data):
    """Get risk level for holiday transaction"""
    risk_score = _calculate_holiday_risk_score(transaction, holiday_data)
    
    if risk_score >= 80:
        return 'critical'
    elif risk_score >= 60:
        return 'high'
    elif risk_score >= 40:
        return 'medium'
    else:
        return 'low'


def _generate_holiday_risk_assessment(holiday_postings, total_transactions):
    """Generate risk assessment for holiday postings"""
    if not holiday_postings:
        return {
            'overall_risk_level': 'low',
            'risk_score': 0,
            'total_holiday_transactions': 0,
            'holiday_transaction_rate': 0,
            'risk_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        }
    
    # Calculate risk distribution
    risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
    risk_scores = []
    
    for posting in holiday_postings:
        risk_level = posting.get('risk_level', 'low')
        risk_distribution[risk_level] += 1
        risk_scores.append(posting.get('risk_score', 0))
    
    # Calculate overall metrics
    overall_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
    holiday_transaction_rate = (len(holiday_postings) / total_transactions * 100) if total_transactions > 0 else 0
    
    # Determine overall risk level
    if overall_risk_score >= 80:
        overall_risk_level = 'critical'
    elif overall_risk_score >= 60:
        overall_risk_level = 'high'
    elif overall_risk_score >= 40:
        overall_risk_level = 'medium'
    else:
        overall_risk_level = 'low'
    
    return {
        'overall_risk_level': overall_risk_level,
        'risk_score': overall_risk_score,
        'total_holiday_transactions': len(holiday_postings),
        'holiday_transaction_rate': holiday_transaction_rate,
        'risk_distribution': risk_distribution,
        'average_amount': sum(abs(p.get('amount_local_currency', 0)) for p in holiday_postings) / len(holiday_postings) if holiday_postings else 0
    }


def _generate_holiday_audit_recommendations(holiday_postings, holiday_by_user, holiday_by_account):
    """Generate audit recommendations for holiday postings"""
    recommendations = []
    
    # High-value holiday transactions
    high_value_postings = [p for p in holiday_postings if abs(p.get('amount_local_currency', 0)) > 50000]
    if high_value_postings:
        recommendations.append({
            'priority': 'high',
            'type': 'high_value_holiday_transactions',
            'description': f'Review {len(high_value_postings)} high-value transactions posted on holidays',
            'affected_transactions': len(high_value_postings),
            'total_amount': sum(abs(p.get('amount_local_currency', 0)) for p in high_value_postings)
        })
    
    # Users with frequent holiday postings
    frequent_users = [(user, count) for user, count in holiday_by_user.items() if count > 5]
    if frequent_users:
        recommendations.append({
            'priority': 'medium',
            'type': 'frequent_holiday_user',
            'description': f'Review users with frequent holiday postings: {len(frequent_users)} users',
            'users': [{'user': user, 'count': count} for user, count in frequent_users]
        })
    
    # Accounts with holiday postings
    if holiday_by_account:
        recommendations.append({
            'priority': 'medium',
            'type': 'holiday_account_activity',
            'description': f'Review {len(holiday_by_account)} accounts with holiday activity',
            'accounts': [{'account': account, 'count': count} for account, count in holiday_by_account.items()]
        })
    
    # Critical risk transactions
    critical_postings = [p for p in holiday_postings if p.get('risk_level') == 'critical']
    if critical_postings:
        recommendations.append({
            'priority': 'immediate',
            'type': 'critical_holiday_risk',
            'description': f'Immediate review required for {len(critical_postings)} critical risk holiday transactions',
            'affected_transactions': len(critical_postings)
        })
    
    return {
        'immediate_actions': [r for r in recommendations if r['priority'] == 'immediate'],
        'high_priority_actions': [r for r in recommendations if r['priority'] == 'high'],
        'medium_priority_actions': [r for r in recommendations if r['priority'] == 'medium'],
        'low_priority_actions': [r for r in recommendations if r['priority'] == 'low']
    }


def _generate_holiday_breakdown(holiday_transactions):
    """Generate breakdown of holiday transactions"""
    if not holiday_transactions:
        return {}
    
    # Group by user
    by_user = {}
    for transaction in holiday_transactions:
        user = transaction.get('user_name', 'Unknown')
        by_user[user] = by_user.get(user, 0) + 1
    
    # Group by account
    by_account = {}
    for transaction in holiday_transactions:
        account = transaction.get('gl_account', 'Unknown')
        by_account[account] = by_account.get(account, 0) + 1
    
    # Group by document type
    by_document_type = {}
    for transaction in holiday_transactions:
        doc_type = transaction.get('document_type', 'Unknown')
        by_document_type[doc_type] = by_document_type.get(doc_type, 0) + 1
    
    # Group by risk level
    by_risk_level = {}
    for transaction in holiday_transactions:
        risk_level = transaction.get('risk_level', 'low')
        by_risk_level[risk_level] = by_risk_level.get(risk_level, 0) + 1
    
    return {
        'by_user': by_user,
        'by_account': by_account,
        'by_document_type': by_document_type,
        'by_risk_level': by_risk_level,
        'total_transactions': len(holiday_transactions)
    }


def log_task_info(task_name, job_id, message, level="info"):
    """Log task information with consistent format"""
    log_message = f"[{task_name}] Job {job_id}: {message}"
    
    if level == "error":
        logger.error(log_message)
    elif level == "warning":
        logger.warning(log_message)
    elif level == "debug":
        logger.debug(log_message)
    else:
        logger.info(log_message)


def debug_task_state(task_name, job_id, state, details=None):
    """Debug task state with detailed information"""
    message = f"Task state: {state}"
    if details:
        message += f" - Details: {details}"
    
    log_task_info(task_name, job_id, message, "debug")


def debug_task_data(task_name, job_id, data_type, data, max_items=5):
    """Debug task data with size limits"""
    if isinstance(data, (list, tuple)):
        data_size = len(data)
        data_preview = data[:max_items] if data_size > max_items else data
        message = f"Data type: {data_type}, Size: {data_size}, Preview: {data_preview}"
    elif isinstance(data, dict):
        data_size = len(data)
        data_preview = dict(list(data.items())[:max_items])
        message = f"Data type: {data_type}, Size: {data_size}, Preview: {data_preview}"
    else:
        message = f"Data type: {data_type}, Value: {data}"
    
    log_task_info(task_name, job_id, message, "debug")


def debug_task_exception(task_name, job_id, exception, context=""):
    """Debug task exception with context"""
    message = f"Exception: {str(exception)}"
    if context:
        message += f" - Context: {context}"
    
    log_task_info(task_name, job_id, message, "error")
    logger.error(traceback.format_exc())


def get_system_info():
    """Get current system information"""
    try:
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'available_memory_gb': psutil.virtual_memory().available / (1024**3),
            'process_count': len(psutil.pids()),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.warning(f"Could not get system info: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
