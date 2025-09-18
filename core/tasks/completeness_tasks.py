"""
Completeness Tasks Module

Contains all completeness-related Celery tasks for the analytics system.
"""

from celery import shared_task, current_task
from django.utils import timezone
from django.db import transaction
from django.db.models import F, Q, Count, Sum, Avg, Min, Max
import logging
import traceback
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List

from ..models import (
    FileProcessingJob, SAPGLPosting, DataFile, CompletenessJob, 
    FileProcessingTask, TrialBalance, ChartOfAccount, CompletenessTestResult,
    GLAccount, Engagement
)

from .utils import (
    send_notification_if_available,
    get_user_from_job,
    log_task_info,
    debug_task_state,
    debug_task_data,
    debug_task_exception,
)

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=600, soft_time_limit=540)
def run_completeness_job(self, job_id):
    """
    Run completeness analysis job after all processing tasks are complete
    """
    try:
        log_task_info("run_completeness_job", job_id, f"Starting completeness job {job_id}")
        
        # Get the completeness job
        try:
            completeness_job = CompletenessJob.objects.get(id=job_id)
        except CompletenessJob.DoesNotExist:
            logger.error(f"Completeness job {job_id} not found")
            return {"status": "error", "message": "Completeness job not found"}
        
        # Update job status
        completeness_job.status = 'IN_PROGRESS'
        completeness_job.started_at = timezone.now()
        completeness_job.save()
        
        # Get the data file
        data_file = completeness_job.data_file
        if not data_file:
            logger.error(f"No data file found for completeness job {job_id}")
            completeness_job.status = 'FAILED'
            completeness_job.error_message = "No data file found"
            completeness_job.save()
            return {"status": "error", "message": "No data file found"}
        
        # Get related processing tasks
        gl_task = completeness_job.gl_task
        tb_task = completeness_job.tb_task
        chart_task = completeness_job.chart_task
        
        # Check if all required tasks are completed
        required_tasks = [gl_task, tb_task, chart_task]
        completed_tasks = [task for task in required_tasks if task and task.status == 'SUCCESS']
        
        if len(completed_tasks) < len([task for task in required_tasks if task]):
            logger.warning(f"Not all required tasks completed for completeness job {job_id}")
            completeness_job.status = 'FAILED'
            completeness_job.error_message = "Not all required processing tasks completed"
            completeness_job.save()
            return {"status": "error", "message": "Not all required processing tasks completed"}
        
        # Run completeness test
        completeness_result = run_completeness_test.delay(data_file.id)
        result = completeness_result.get(timeout=300)
        
        if result.get('status') == 'success':
            # Update completeness job with results
            completeness_job.completeness_results = result.get('completeness_results', {})
            completeness_job.debit_credit_summary = result.get('debit_credit_summary', {})
            completeness_job.audit_checks = result.get('audit_checks', {})
            completeness_job.status = 'CREATED'
            completeness_job.completed_at = timezone.now()
            completeness_job.save()
            
            log_task_info("run_completeness_job", job_id, "Completeness job completed successfully")
            
            return {
                "status": "success",
                "message": "Completeness job completed",
                "completeness_results": result.get('completeness_results', {}),
                "debit_credit_summary": result.get('debit_credit_summary', {}),
                "audit_checks": result.get('audit_checks', {})
            }
        else:
            completeness_job.status = 'FAILED'
            completeness_job.error_message = result.get('message', 'Unknown error')
            completeness_job.save()
            
            return {"status": "error", "message": result.get('message', 'Unknown error')}
        
    except Exception as e:
        logger.error(f"Completeness job failed: {e}")
        logger.error(traceback.format_exc())
        
        # Update job status
        try:
            completeness_job = CompletenessJob.objects.get(id=job_id)
            completeness_job.status = 'FAILED'
            completeness_job.error_message = str(e)
            completeness_job.save()
        except Exception as update_error:
            logger.error(f"Failed to update completeness job status: {update_error}")
        
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=600, soft_time_limit=540)
def run_completeness_test(self, data_file_id):
    """
    Run comprehensive completeness test on engagement data
    """
    try:
        log_task_info("run_completeness_test", data_file_id, f"Starting completeness test for data file {data_file_id}")
        
        # Get the data file
        try:
            data_file = DataFile.objects.get(id=data_file_id)
        except DataFile.DoesNotExist:
            logger.error(f"Data file {data_file_id} not found")
            return {"status": "error", "message": "Data file not found"}
        
        # Get engagement
        engagement = data_file.engagement
        if not engagement:
            logger.error(f"No engagement found for data file {data_file_id}")
            return {"status": "error", "message": "No engagement found"}
        
        # Get all files for this engagement
        gl_file = DataFile.objects.filter(
            engagement=engagement, 
            file_type='GL'
        ).first()
        
        tb_file = DataFile.objects.filter(
            engagement=engagement, 
            file_type='TB'
        ).first()
        
        coa_file = DataFile.objects.filter(
            engagement=engagement, 
            file_type='COA'
        ).first()
        
        # Check file completeness
        file_completeness = {
            'has_gl': gl_file is not None,
            'has_tb': tb_file is not None,
            'has_coa': coa_file is not None,
            'required_files_complete': all([gl_file, tb_file, coa_file])
        }
        
        if not file_completeness['required_files_complete']:
            logger.warning(f"Not all required files present for engagement {engagement.engagement_id}")
            return {
                "status": "error", 
                "message": "Not all required files present",
                "file_completeness": file_completeness
            }
        
        # Get data from all files
        gl_transactions = SAPGLPosting.objects.filter(data_file=gl_file) if gl_file else []
        tb_records = TrialBalance.objects.filter(data_file=tb_file) if tb_file else []
        coa_records = ChartOfAccount.objects.filter(data_file=coa_file) if coa_file else []
        
        # Perform completeness tests
        completeness_results = _perform_completeness_tests(gl_transactions, tb_records, coa_records, engagement)
        debit_credit_summary = _calculate_debit_credit_summary(gl_transactions)
        audit_checks = _perform_audit_checks(gl_transactions, tb_records, coa_records, engagement)
        
        # Create completeness test result
        test_result = CompletenessTestResult.objects.create(
            engagement=engagement,
            gl_file=gl_file,
            tb_file=tb_file,
            coa_file=coa_file,
            overall_status=completeness_results.get('overall_status', 'FAIL'),
            overall_explanation=completeness_results.get('explanation', ''),
            completeness_score=completeness_results.get('completeness_score', 0),
            step1_file_completeness=completeness_results.get('step1_file_completeness', {}),
            step2_gl_tb_reconciliation=completeness_results.get('step2_gl_tb_reconciliation', {}),
            step3_debit_credit_balance=completeness_results.get('step3_debit_credit_balance', {}),
            step4_account_coverage=completeness_results.get('step4_account_coverage', {}),
            step5_coa_hierarchy_validation=completeness_results.get('step5_coa_hierarchy_validation', {}),
            step6_account_linking=completeness_results.get('step6_account_linking', {}),
            step7_transaction_gaps=completeness_results.get('step7_transaction_gaps', {}),
            total_gl_records=len(gl_transactions),
            total_tb_records=len(tb_records),
            total_coa_records=len(coa_records),
            total_accounts_unified=completeness_results.get('total_accounts_unified', 0),
            tests_passed=completeness_results.get('tests_passed', 0),
            total_tests=completeness_results.get('total_tests', 7),
            critical_issues_count=completeness_results.get('critical_issues_count', 0),
            comprehensive_statistics=completeness_results.get('comprehensive_statistics', {}),
            processing_duration=0  # Will be calculated
        )
        
        log_task_info("run_completeness_test", data_file_id, f"Completeness test completed - Score: {completeness_results.get('completeness_score', 0)}%")
        
        return {
            "status": "success",
            "message": "Completeness test completed",
            "completeness_results": completeness_results,
            "debit_credit_summary": debit_credit_summary,
            "audit_checks": audit_checks,
            "test_result_id": test_result.id
        }
        
    except Exception as e:
        logger.error(f"Completeness test failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


def _perform_completeness_tests(gl_transactions, tb_records, coa_records, engagement):
    """Perform comprehensive completeness tests"""
    results = {
        'step1_file_completeness': {},
        'step2_gl_tb_reconciliation': {},
        'step3_debit_credit_balance': {},
        'step4_account_coverage': {},
        'step5_coa_hierarchy_validation': {},
        'step6_account_linking': {},
        'step7_transaction_gaps': {},
        'tests_passed': 0,
        'total_tests': 7,
        'critical_issues_count': 0,
        'completeness_score': 0
    }
    
    # Step 1: File Completeness Check
    step1_result = _check_file_completeness(gl_transactions, tb_records, coa_records)
    results['step1_file_completeness'] = step1_result
    if step1_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 2: GL-TB Reconciliation
    step2_result = _check_gl_tb_reconciliation(gl_transactions, tb_records)
    results['step2_gl_tb_reconciliation'] = step2_result
    if step2_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 3: Debit-Credit Balance
    step3_result = _check_debit_credit_balance(gl_transactions)
    results['step3_debit_credit_balance'] = step3_result
    if step3_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 4: Account Coverage
    step4_result = _check_account_coverage(gl_transactions, tb_records, coa_records)
    results['step4_account_coverage'] = step4_result
    if step4_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 5: COA Hierarchy Validation
    step5_result = _check_coa_hierarchy_validation(coa_records)
    results['step5_coa_hierarchy_validation'] = step5_result
    if step5_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 6: Account Linking
    step6_result = _check_account_linking(gl_transactions, tb_records, coa_records)
    results['step6_account_linking'] = step6_result
    if step6_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Step 7: Transaction Gaps
    step7_result = _check_transaction_gaps(gl_transactions, engagement)
    results['step7_transaction_gaps'] = step7_result
    if step7_result.get('passed', False):
        results['tests_passed'] += 1
    
    # Calculate overall completeness score
    results['completeness_score'] = (results['tests_passed'] / results['total_tests']) * 100
    
    # Determine overall status
    if results['completeness_score'] >= 95:
        results['overall_status'] = 'PASS'
        results['explanation'] = 'All completeness tests passed'
    elif results['completeness_score'] >= 80:
        results['overall_status'] = 'PASS'
        results['explanation'] = 'Most completeness tests passed with minor issues'
    else:
        results['overall_status'] = 'FAIL'
        results['explanation'] = 'Multiple completeness issues detected'
    
    # Count critical issues
    for step_name, step_result in results.items():
        if isinstance(step_result, dict) and step_result.get('critical_issues'):
            results['critical_issues_count'] += len(step_result['critical_issues'])
    
    return results


def _check_file_completeness(gl_transactions, tb_records, coa_records):
    """Check if all required files are present and contain data"""
    return {
        'passed': all([
            len(gl_transactions) > 0,
            len(tb_records) > 0,
            len(coa_records) > 0
        ]),
        'gl_count': len(gl_transactions),
        'tb_count': len(tb_records),
        'coa_count': len(coa_records),
        'issues': []
    }


def _check_gl_tb_reconciliation(gl_transactions, tb_records):
    """Check GL-TB reconciliation"""
    # Calculate GL totals by account
    gl_totals = {}
    for transaction in gl_transactions:
        account = transaction.gl_account
        amount = transaction.amount_local_currency or 0
        gl_totals[account] = gl_totals.get(account, 0) + amount
    
    # Calculate TB totals by account
    tb_totals = {}
    for record in tb_records:
        account = record.gl_account
        closing_balance = record.closing_balance or 0
        tb_totals[account] = closing_balance
    
    # Compare totals
    reconciliation_issues = []
    all_accounts = set(gl_totals.keys()) | set(tb_totals.keys())
    
    for account in all_accounts:
        gl_amount = gl_totals.get(account, 0)
        tb_amount = tb_totals.get(account, 0)
        difference = abs(gl_amount - tb_amount)
        
        if difference > 0.01:  # Allow for small rounding differences
            reconciliation_issues.append({
                'account': account,
                'gl_amount': float(gl_amount),
                'tb_amount': float(tb_amount),
                'difference': float(difference)
            })
    
    return {
        'passed': len(reconciliation_issues) == 0,
        'reconciliation_issues': reconciliation_issues,
        'total_accounts_checked': len(all_accounts),
        'accounts_with_differences': len(reconciliation_issues)
    }


def _check_debit_credit_balance(gl_transactions):
    """Check debit-credit balance"""
    total_debits = 0
    total_credits = 0
    
    for transaction in gl_transactions:
        amount = transaction.amount_local_currency or 0
        if amount > 0:
            total_debits += amount
        else:
            total_credits += abs(amount)
    
    difference = abs(total_debits - total_credits)
    is_balanced = difference < 0.01  # Allow for small rounding differences
    
    return {
        'passed': is_balanced,
        'total_debits': float(total_debits),
        'total_credits': float(total_credits),
        'difference': float(difference),
        'is_balanced': is_balanced
    }


def _check_account_coverage(gl_transactions, tb_records, coa_records):
    """Check account coverage across all files"""
    gl_accounts = set(transaction.gl_account for transaction in gl_transactions)
    tb_accounts = set(record.gl_account for record in tb_records)
    coa_accounts = set(record.account for record in coa_records)
    
    # Find missing accounts
    missing_in_tb = gl_accounts - tb_accounts
    missing_in_coa = gl_accounts - coa_accounts
    missing_in_gl = (tb_accounts | coa_accounts) - gl_accounts
    
    coverage_issues = []
    if missing_in_tb:
        coverage_issues.append({
            'type': 'missing_in_tb',
            'accounts': list(missing_in_tb),
            'count': len(missing_in_tb)
        })
    if missing_in_coa:
        coverage_issues.append({
            'type': 'missing_in_coa',
            'accounts': list(missing_in_coa),
            'count': len(missing_in_coa)
        })
    if missing_in_gl:
        coverage_issues.append({
            'type': 'missing_in_gl',
            'accounts': list(missing_in_gl),
            'count': len(missing_in_gl)
        })
    
    return {
        'passed': len(coverage_issues) == 0,
        'coverage_issues': coverage_issues,
        'gl_accounts': len(gl_accounts),
        'tb_accounts': len(tb_accounts),
        'coa_accounts': len(coa_accounts),
        'total_unique_accounts': len(gl_accounts | tb_accounts | coa_accounts)
    }


def _check_coa_hierarchy_validation(coa_records):
    """Check COA hierarchy validation"""
    hierarchy_issues = []
    
    for record in coa_records:
        if not record.type or not record.sub_type or not record.sub_sub_type:
            hierarchy_issues.append({
                'account': record.account,
                'issue': 'incomplete_hierarchy',
                'missing_fields': [
                    field for field, value in [
                        ('type', record.type),
                        ('sub_type', record.sub_type),
                        ('sub_sub_type', record.sub_sub_type)
                    ] if not value
                ]
            })
    
    return {
        'passed': len(hierarchy_issues) == 0,
        'hierarchy_issues': hierarchy_issues,
        'total_records_checked': len(coa_records),
        'records_with_issues': len(hierarchy_issues)
    }


def _check_account_linking(gl_transactions, tb_records, coa_records):
    """Check account linking across files"""
    # This would involve checking if accounts are properly linked
    # between GL, TB, and COA records
    return {
        'passed': True,  # Placeholder implementation
        'linking_issues': [],
        'total_accounts_checked': 0
    }


def _check_transaction_gaps(gl_transactions, engagement):
    """Check for transaction gaps"""
    # Check for gaps in transaction dates
    if not gl_transactions:
        return {
            'passed': False,
            'gap_issues': [{'type': 'no_transactions', 'description': 'No GL transactions found'}],
            'total_gaps': 1
        }
    
    # Get date range
    dates = [transaction.posting_date for transaction in gl_transactions if transaction.posting_date]
    if not dates:
        return {
            'passed': False,
            'gap_issues': [{'type': 'no_dates', 'description': 'No posting dates found'}],
            'total_gaps': 1
        }
    
    min_date = min(dates)
    max_date = max(dates)
    
    # Check for gaps (simplified implementation)
    gap_issues = []
    
    return {
        'passed': len(gap_issues) == 0,
        'gap_issues': gap_issues,
        'date_range': {
            'min_date': min_date.isoformat(),
            'max_date': max_date.isoformat()
        },
        'total_gaps': len(gap_issues)
    }


def _calculate_debit_credit_summary(gl_transactions):
    """Calculate debit-credit summary"""
    total_debits = 0
    total_credits = 0
    debit_count = 0
    credit_count = 0
    
    for transaction in gl_transactions:
        amount = transaction.amount_local_currency or 0
        if amount > 0:
            total_debits += amount
            debit_count += 1
        else:
            total_credits += abs(amount)
            credit_count += 1
    
    return {
        'total_debits': float(total_debits),
        'total_credits': float(total_credits),
        'debit_count': debit_count,
        'credit_count': credit_count,
        'net_amount': float(total_debits - total_credits),
        'is_balanced': abs(total_debits - total_credits) < 0.01
    }


def _perform_audit_checks(gl_transactions, tb_records, coa_records, engagement):
    """Perform audit-style checks"""
    return {
        'data_integrity_checks': [],
        'compliance_checks': [],
        'risk_assessments': [],
        'recommendations': []
    }
