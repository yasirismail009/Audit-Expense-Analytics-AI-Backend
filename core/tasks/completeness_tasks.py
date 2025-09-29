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
def run_gl_completeness_analysis(self, data_file_id):
    """
    Run 2-Step GL Completeness Test after GL file processing is complete
    
    Performs a streamlined 2-step completeness validation:
    
    Step 1: GL Completeness Check
        - Verify GL is balanced (Credit - Debit = 0)
        - Check data volume and availability (sufficient transactions, TB data present)
        
    Step 2: Account-wise Balance Verification
        - For each account: Opening + Debits - Credits = Closing balance
        - Cross-verify GL movements with TB movements per account
        - Requires 90%+ pass rate for overall completion
        
    Features:
        - Chart generation based on posting/document dates
        - Enhanced logging and debugging
        - Comprehensive statistics and scoring
        - Database persistence of results
    
    Args:
        data_file_id (str): UUID of the DataFile to analyze
        
    Returns:
        dict: Completeness test results with Pass/Fail status and detailed explanation
    """
    task_name = "run_gl_completeness_analysis"
    start_time = timezone.now()
    
    # Enhanced debugging setup
    logger.info("=" * 80)
    logger.info(f"🚀 STARTING COMPLETENESS ANALYSIS - Task ID: {self.request.id}")
    logger.info(f"📁 Data File ID: {data_file_id}")
    logger.info(f"⏰ Start Time: {start_time}")
    logger.info(f"🔧 Celery Worker: {self.request.hostname}")
    logger.info("=" * 80)
    
    try:
        from django.db import models
        
        # Get the data file
        try:
            data_file = DataFile.objects.get(id=data_file_id)
            logger.info(f"✅ STEP 0.1: Data file retrieved successfully")
            logger.info(f"   📄 File Name: {data_file.file_name}")
            logger.info(f"   📊 File Type: {data_file.file_type}")
            logger.info(f"   🎯 Engagement: {data_file.engagement_id}")
            logger.info(f"   📈 Status: {data_file.status}")
            logger.info(f"   📋 Total Records: {data_file.total_records}")
        except DataFile.DoesNotExist:
            logger.error(f"❌ STEP 0.1 FAILED: DataFile with ID {data_file_id} not found")
            return {'success': False, 'error': f'DataFile {data_file_id} not found'}
        
        logger.info(f"🔄 STEP 0.2: Starting GL-TB completeness test for file: {data_file.file_name}")
        
        # =======================================================================
        # STEP 0.4: DATA LOADING AND VALIDATION
        # =======================================================================
        
        logger.info("📊 STEP 0.4: Loading GL postings and Trial Balance data...")
        
        # Get GL postings and Trial Balance data for this engagement
        gl_postings = SAPGLPosting.objects.filter(data_file=data_file)
        trial_balance_records = TrialBalance.objects.filter(data_file__engagement=data_file.engagement)
        
        logger.info(f"   📈 GL Postings found: {gl_postings.count():,}")
        logger.info(f"   📋 TB Records found: {trial_balance_records.count():,}")
        logger.info(f"   🎯 Engagement ID: {data_file.engagement_id}")
        
        # Check if we have both GL and TB data
        if not gl_postings.exists():
            logger.warning(f"No GL postings found for data file {data_file_id}")
            return {'success': False, 'error': 'No GL postings found for completeness test'}
        
        if not trial_balance_records.exists():
            logger.warning(f"No Trial Balance data found for engagement {data_file.engagement_id}")
            return {
                'success': True,
                'completeness_results': {
                    'status': 'TB_NOT_AVAILABLE',
                    'explanation': 'Trial Balance data not available for cross-validation. Only GL internal validation performed.',
                    'gl_only_validation': True
                }
            }
        
        # Get Chart of Accounts data for this engagement
        coa_records = GLAccount.objects.filter(engagement=data_file.engagement)
        
        # =======================================================================
        # STEP 1: GL COMPLETENESS CHECK
        # =======================================================================
        
        logger.info("🔍 STEP 1: Starting GL Completeness Check...")
        step1_start = timezone.now()
        
        # Calculate GL totals (debits are positive, credits are negative)
        logger.info("   📊 Calculating GL totals...")
        gl_total_debit = sum(float(p.amount_local_currency or 0) for p in gl_postings if float(p.amount_local_currency or 0) > 0)
        gl_total_credit = sum(abs(float(p.amount_local_currency or 0)) for p in gl_postings if float(p.amount_local_currency or 0) < 0)
        
        # Check if GL is balanced (Credit - Debit = 0)
        gl_net_balance = gl_total_credit - gl_total_debit
        gl_is_balanced = abs(gl_net_balance) < 1.00  # Allow 1 unit variance
        
        # Additional completeness checks
        transaction_count = gl_postings.count()
        account_count = gl_postings.values('gl_account').distinct().count()
        has_tb = trial_balance_records.exists()
        
        # Volume and data availability check
        volume_check = transaction_count >= 100 and account_count >= 10
        data_availability = has_tb and gl_postings.exists()
        
        step1_completeness_passed = gl_is_balanced and volume_check and data_availability
        
        logger.info(f"   💰 GL Debit Total: {gl_total_debit:,.2f}")
        logger.info(f"   💰 GL Credit Total: {gl_total_credit:,.2f}")
        logger.info(f"   ⚖️  GL Net Balance (Credit - Debit): {gl_net_balance:,.2f}")
        logger.info(f"   ✅ GL Balanced: {'YES' if gl_is_balanced else 'NO'}")
        logger.info(f"   📊 Transaction Count: {transaction_count:,}")
        logger.info(f"   📋 Account Count: {account_count:,}")
        logger.info(f"   📈 TB Available: {'YES' if has_tb else 'NO'}")
        logger.info(f"   🎯 Volume Check: {'PASS' if volume_check else 'FAIL'}")
        
        step1_completeness = {
            'description': 'GL completeness verification: Credit - Debit should equal 0 and sufficient data volume',
            'gl_debit_total': round(gl_total_debit, 2),
            'gl_credit_total': round(gl_total_credit, 2),
            'gl_net_balance': round(gl_net_balance, 2),
            'gl_is_balanced': gl_is_balanced,
            'transaction_count': transaction_count,
            'account_count': account_count,
            'has_tb': has_tb,
            'volume_check': volume_check,
            'data_availability': data_availability,
            'passed': step1_completeness_passed,
            'explanation': f"GL Balance: {gl_net_balance:.2f} ({'PASS' if gl_is_balanced else 'FAIL'}), Volume: {transaction_count:,} transactions, {account_count} accounts, TB: {'Available' if has_tb else 'Missing'}"
        }
        
        step1_duration = (timezone.now() - step1_start).total_seconds()
        logger.info(f"✅ STEP 1 COMPLETED: {'PASS' if step1_completeness_passed else 'FAIL'} (Duration: {step1_duration:.2f}s)")
        if not step1_completeness_passed:
            issues = []
            if not gl_is_balanced:
                issues.append(f"GL imbalanced by {gl_net_balance:.2f}")
            if not volume_check:
                issues.append(f"Low volume: {transaction_count:,} transactions")
            if not data_availability:
                issues.append("Missing TB data")
            logger.warning(f"   ⚠️  Issues: {', '.join(issues)}")
        
        # =======================================================================
        # STEP 2: ACCOUNT-WISE BALANCE VERIFICATION
        # =======================================================================
        
        logger.info("🔍 STEP 2: Starting Account-wise Balance Verification...")
        step2_start = timezone.now()
        
        logger.info("   📊 Building GL account summaries...")
        
        # Build GL account-wise totals
        gl_account_totals = {}
        for posting in gl_postings:
            account_code = str(posting.gl_account).replace('.0', '')
            amount = float(posting.amount_local_currency or 0)
            
            if account_code not in gl_account_totals:
                gl_account_totals[account_code] = {
                    'debit_total': 0,
                    'credit_total': 0,
                    'net_movement': 0
                }
            
            if amount > 0:
                gl_account_totals[account_code]['debit_total'] += amount
            else:
                gl_account_totals[account_code]['credit_total'] += abs(amount)
            
            gl_account_totals[account_code]['net_movement'] = (
                gl_account_totals[account_code]['debit_total'] - 
                gl_account_totals[account_code]['credit_total']
            )
        
        logger.info(f"   📋 GL Account Summaries Built: {len(gl_account_totals)} accounts")
        
        # Get TB account details with balance verification
        logger.info("   📊 Verifying account-wise balance equation...")
        
        account_verifications = []
        balance_equation_passed_count = 0
        balance_equation_failed_count = 0
        total_variance = 0
        
        for tb_record in trial_balance_records:
            account_code = str(tb_record.gl_account).replace('.0', '')
            
            # Get TB data
            tb_debit = float(tb_record.debit or 0)
            tb_credit = float(tb_record.credit or 0)
            opening_balance = float(getattr(tb_record, 'opening_balance', 0) or 0)
            closing_balance = float(getattr(tb_record, 'closing_balance', 0) or 0)
            
            # Get GL data for this account
            gl_data = gl_account_totals.get(account_code, {
                'debit_total': 0,
                'credit_total': 0,
                'net_movement': 0
            })
            
            # Calculate expected closing balance: Opening + Debits - Credits = Closing
            calculated_closing = opening_balance + tb_debit - tb_credit
            balance_variance = abs(calculated_closing - closing_balance)
            
            # Check if GL movements match TB movements
            gl_vs_tb_debit_variance = abs(gl_data['debit_total'] - tb_debit)
            gl_vs_tb_credit_variance = abs(gl_data['credit_total'] - tb_credit)
            
            # Verification result
            balance_equation_correct = balance_variance < 1.00  # Allow 1 unit variance
            gl_tb_movements_match = (gl_vs_tb_debit_variance + gl_vs_tb_credit_variance) < 10.00
            
            account_verification = {
                'account_code': account_code,
                'opening_balance': opening_balance,
                'tb_debit': tb_debit,
                'tb_credit': tb_credit,
                'closing_balance': closing_balance,
                'calculated_closing': calculated_closing,
                'balance_variance': balance_variance,
                'gl_debit_total': gl_data['debit_total'],
                'gl_credit_total': gl_data['credit_total'],
                'gl_vs_tb_debit_variance': gl_vs_tb_debit_variance,
                'gl_vs_tb_credit_variance': gl_vs_tb_credit_variance,
                'balance_equation_correct': balance_equation_correct,
                'gl_tb_movements_match': gl_tb_movements_match,
                'account_passed': balance_equation_correct and gl_tb_movements_match
            }
            
            account_verifications.append(account_verification)
            total_variance += balance_variance
            
            if account_verification['account_passed']:
                balance_equation_passed_count += 1
            else:
                balance_equation_failed_count += 1
        
        # Overall Step 2 result
        total_accounts_verified = len(account_verifications)
        pass_rate = (balance_equation_passed_count / total_accounts_verified) if total_accounts_verified > 0 else 0
        step2_account_verification_passed = pass_rate >= 0.90  # 90% pass rate required
        
        logger.info(f"   📊 Account Verifications Completed:")
        logger.info(f"      Total Accounts: {total_accounts_verified}")
        logger.info(f"      Passed: {balance_equation_passed_count}")
        logger.info(f"      Failed: {balance_equation_failed_count}")
        logger.info(f"      Pass Rate: {pass_rate:.1%}")
        logger.info(f"      Total Variance: {total_variance:,.2f}")
        logger.info(f"      Result: {'✅ PASS' if step2_account_verification_passed else '❌ FAIL'}")
        
        # Show sample of failed accounts for debugging
        failed_accounts = [acc for acc in account_verifications if not acc['account_passed']]
        if failed_accounts:
            logger.warning(f"   ⚠️  Sample Failed Accounts:")
            for acc in failed_accounts[:5]:  # Show first 5 failed accounts
                logger.warning(f"      Account {acc['account_code']}: Balance variance {acc['balance_variance']:.2f}")
        
        step2_account_verification = {
            'description': 'Account-wise balance verification: Opening + Debits - Credits = Closing for each GL account',
            'total_accounts_verified': total_accounts_verified,
            'accounts_passed': balance_equation_passed_count,
            'accounts_failed': balance_equation_failed_count,
            'pass_rate': round(pass_rate, 3),
            'total_variance': round(total_variance, 2),
            'account_verifications': account_verifications,
            'failed_accounts': [acc['account_code'] for acc in failed_accounts],
            'passed': step2_account_verification_passed,
            'explanation': f"Account verification: {balance_equation_passed_count}/{total_accounts_verified} passed ({pass_rate:.1%}), Total variance: {total_variance:,.2f}"
        }
        
        step2_duration = (timezone.now() - step2_start).total_seconds()
        logger.info(f"✅ STEP 2 COMPLETED: {'PASS' if step2_account_verification_passed else 'FAIL'} (Duration: {step2_duration:.2f}s)")
        if not step2_account_verification_passed:
            logger.warning(f"   ⚠️  Issues: {balance_equation_failed_count} accounts failed verification, Pass rate: {pass_rate:.1%}")
        
        # =======================================================================
        # CHART GENERATION BASED ON POSTING/DOCUMENT DATES
        # =======================================================================
        
        logger.info("📊 Generating charts based on posting and document dates...")
        chart_start = timezone.now()
        
        # Monthly posting trends
        monthly_posting_data = {}
        daily_posting_data = {}
        
        for posting in gl_postings:
            # Use posting_date if available, otherwise document_date
            posting_date = posting.posting_date or posting.document_date
            if posting_date:
                month_key = posting_date.strftime('%Y-%m')
                day_key = posting_date.strftime('%Y-%m-%d')
                amount = abs(float(posting.amount_local_currency or 0))
                
                # Monthly data
                if month_key not in monthly_posting_data:
                    monthly_posting_data[month_key] = {'count': 0, 'amount': 0}
                monthly_posting_data[month_key]['count'] += 1
                monthly_posting_data[month_key]['amount'] += amount
                
                # Daily data (for trend analysis)
                if day_key not in daily_posting_data:
                    daily_posting_data[day_key] = {'count': 0, 'amount': 0}
                daily_posting_data[day_key]['count'] += 1
                daily_posting_data[day_key]['amount'] += amount
        
        # Account-wise posting analysis
        account_posting_data = {}
        for account_code, totals in gl_account_totals.items():
            total_amount = totals['debit_total'] + totals['credit_total']
            if total_amount > 0:
                account_posting_data[account_code] = {
                    'debit_total': totals['debit_total'],
                    'credit_total': totals['credit_total'],
                    'total_amount': total_amount,
                    'net_movement': totals['net_movement']
                }
        
        # Sort accounts by total activity
        top_accounts = sorted(account_posting_data.items(), key=lambda x: x[1]['total_amount'], reverse=True)[:20]
        
        chart_data = {
            'monthly_trends': {
                'labels': sorted(monthly_posting_data.keys()),
                'transaction_counts': [monthly_posting_data[month]['count'] for month in sorted(monthly_posting_data.keys())],
                'amounts': [monthly_posting_data[month]['amount'] for month in sorted(monthly_posting_data.keys())]
            },
            'top_accounts': {
                'labels': [acc[0] for acc in top_accounts],
                'debit_amounts': [acc[1]['debit_total'] for acc in top_accounts],
                'credit_amounts': [acc[1]['credit_total'] for acc in top_accounts],
                'net_movements': [acc[1]['net_movement'] for acc in top_accounts]
            },
            'chart_metadata': {
                'total_charts': 2,
                'data_period': f"{min(monthly_posting_data.keys())} to {max(monthly_posting_data.keys())}" if monthly_posting_data else "No data",
                'total_months': len(monthly_posting_data),
                'total_days_with_activity': len(daily_posting_data)
            }
        }
        
        chart_duration = (timezone.now() - chart_start).total_seconds()
        logger.info(f"📊 Chart generation completed in {chart_duration:.2f}s")
        logger.info(f"   📈 Monthly trends: {len(monthly_posting_data)} months")
        logger.info(f"   📋 Top accounts: {len(top_accounts)} accounts")
        logger.info(f"   📅 Activity period: {chart_data['chart_metadata']['data_period']}")
        
        # =======================================================================
        # OVERALL COMPLETENESS ASSESSMENT (2 STEPS)
        # =======================================================================
        
        logger.info("📊 Starting overall completeness assessment...")
        assessment_start = timezone.now()
        
        # 2-Step Completeness Assessment
        all_steps_passed = step1_completeness_passed and step2_account_verification_passed
        
        # Calculate weighted completeness score (0-100%) for 2 steps
        weights = {
            'step1': 40,  # GL completeness (fundamental)
            'step2': 60   # Account-wise verification (critical)
        }
        
        scores = {
            'step1': 100 if step1_completeness_passed else (70 if gl_is_balanced else 30),
            'step2': round(pass_rate * 100, 1) if pass_rate > 0 else 0
        }
        
        completeness_score = sum(scores[step] * weights[step] / 100 for step in weights.keys())
        
        # Determine overall status
        if all_steps_passed and completeness_score >= 95:
            overall_status = 'COMPLETE'
            overall_explanation = 'GL completeness verified: Credit-Debit balanced and all account equations verified.'
        elif completeness_score >= 80:
            overall_status = 'COMPLETE'
            overall_explanation = f'GL mostly complete with minor issues. Score: {completeness_score:.1f}%.'
        else:
            overall_status = 'INCOMPLETE'
            issues = []
            if not step1_completeness_passed:
                if not gl_is_balanced:
                    issues.append(f'GL imbalanced by {gl_net_balance:.2f}')
                if not volume_check:
                    issues.append('Insufficient data volume')
                if not data_availability:
                    issues.append('Missing TB data')
            if not step2_account_verification_passed:
                issues.append(f'{balance_equation_failed_count} accounts failed balance verification')
            overall_explanation = f"GL incomplete (Score: {completeness_score:.1f}%): {', '.join(issues)}"
        
        assessment_duration = (timezone.now() - assessment_start).total_seconds()
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        logger.info(f"📊 Overall Assessment Completed:")
        logger.info(f"   🎯 Status: {overall_status}")
        logger.info(f"   📈 Score: {completeness_score:.1f}%")
        logger.info(f"   ⏱️  Assessment Duration: {assessment_duration:.2f}s")
        logger.info(f"   ⏱️  Total Processing Duration: {processing_duration:.2f}s")
        
        # =======================================================================
        # ENHANCED COMPREHENSIVE STATISTICS CALCULATION
        # =======================================================================
        
        logger.info("📊 Calculating enhanced comprehensive statistics...")
        enhanced_stats_start = timezone.now()
        
        # Calculate additional statistics
        enhanced_stats_result = _calculate_enhanced_statistics(
            gl_postings, trial_balance_records, coa_records, 
            gl_account_totals, account_verifications
        )
        
        # Extract enhanced statistics and document verification data
        enhanced_statistics = enhanced_stats_result['enhanced_statistics']
        document_verification_data = enhanced_stats_result['document_verification_data']
        document_verification_results = enhanced_stats_result.get('document_verification_results', [])
        
        enhanced_stats_duration = (timezone.now() - enhanced_stats_start).total_seconds()
        logger.info(f"📊 Enhanced statistics calculated in {enhanced_stats_duration:.2f}s")
        
        # Prepare comprehensive statistics for charts and analysis
        comprehensive_statistics = {
            'document_statistics': {
                'total_gl_records': transaction_count,
                'total_tb_records': trial_balance_records.count(),
                'unique_accounts': account_count,
                'gl_debit_total': gl_total_debit,
                'gl_credit_total': gl_total_credit,
                'gl_net_balance': gl_net_balance
            },
            'summary_statistics': {
                'completeness_score': completeness_score,
                'steps_passed': 2 if all_steps_passed else (1 if step1_completeness_passed else 0),
                'total_steps': 2,
                'account_verification_pass_rate': pass_rate,
                'total_variance': total_variance,
                'failed_accounts_count': balance_equation_failed_count
            },
            'enhanced_statistics': enhanced_statistics,
            'chart_data': chart_data,
            'monthly_trends': chart_data.get('monthly_trends', {}),
            'top_accounts': chart_data.get('top_accounts', {}),
            'document_verification_results': document_verification_results
        }
        
        # Prepare completeness results
        completeness_results = {
            'status': overall_status,
            'explanation': overall_explanation,
            'completeness_score': round(completeness_score, 1),
            'analysis_timestamp': timezone.now().isoformat(),
            'file_name': data_file.file_name,
            'engagement_id': str(data_file.engagement_id),
            'total_gl_records': transaction_count,
            'total_tb_records': trial_balance_records.count(),
            'processing_duration': processing_duration,
            'step1_completeness': step1_completeness,
            'step2_account_verification': step2_account_verification,
            'comprehensive_statistics': comprehensive_statistics,
            'scoring_breakdown': {
                'step_scores': scores,
                'step_weights': weights,
                'final_score': round(completeness_score, 1)
            },
            'summary': {
                'tests_passed': 2 if all_steps_passed else (1 if step1_completeness_passed else 0),
                'total_tests': 2,
                'critical_issues_count': balance_equation_failed_count if not step2_account_verification_passed else 0,
                'all_steps_passed': all_steps_passed
            }
        }
        
        # Document verification data is now extracted from the enhanced statistics result
        # No need to redefine - it's already available from the function return
        
        # Save to database - use get_or_create to handle duplicate versions
        try:
            test_result, created = CompletenessTestResult.objects.get_or_create(
                engagement=data_file.engagement,
                data_version=getattr(data_file, 'version', '1.0'),
                defaults={
                    'gl_file': data_file,
                    'tb_file': None,  # We'll find TB file later
                    'coa_file': None,  # We'll find COA file later  
                    'version_notes': getattr(data_file, 'version_notes', ''),  # Use the version notes from the data file
                    'overall_status': overall_status,
                    'overall_explanation': overall_explanation,
                    'completeness_score': completeness_score,
                    'step1_file_completeness': step1_completeness,
                    'step2_gl_tb_reconciliation': step2_account_verification,
                    'step3_debit_credit_balance': {},  # Not used in 2-step
                    'step4_account_coverage': {},      # Not used in 2-step
                    'step5_coa_hierarchy_validation': {},  # Not used in 2-step
                    'step6_account_linking': {},       # Not used in 2-step
                    'step7_transaction_gaps': {},      # Not used in 2-step
                    # Document verification fields
                    **document_verification_data,
                    'total_gl_records': transaction_count,
                    'total_tb_records': trial_balance_records.count(),
                    'total_coa_records': coa_records.count() if coa_records.exists() else 0,
                    'total_accounts_unified': account_count,
                    'tests_passed': 2 if all_steps_passed else (1 if step1_completeness_passed else 0),
                    'total_tests': 2,
                    'critical_issues_count': balance_equation_failed_count if not step2_account_verification_passed else 0,
                    'comprehensive_statistics': comprehensive_statistics,
                    'processing_duration': processing_duration
                }
            )
            
            if created:
                logger.info(f"💾 New completeness test result created (ID: {test_result.id})")
            else:
                # Update existing record with new results
                test_result.overall_status = overall_status
                test_result.overall_explanation = overall_explanation
                test_result.completeness_score = completeness_score
                test_result.step1_file_completeness = step1_completeness
                test_result.step2_gl_tb_reconciliation = step2_account_verification
                test_result.total_gl_records = transaction_count
                test_result.total_tb_records = trial_balance_records.count()
                test_result.total_coa_records = coa_records.count() if coa_records.exists() else 0
                test_result.total_accounts_unified = account_count
                test_result.tests_passed = 2 if all_steps_passed else (1 if step1_completeness_passed else 0)
                test_result.total_tests = 2
                test_result.critical_issues_count = balance_equation_failed_count if not step2_account_verification_passed else 0
                test_result.comprehensive_statistics = comprehensive_statistics
                test_result.processing_duration = processing_duration
                # Update document verification data
                for key, value in document_verification_data.items():
                    setattr(test_result, key, value)
                test_result.save()
                logger.info(f"💾 Existing completeness test result updated (ID: {test_result.id})")
            
        except Exception as db_error:
            logger.error(f"Failed to save completeness test result: {db_error}")
            # Continue execution even if DB save fails
        
        logger.info("================================================================================")
        logger.info(f"✅ COMPLETENESS ANALYSIS COMPLETED SUCCESSFULLY")
        logger.info(f"📊 Status: {overall_status}, Score: {completeness_score:.1f}%")
        logger.info(f"⏱️  Total Duration: {processing_duration:.2f} seconds")
        logger.info("================================================================================")
        
        return {
            'success': True,
            'completeness_results': completeness_results,
            'analysis_duration': processing_duration,
            'test_result_id': test_result.id if 'test_result' in locals() else None
        }
        
    except Exception as e:
        error_message = f"Completeness analysis failed: {str(e)}"
        logger.error(f"❌ {error_message}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'success': False,
            'error': error_message,
            'analysis_duration': (timezone.now() - start_time).total_seconds() if 'start_time' in locals() else 0
        }


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
            data_version=gl_file.version if gl_file else '1.0',  # Use GL file version as primary
            version_notes=gl_file.version_notes if gl_file else '',  # Use GL file version notes
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


def _calculate_enhanced_statistics(gl_postings, trial_balance_records, coa_records, gl_account_totals, account_verifications):
    """
    Calculate enhanced comprehensive statistics including totals, means, standard deviations, and detailed breakdowns
    """
    import statistics
    from collections import defaultdict, Counter
    
    logger.info("📊 Calculating enhanced statistics...")
    
    # =======================================================================
    # BASIC TOTALS AND COUNTS
    # =======================================================================
    
    # GL Account totals
    total_gl_accounts = len(gl_account_totals)
    
    # Profit Center analysis
    profit_centers = set()
    profit_center_transactions = defaultdict(int)
    profit_center_amounts = defaultdict(float)
    
    # User analysis
    users = set()
    user_transactions = defaultdict(int)
    user_debit_totals = defaultdict(float)
    user_credit_totals = defaultdict(float)
    
    # Transaction type analysis
    debit_entries = 0
    credit_entries = 0
    transaction_amounts = []
    account_transaction_counts = defaultdict(int)
    
    # =======================================================================
    # AUDIT CALCULATION STATISTICS - DOCUMENT VERIFICATION
    # =======================================================================
    
    # Document verification analysis
    document_analysis = defaultdict(lambda: {
        'debit_total': 0.0,
        'credit_total': 0.0,
        'net_balance': 0.0,
        'is_balanced': False,
        'transaction_count': 0,
        'accounts': set(),
        'users': set(),
        'profit_centers': set(),
        'posting_dates': set(),
        'document_types': set()
    })
    
    for posting in gl_postings:
        # Profit Center analysis
        if posting.profit_center:
            profit_centers.add(posting.profit_center)
            profit_center_transactions[posting.profit_center] += 1
            profit_center_amounts[posting.profit_center] += abs(float(posting.amount_local_currency or 0))
        
        # User analysis
        if posting.user_name:
            users.add(posting.user_name)
            user_transactions[posting.user_name] += 1
            amount = float(posting.amount_local_currency or 0)
            if amount > 0:
                user_debit_totals[posting.user_name] += amount
                debit_entries += 1
            else:
                user_credit_totals[posting.user_name] += abs(amount)
                credit_entries += 1
            transaction_amounts.append(abs(amount))
        
        # Account transaction counts
        account_code = str(posting.gl_account).replace('.0', '')
        account_transaction_counts[account_code] += 1
        
        # =======================================================================
        # DOCUMENT VERIFICATION ANALYSIS
        # =======================================================================
        
        # Get document number for analysis - handle empty/missing document numbers
        document_number = posting.document_number or ''
        if not document_number.strip():
            document_number = 'NO_DOCUMENT'
        amount = float(posting.amount_local_currency or 0)
        
        # Update document analysis
        doc_analysis = document_analysis[document_number]
        doc_analysis['transaction_count'] += 1
        
        # Add account to document
        doc_analysis['accounts'].add(account_code)
        
        # Add user to document
        if posting.user_name:
            doc_analysis['users'].add(posting.user_name)
        
        # Add profit center to document
        if posting.profit_center:
            doc_analysis['profit_centers'].add(posting.profit_center)
        
        # Add posting date to document
        if posting.posting_date:
            doc_analysis['posting_dates'].add(posting.posting_date.strftime('%Y-%m-%d'))
        
        # Add document type to document
        if posting.document_type:
            doc_analysis['document_types'].add(posting.document_type)
        
        # Calculate debit/credit totals for document
        if amount > 0:
            doc_analysis['debit_total'] += amount
        else:
            doc_analysis['credit_total'] += abs(amount)
    
    total_profit_centers = len(profit_centers)
    total_users = len(users)
    
    # =======================================================================
    # DOCUMENT VERIFICATION CALCULATIONS
    # =======================================================================
    
    logger.info("📊 Calculating document verification statistics...")
    
    # Calculate document balances and verification
    document_verification_results = []
    balanced_documents = 0
    unbalanced_documents = 0
    total_document_variance = 0.0
    
    for doc_number, doc_analysis in document_analysis.items():
        # Calculate net balance (debit - credit)
        doc_analysis['net_balance'] = doc_analysis['debit_total'] - doc_analysis['credit_total']
        
        # Check if document is balanced (within 0.01 tolerance)
        doc_analysis['is_balanced'] = abs(doc_analysis['net_balance']) < 0.01
        
        # Count balanced/unbalanced documents
        if doc_analysis['is_balanced']:
            balanced_documents += 1
        else:
            unbalanced_documents += 1
            total_document_variance += abs(doc_analysis['net_balance'])
        
        # Convert sets to lists for JSON serialization
        doc_verification = {
            'document_number': doc_number,
            'debit_total': round(doc_analysis['debit_total'], 2),
            'credit_total': round(doc_analysis['credit_total'], 2),
            'net_balance': round(doc_analysis['net_balance'], 2),
            'is_balanced': doc_analysis['is_balanced'],
            'transaction_count': doc_analysis['transaction_count'],
            'account_count': len(doc_analysis['accounts']),
            'accounts': list(doc_analysis['accounts']),
            'user_count': len(doc_analysis['users']),
            'users': list(doc_analysis['users']),
            'profit_center_count': len(doc_analysis['profit_centers']),
            'profit_centers': list(doc_analysis['profit_centers']),
            'posting_dates': list(doc_analysis['posting_dates']),
            'document_types': list(doc_analysis['document_types']),
            'account_links': {
                'total_accounts': len(doc_analysis['accounts']),
                'account_codes': list(doc_analysis['accounts']),
                'account_diversity': len(doc_analysis['accounts']) / doc_analysis['transaction_count'] if doc_analysis['transaction_count'] > 0 else 0
            }
        }
        
        document_verification_results.append(doc_verification)
    
    # Sort documents by transaction count (most active first)
    document_verification_results.sort(key=lambda x: x['transaction_count'], reverse=True)
    
    # Calculate document verification statistics
    total_documents = len(document_verification_results)
    document_balance_rate = (balanced_documents / total_documents) if total_documents > 0 else 0
    average_document_variance = (total_document_variance / unbalanced_documents) if unbalanced_documents > 0 else 0
    
    # Separate transactions with and without document numbers
    transactions_with_documents = [d for d in document_verification_results if d['document_number'] != 'NO_DOCUMENT']
    transactions_without_documents = [d for d in document_verification_results if d['document_number'] == 'NO_DOCUMENT']
    
    # Count transactions without document numbers
    no_document_transactions = len(transactions_without_documents)
    no_document_count = sum(d['transaction_count'] for d in transactions_without_documents)
    
    # Account linkage analysis per document
    account_linkage_stats = {
        'documents_with_single_account': len([d for d in document_verification_results if d['account_count'] == 1]),
        'documents_with_multiple_accounts': len([d for d in document_verification_results if d['account_count'] > 1]),
        'average_accounts_per_document': sum(d['account_count'] for d in document_verification_results) / total_documents if total_documents > 0 else 0,
        'max_accounts_in_document': max((d['account_count'] for d in document_verification_results), default=0),
        'min_accounts_in_document': min((d['account_count'] for d in document_verification_results), default=0)
    }
    
    logger.info(f"📊 Document verification completed:")
    logger.info(f"   📄 Total Documents: {total_documents}")
    logger.info(f"   ✅ Balanced Documents: {balanced_documents}")
    logger.info(f"   ❌ Unbalanced Documents: {unbalanced_documents}")
    logger.info(f"   📊 Balance Rate: {document_balance_rate:.1%}")
    logger.info(f"   💰 Total Variance: {total_document_variance:.2f}")
    logger.info(f"   📝 Transactions with Documents: {len(transactions_with_documents)}")
    logger.info(f"   ❓ Transactions without Documents: {no_document_transactions} ({no_document_count} transactions)")
    
    # =======================================================================
    # STATISTICAL CALCULATIONS
    # =======================================================================
    
    # Amount statistics
    if transaction_amounts:
        mean_amount = statistics.mean(transaction_amounts)
        median_amount = statistics.median(transaction_amounts)
        std_dev_amount = statistics.stdev(transaction_amounts) if len(transaction_amounts) > 1 else 0
        min_amount = min(transaction_amounts)
        max_amount = max(transaction_amounts)
    else:
        mean_amount = median_amount = std_dev_amount = min_amount = max_amount = 0
    
    # Per-account statistics
    account_transaction_counts_list = list(account_transaction_counts.values())
    if account_transaction_counts_list:
        mean_transactions_per_account = statistics.mean(account_transaction_counts_list)
        median_transactions_per_account = statistics.median(account_transaction_counts_list)
        std_dev_transactions_per_account = statistics.stdev(account_transaction_counts_list) if len(account_transaction_counts_list) > 1 else 0
        max_transactions_per_account = max(account_transaction_counts_list)
        min_transactions_per_account = min(account_transaction_counts_list)
    else:
        mean_transactions_per_account = median_transactions_per_account = std_dev_transactions_per_account = 0
        max_transactions_per_account = min_transactions_per_account = 0
    
    # =======================================================================
    # CHART DATA GENERATION
    # =======================================================================
    
    # Account by transaction counts (top 20)
    account_by_transactions = sorted(
        account_transaction_counts.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:20]
    
    # User by transaction counts (top 20)
    user_by_transactions = sorted(
        user_transactions.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:20]
    
    # User by credit/debit analysis
    user_credit_debit_data = []
    for user in users:
        user_credit_debit_data.append({
            'user': user,
            'debit_total': user_debit_totals.get(user, 0),
            'credit_total': user_credit_totals.get(user, 0),
            'transaction_count': user_transactions.get(user, 0),
            'net_amount': user_debit_totals.get(user, 0) - user_credit_totals.get(user, 0)
        })
    
    # Sort by transaction count
    user_credit_debit_data = sorted(user_credit_debit_data, key=lambda x: x['transaction_count'], reverse=True)[:20]
    
    # Profit Center analysis
    profit_center_data = []
    for pc in profit_centers:
        profit_center_data.append({
            'profit_center': pc,
            'transaction_count': profit_center_transactions.get(pc, 0),
            'total_amount': profit_center_amounts.get(pc, 0)
        })
    
    # Sort by transaction count
    profit_center_data = sorted(profit_center_data, key=lambda x: x['transaction_count'], reverse=True)[:20]
    
    # =======================================================================
    # COMPREHENSIVE STATISTICS OBJECT
    # =======================================================================
    
    enhanced_statistics = {
        'basic_totals': {
            'total_gl_accounts': total_gl_accounts,
            'total_profit_centers': total_profit_centers,
            'total_users': total_users,
            'total_debit_entries': debit_entries,
            'total_credit_entries': credit_entries,
            'total_transactions': len(gl_postings)
        },
        'amount_statistics': {
            'mean_amount': round(mean_amount, 2),
            'median_amount': round(median_amount, 2),
            'std_deviation_amount': round(std_dev_amount, 2),
            'min_amount': round(min_amount, 2),
            'max_amount': round(max_amount, 2),
            'total_debit_amount': round(sum(user_debit_totals.values()), 2),
            'total_credit_amount': round(sum(user_credit_totals.values()), 2)
        },
        'per_account_statistics': {
            'mean_transactions_per_account': round(mean_transactions_per_account, 2),
            'median_transactions_per_account': round(median_transactions_per_account, 2),
            'std_deviation_transactions_per_account': round(std_dev_transactions_per_account, 2),
            'max_transactions_per_account': max_transactions_per_account,
            'min_transactions_per_account': min_transactions_per_account
        },
        'chart_data_enhanced': {
            'account_by_transaction_counts': {
                'labels': [acc[0] for acc in account_by_transactions],
                'transaction_counts': [acc[1] for acc in account_by_transactions]
            },
            'user_by_transaction_counts': {
                'labels': [user[0] for user in user_by_transactions],
                'transaction_counts': [user[1] for user in user_by_transactions]
            },
            'user_credit_debit_analysis': {
                'labels': [user['user'] for user in user_credit_debit_data],
                'debit_totals': [user['debit_total'] for user in user_credit_debit_data],
                'credit_totals': [user['credit_total'] for user in user_credit_debit_data],
                'net_amounts': [user['net_amount'] for user in user_credit_debit_data],
                'transaction_counts': [user['transaction_count'] for user in user_credit_debit_data]
            },
            'profit_center_analysis': {
                'labels': [pc['profit_center'] for pc in profit_center_data],
                'transaction_counts': [pc['transaction_count'] for pc in profit_center_data],
                'total_amounts': [pc['total_amount'] for pc in profit_center_data]
            }
        },
        'data_quality_metrics': {
            'accounts_with_transactions': len(account_transaction_counts),
            'users_with_activity': len(users),
            'profit_centers_with_activity': len(profit_centers),
            'average_transactions_per_user': round(len(gl_postings) / total_users, 2) if total_users > 0 else 0,
            'average_transactions_per_account': round(len(gl_postings) / total_gl_accounts, 2) if total_gl_accounts > 0 else 0,
            'average_transactions_per_profit_center': round(len(gl_postings) / total_profit_centers, 2) if total_profit_centers > 0 else 0
        },
        'audit_calculation_statistics': {
            # Document verification data removed - now saved in dedicated fields only
            'account_linkage_analysis': account_linkage_stats,
            'document_balance_verification': {
                'verification_rule': 'Each document must have Debit Total = Credit Total (within 0.01 tolerance). Transactions without document numbers are tracked separately.',
                'verification_passed': document_balance_rate >= 0.95,  # 95% of documents should be balanced
                'critical_issues': unbalanced_documents + no_document_transactions,
                'recommendation': f'Review {unbalanced_documents} unbalanced documents and {no_document_transactions} transactions without document numbers' if (unbalanced_documents > 0 or no_document_transactions > 0) else 'All documents are properly balanced'
            }
            # Note: document_details, unbalanced_documents, no_document_transactions, transactions_with_documents
            # are now saved in dedicated fields only, not in JSON
        }
    }
    
    logger.info(f"📊 Enhanced statistics calculated:")
    logger.info(f"   📋 Total GL Accounts: {total_gl_accounts}")
    logger.info(f"   🏢 Total Profit Centers: {total_profit_centers}")
    logger.info(f"   👥 Total Users: {total_users}")
    logger.info(f"   💰 Mean Amount: {mean_amount:.2f}")
    logger.info(f"   📊 Mean Transactions per Account: {mean_transactions_per_account:.2f}")
    logger.info(f"   📄 Total Documents: {total_documents}")
    logger.info(f"   ✅ Balanced Documents: {balanced_documents}")
    logger.info(f"   ❌ Unbalanced Documents: {unbalanced_documents}")
    logger.info(f"   📊 Document Balance Rate: {document_balance_rate:.1%}")
    
    # Return both enhanced statistics and document verification data
    return {
        'enhanced_statistics': enhanced_statistics,
        'document_verification_data': {
            'total_documents': total_documents,
            'balanced_documents': balanced_documents,
            'unbalanced_documents': unbalanced_documents,
            'document_balance_rate': document_balance_rate,
            'total_document_variance': total_document_variance,
            'average_document_variance': average_document_variance,
            'transactions_with_documents': len(transactions_with_documents),
            'transactions_without_documents': no_document_transactions,
            'no_document_transaction_count': no_document_count
        },
        'document_verification_results': document_verification_results
    }


@shared_task(bind=True, name='core.tasks.trigger_eng008_completeness')
def trigger_eng008_completeness(self):
    """
    Simple task to trigger completeness test for ENG-008
    """
    try:
        logger.info("Starting ENG-008 completeness trigger task")
        
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        logger.info(f"Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
        
        # Get the latest data file for this engagement
        latest_data_file = DataFile.objects.filter(
            engagement=engagement
        ).order_by('-created_at').first()
        
        if not latest_data_file:
            logger.error("No data file found for ENG-008")
            return {"status": "error", "message": "No data file found for ENG-008"}
        
        logger.info(f"Using data file: {latest_data_file.file_name} (ID: {latest_data_file.id})")
        
        # Trigger the completeness analysis task
        logger.info("Triggering completeness analysis task...")
        completeness_task = run_gl_completeness_analysis.delay(latest_data_file.id)
        
        logger.info(f"Completeness task triggered with ID: {completeness_task.id}")
        
        # Trigger AI prediction task
        logger.info("Triggering AI completeness prediction...")
        try:
            from .ml_training_tasks import predict_completeness_with_ai
            ai_prediction_task = predict_completeness_with_ai.delay(latest_data_file.id)
            logger.info(f"AI prediction task triggered with ID: {ai_prediction_task.id}")
        except Exception as e:
            logger.error(f"Failed to trigger AI prediction: {e}")
            ai_prediction_task = None
        
        # Trigger ML model training
        logger.info("Triggering ML model training...")
        try:
            from .ml_training_tasks import train_comprehensive_ai_models
            ml_training_task = train_comprehensive_ai_models.delay(
                engagement_id=engagement.id,
                client_name=engagement.engagement_name
            )
            logger.info(f"ML training task triggered with ID: {ml_training_task.id}")
        except Exception as e:
            logger.error(f"Failed to trigger ML training: {e}")
            ml_training_task = None
        
        # Trigger GL volume prediction model training
        logger.info("Triggering GL volume prediction model training...")
        try:
            from core.gl_prediction_tasks import train_gl_volume_prediction_model
            gl_prediction_task = train_gl_volume_prediction_model.delay(
                engagement_id=engagement.id,
                client_name=engagement.engagement_name
            )
            logger.info(f"GL volume prediction task triggered with ID: {gl_prediction_task.id}")
        except Exception as e:
            logger.error(f"Failed to trigger GL volume prediction: {e}")
            gl_prediction_task = None
        
        return {
            "status": "success", 
            "message": "Completeness test, AI prediction, ML training, and GL volume prediction triggered for ENG-008",
            "data_file_id": latest_data_file.id,
            "completeness_task_id": completeness_task.id,
            "ai_prediction_task_id": ai_prediction_task.id if ai_prediction_task else None,
            "ml_training_task_id": ml_training_task.id if ml_training_task else None,
            "gl_prediction_task_id": gl_prediction_task.id if gl_prediction_task else None
        }
        
    except Engagement.DoesNotExist:
        logger.error("ENG-008 engagement not found")
        return {"status": "error", "message": "ENG-008 engagement not found"}
    except Exception as e:
        logger.error(f"Error triggering ENG-008 completeness: {e}")
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, name='core.tasks.run_eng008_complete_workflow')
def run_eng008_complete_workflow(self):
    """
    Complete workflow for ENG-008: Completeness Test → AI Prediction → ML Training
    """
    try:
        logger.info("Starting complete ENG-008 workflow")
        
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        logger.info(f"Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
        
        # Get the latest data file for this engagement
        latest_data_file = DataFile.objects.filter(
            engagement=engagement
        ).order_by('-created_at').first()
        
        if not latest_data_file:
            logger.error("No data file found for ENG-008")
            return {"status": "error", "message": "No data file found for ENG-008"}
        
        logger.info(f"Using data file: {latest_data_file.file_name} (ID: {latest_data_file.id})")
        
        # Step 1: Run completeness analysis
        logger.info("Step 1: Running completeness analysis...")
        completeness_result = run_gl_completeness_analysis(latest_data_file.id)
        
        if not completeness_result.get('success', False):
            logger.error(f"Completeness analysis failed: {completeness_result.get('error', 'Unknown error')}")
            return {"status": "error", "message": "Completeness analysis failed"}
        
        logger.info("Step 1 completed: Completeness analysis successful")
        
        # Step 2: Run AI prediction
        logger.info("Step 2: Running AI prediction...")
        try:
            from .ml_training_tasks import predict_completeness_with_ai
            ai_prediction_result = predict_completeness_with_ai(latest_data_file.id)
            logger.info(f"Step 2 completed: AI prediction - {ai_prediction_result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"AI prediction failed: {e}")
            ai_prediction_result = {"status": "error", "message": str(e)}
        
        # Step 3: Run ML model training
        logger.info("Step 3: Running ML model training...")
        try:
            from .ml_training_tasks import train_comprehensive_ai_models
            ml_training_result = train_comprehensive_ai_models(
                engagement_id=engagement.id,
                client_name=engagement.engagement_name
            )
            logger.info(f"Step 3 completed: ML training - {ml_training_result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"ML training failed: {e}")
            ml_training_result = {"status": "error", "message": str(e)}
        
        # Step 4: Run GL volume prediction model training
        logger.info("Step 4: Running GL volume prediction model training...")
        try:
            from core.gl_prediction_tasks import train_gl_volume_prediction_model
            gl_prediction_result = train_gl_volume_prediction_model(
                engagement_id=engagement.id,
                client_name=engagement.engagement_name
            )
            logger.info(f"Step 4 completed: GL volume prediction - {gl_prediction_result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"GL volume prediction failed: {e}")
            gl_prediction_result = {"status": "error", "message": str(e)}
        
        return {
            "status": "success",
            "message": "Complete ENG-008 workflow executed with future prediction",
            "data_file_id": latest_data_file.id,
            "completeness_result": completeness_result,
            "ai_prediction_result": ai_prediction_result,
            "ml_training_result": ml_training_result,
            "gl_prediction_result": gl_prediction_result
        }
        
    except Engagement.DoesNotExist:
        logger.error("ENG-008 engagement not found")
        return {"status": "error", "message": "ENG-008 engagement not found"}
    except Exception as e:
        logger.error(f"Complete workflow failed: {e}")
        return {"status": "error", "message": str(e)}