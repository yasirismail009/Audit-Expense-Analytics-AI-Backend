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

def run_duplicate_analysis_sync(job_id):
    """Run Duplicate Analysis synchronously and save to database"""
    
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
        
        # Run basic duplicate analysis without ML orchestrator
        duplicate_pairs = []
        duplicates_found = 0
        
        # Simple duplicate detection based on amount and date
        transaction_dict = {}
        for t in transactions:
            key = (float(t.amount_local_currency), t.posting_date, t.gl_account)
            if key in transaction_dict:
                # Found a potential duplicate
                duplicate_pairs.append({
                    'transaction1': {
                        'id': str(transaction_dict[key].id),
                        'document_number': transaction_dict[key].document_number,
                        'amount': float(transaction_dict[key].amount_local_currency),
                        'posting_date': transaction_dict[key].posting_date.isoformat() if transaction_dict[key].posting_date else None,
                        'user': transaction_dict[key].user_name,
                        'account': transaction_dict[key].gl_account
                    },
                    'transaction2': {
                        'id': str(t.id),
                        'document_number': t.document_number,
                        'amount': float(t.amount_local_currency),
                        'posting_date': t.posting_date.isoformat() if t.posting_date else None,
                        'user': t.user_name,
                        'account': t.gl_account
                    },
                    'similarity_score': 1.0,
                    'risk_level': 'HIGH'
                })
                duplicates_found += 1
            else:
                transaction_dict[key] = t
        
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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Save to DuplicateAnalysisResult table
        duplicate_analysis_result = DuplicateAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_duplicate',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'duplicates_found': duplicate_results.get('duplicates_found', 0),
                'duplicate_percentage': duplicate_results.get('compliance_assessment', {}).get('duplicate_percentage', 0)
            },
            duplicate_list=duplicate_results.get('duplicate_pairs', []),
            breakdowns={
                'duplicate_by_document': duplicate_results.get('duplicate_by_amount', []),
                'duplicate_by_account': duplicate_results.get('duplicate_by_account', []),
                'duplicate_by_user': duplicate_results.get('duplicate_by_user', []),
                'audit_recommendations': duplicate_results.get('audit_recommendations', []),
                'compliance_assessment': duplicate_results.get('compliance_assessment', {}),
                'financial_statement_impact': duplicate_results.get('financial_statement_impact', {})
            },
            chart_data=duplicate_results.get('chart_data', {}),
            export_data=duplicate_results.get('export_data', []),
            processing_duration=duplicate_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
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
        
        # Save failed result to database
        try:
            DuplicateAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_duplicate',
                analysis_version='1.0.0',
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
        
        # Run basic backdated analysis without ML orchestrator
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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Save to BackdatedAnalysisResult table
        backdated_analysis_result = BackdatedAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_backdated',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'backdated_entries_found': backdated_results.get('backdated_entries_found', 0),
                'backdated_percentage': backdated_results.get('compliance_assessment', {}).get('backdated_percentage', 0)
            },
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
        
        # Save failed result to database
        try:
            BackdatedAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_backdated',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Save to UserAnalysisResult table
        user_analysis_result = UserAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='user_analysis',
            analysis_version='1.0.0',
            analysis_info={
                'total_users': user_results.get('total_users', 0),
                'total_transactions': len(transactions),
                'high_risk_users': user_results.get('statistical_summary', {}).get('high_risk_users', 0),
                'users_with_anomalies': user_results.get('statistical_summary', {}).get('users_with_anomalies', 0)
            },
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
        
        # Save failed result to database
        try:
            UserAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='user_analysis',
                analysis_version='1.0.0',
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
        
        # Run basic unusual days analysis without ML orchestrator
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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Save to UnusualDaysAnalysisResult table
        unusual_days_analysis_result = UnusualDaysAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='unusual_days_analysis',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'unusual_days_count': len(unusual_days_transactions)
            },
            weekend_postings=unusual_days_transactions,
            unusual_days=unusual_days_transactions,
            audit_recommendations=unusual_days_results.get('audit_recommendations', {}),
            compliance_assessment=unusual_days_results.get('compliance_assessment', {}),
            financial_statement_impact=unusual_days_results.get('financial_statement_impact', {}),
            chart_data=unusual_days_results.get('chart_data', {}),
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
        
        # Save failed result to database
        try:
            UnusualDaysAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='unusual_days_analysis',
                analysis_version='1.0.0',
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
        
        # Run basic closing entries analysis without ML orchestrator
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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Save to ClosingEntriesAnalysisResult table
        closing_entries_analysis_result = ClosingEntriesAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='closing_entries_analysis',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'closing_entries_count': len(closing_entries_transactions)
            },
            closing_entries=closing_entries_transactions,
            audit_recommendations=closing_entries_results.get('audit_recommendations', {}),
            compliance_assessment=closing_entries_results.get('compliance_assessment', {}),
            financial_statement_impact=closing_entries_results.get('financial_statement_impact', {}),
            chart_data=closing_entries_results.get('chart_data', {}),
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
        
        # Save failed result to database
        try:
            ClosingEntriesAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='closing_entries_analysis',
                analysis_version='1.0.0',
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
        
        # Run basic holiday analysis without ML orchestrator
        from datetime import datetime, timedelta
        
        # Use holiday_utils to get dynamic Saudi Arabian holidays
        from .holiday_utils import get_holidays, is_holiday
        
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
                posting_date_str = t.posting_date.strftime('%Y-%m-%d')
                
                # Check if the posting date is in our pre-fetched holiday dates
                if posting_date_str in holiday_dates:
                    holiday_name = holiday_info.get(posting_date_str, {}).get('name', 'Saudi Holiday')
                    
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
            'processing_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Calculate overall risk score based on holiday transactions
        total_amount = sum(float(t.get('amount', 0)) for t in holiday_transactions)
        holiday_percentage = (len(holiday_transactions) / len(transactions) * 100) if transactions else 0.0
        
        # Risk score calculation: higher score for more holiday transactions and higher amounts
        risk_score = min(100.0, (len(holiday_transactions) * 10) + (holiday_percentage * 2) + (total_amount / 1000000))
        
        # Save to HolidayAnalysisResult table
        holiday_analysis_result = HolidayAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='holiday_analysis',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'holiday_transactions_count': len(holiday_transactions),
                'holiday_breakdown': _generate_holiday_breakdown(holiday_transactions),
                'total_holiday_amount': total_amount,
                'holidays_detected': list(set(t.get('holiday_name') for t in holiday_transactions)),
                'overall_risk_score': risk_score,
                'holiday_percentage': holiday_percentage
            },
            holiday_postings=holiday_transactions,
            chart_data=holiday_results.get('chart_data', {}),
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
        
        # Save failed result to database
        try:
            HolidayAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='holiday_analysis',
                analysis_version='1.0.0',
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