"""
ML Analysis Orchestrator
Centralized system for efficient ML model training and analysis
"""

import os
import django
from django.utils import timezone
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class MLAnalysisOrchestrator:
    """
    Centralized orchestrator for ML model training and analysis
    Ensures models are trained only once and reused efficiently
    """
    
    def __init__(self):
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        django.setup()
        
        from .specialized_analysis_models import AnalysisModelManager
        self.model_manager = AnalysisModelManager()
        self._analysis_cache = {}  # Cache analysis results
        self._training_log = {}  # Log of training activities
        
    def get_model_status(self) -> Dict[str, str]:
        """Get current status of all ML models"""
        return self.model_manager.get_training_status()
    
    def ensure_models_ready(self, transactions: List) -> bool:
        """
        Ensure all required models are trained and ready for analysis
        
        Args:
            transactions: List of transactions for training
            
        Returns:
            bool: True if all models are ready
        """
        logger.info("Ensuring all ML models are ready for analysis...")
        
        # Check if we have enough data
        if len(transactions) < 10:
            logger.warning("Insufficient data for ML analysis. Need at least 10 transactions.")
            return False
        
        # Step 1: Train basic models (general, duplicate, backdated)
        basic_models = ['general', 'duplicate', 'backdated']
        basic_results = {}
        
        for model_type in basic_models:
            success = self.model_manager.ensure_model_trained(model_type, transactions)
            basic_results[model_type] = success
            if success:
                self._training_log[model_type] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'SUCCESS'
                }
            else:
                self._training_log[model_type] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'FAILED'
                }
        
        # Step 2: Train overall model (requires basic model results)
        if all(basic_results.values()):
            logger.info("Training overall analysis model...")
            overall_success = self._train_overall_model(transactions)
            basic_results['overall'] = overall_success
            if overall_success:
                self._training_log['overall'] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'SUCCESS'
                }
            else:
                self._training_log['overall'] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'FAILED'
                }
        
        # Step 3: Train risk model (requires overall model results)
        if basic_results.get('overall', False):
            logger.info("Training risk analysis model...")
            risk_success = self._train_risk_model(transactions)
            basic_results['risk'] = risk_success
            if risk_success:
                self._training_log['risk'] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'SUCCESS'
                }
            else:
                self._training_log['risk'] = {
                    'trained_at': timezone.now(),
                    'data_size': len(transactions),
                    'status': 'FAILED'
                }
        
        all_ready = all(basic_results.values())
        logger.info(f"Model training completed. All ready: {all_ready}")
        
        return all_ready
    
    def _train_overall_model(self, transactions: List) -> bool:
        """Train the overall analysis model with dependencies"""
        try:
            # Get results from basic models
            general_results = self.model_manager.predict_with_model('general', transactions)
            duplicate_results = self.model_manager.predict_with_model('duplicate', transactions)
            backdated_results = self.model_manager.predict_with_model('backdated', transactions)
            
            # Create overall risk scores for training (simple heuristic)
            overall_risk_scores = []
            for i, t in enumerate(transactions):
                risk_score = 0.0
                
                # Add risk from general analysis
                if i < len(general_results):
                    general_risk = general_results[i].get('risk_score', 0.0)
                    # Scale down general risk to prevent over-weighting (max 30 points)
                    risk_score += min(general_risk / 3.33, 30.0)  # Scale factor to cap at 30
                
                # Add risk from duplicate analysis
                duplicate_found = any(d.get('transaction1', {}).get('id') == str(t.id) for d in duplicate_results)
                if duplicate_found:
                    # Find the duplicate and add risk based on duplicate type
                    duplicate_entry = next((d for d in duplicate_results if d.get('transaction1', {}).get('id') == str(t.id)), None)
                    if duplicate_entry:
                        risk_score += duplicate_entry.get('risk_score', 25.0) / 4.0  # Scale down the risk score
                
                # Add risk from backdated analysis
                backdated_found = any(b.get('transaction_id') == str(t.id) for b in backdated_results)
                if backdated_found:
                    risk_score += 25.0
                
                # Add risk from fiscal year validation
                if t.data_file and t.data_file.fiscal_year:
                    # Check if transaction fiscal year matches file fiscal year
                    if t.fiscal_year != t.data_file.fiscal_year:
                        risk_score += 15.0  # Fiscal year mismatch
                    
                    # Check if transaction date is within audit period
                    if t.data_file.audit_start_date and t.data_file.audit_end_date:
                        if t.posting_date and (t.posting_date < t.data_file.audit_start_date or t.posting_date > t.data_file.audit_end_date):
                            risk_score += 20.0  # Transaction outside audit period
                
                overall_risk_scores.append(min(risk_score, 100.0))  # Cap at 100
            
            # Train overall model
            overall_model = self.model_manager.get_model('overall')
            success = overall_model.train(transactions, general_results, duplicate_results, backdated_results, overall_risk_scores)
            
            if success:
                logger.info("Overall analysis model trained successfully")
            else:
                logger.error("Failed to train overall analysis model")
            
            return success
            
        except Exception as e:
            logger.error(f"Error training overall model: {e}")
            return False
    
    def _train_risk_model(self, transactions: List) -> bool:
        """Train the risk analysis model with dependencies"""
        try:
            # Get overall analysis results
            overall_results = self.model_manager.predict_with_model('overall', transactions)
            
            if not overall_results:
                logger.warning("No overall results available for risk model training")
                return False
            
            # Create risk labels for training (based on overall risk scores)
            risk_labels = []
            for result in overall_results:
                risk_score = result['overall_risk_score']
                if risk_score >= 80:
                    risk_labels.append(3)  # Critical
                elif risk_score >= 60:
                    risk_labels.append(2)  # High
                elif risk_score >= 30:
                    risk_labels.append(1)  # Medium
                else:
                    risk_labels.append(0)  # Low
            
            # Train risk model
            risk_model = self.model_manager.get_model('risk')
            success = risk_model.train(transactions, overall_results, risk_labels)
            
            if success:
                logger.info("Risk analysis model trained successfully")
            else:
                logger.error("Failed to train risk analysis model")
            
            return success
            
        except Exception as e:
            logger.error(f"Error training risk model: {e}")
            return False
    
    def run_general_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run general analysis with ML model and detailed GL account analysis
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running General Analysis with ML for {len(transactions)} transactions")
        
        # Get ML predictions
        ml_results = self.model_manager.predict_with_model('general', transactions)
        
        # Calculate basic statistics
        total_amount = sum(t.amount_local_currency for t in transactions)
        unique_users = len(set(t.user_name for t in transactions))
        unique_accounts = len(set(t.gl_account for t in transactions))
        
        # Process ML insights
        ml_anomalies = [r for r in ml_results if r['prediction'] == 1]
        ml_confidence_scores = [r['confidence'] for r in ml_results]
        avg_ml_confidence = sum(ml_confidence_scores) / len(ml_confidence_scores) if ml_confidence_scores else 0
        
        # Calculate trial balance (separate debits and credits)
        total_debits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'DEBIT')
        total_credits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'CREDIT')
        trial_balance = float(total_debits - total_credits)
        
        # Detailed GL Account Analysis with Debit/Credit/Balance
        gl_account_analysis = {}
        for transaction in transactions:
            account = transaction.gl_account
            if account not in gl_account_analysis:
                gl_account_analysis[account] = {
                    'account': account,
                    'debit_amount': 0.0,
                    'credit_amount': 0.0,
                    'balance': 0.0,
                    'debit_count': 0,
                    'credit_count': 0,
                    'total_count': 0,
                    'transactions': []
                }
            
            amount = float(transaction.amount_local_currency)
            gl_account_analysis[account]['total_count'] += 1
            gl_account_analysis[account]['transactions'].append({
                'id': str(transaction.id),
                'document_number': transaction.document_number,
                'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
                'amount': amount,
                'transaction_type': transaction.transaction_type,
                'user_name': transaction.user_name,
                'text': transaction.text
            })
            
            if transaction.transaction_type == 'DEBIT':
                gl_account_analysis[account]['debit_amount'] += amount
                gl_account_analysis[account]['debit_count'] += 1
            else:  # CREDIT
                gl_account_analysis[account]['credit_amount'] += amount
                gl_account_analysis[account]['credit_count'] += 1
        
        # Calculate balance for each account
        for account_data in gl_account_analysis.values():
            account_data['balance'] = account_data['debit_amount'] - account_data['credit_amount']
        
        # Convert to list and sort by balance (highest to lowest)
        gl_account_summaries = list(gl_account_analysis.values())
        gl_account_summaries.sort(key=lambda x: abs(x['balance']), reverse=True)
        
        # User Analysis with Debit/Credit breakdown
        user_analysis = {}
        for transaction in transactions:
            user = transaction.user_name
            if user not in user_analysis:
                user_analysis[user] = {
                    'user': user,
                    'debit_amount': 0.0,
                    'credit_amount': 0.0,
                    'balance': 0.0,
                    'debit_count': 0,
                    'credit_count': 0,
                    'total_count': 0,
                    'accounts_used': set(),
                    'transactions': []
                }
            
            amount = float(transaction.amount_local_currency)
            user_analysis[user]['total_count'] += 1
            user_analysis[user]['accounts_used'].add(transaction.gl_account)
            user_analysis[user]['transactions'].append({
                'id': str(transaction.id),
                'document_number': transaction.document_number,
                'gl_account': transaction.gl_account,
                'amount': amount,
                'transaction_type': transaction.transaction_type,
                'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None
            })
            
            if transaction.transaction_type == 'DEBIT':
                user_analysis[user]['debit_amount'] += amount
                user_analysis[user]['debit_count'] += 1
            else:  # CREDIT
                user_analysis[user]['credit_amount'] += amount
                user_analysis[user]['credit_count'] += 1
        
        # Calculate balance for each user and convert sets to lists
        for user_data in user_analysis.values():
            user_data['balance'] = user_data['debit_amount'] - user_data['credit_amount']
            user_data['accounts_used'] = list(user_data['accounts_used'])
            user_data['unique_accounts_count'] = len(user_data['accounts_used'])
        
        # Convert to list and sort by total amount
        user_summaries = list(user_analysis.values())
        user_summaries.sort(key=lambda x: x['debit_amount'] + x['credit_amount'], reverse=True)
        
        # Create analysis results
        analysis_results = {
            'trial_balance_summary': {
                'total_debits': float(total_debits),
                'total_credits': float(total_credits),
                'balance': trial_balance,
                'is_balanced': abs(trial_balance) < 0.01,  # Consider balanced if difference is less than 0.01
                'balance_percentage': (abs(trial_balance) / total_amount * 100) if total_amount > 0 else 0
            },
            'gl_account_summaries': gl_account_summaries,
            'user_summaries': user_summaries,
            'statistical_calculations': {
                'average_transaction_amount': float(total_amount / len(transactions)) if transactions else 0,
                'total_transactions': len(transactions),
                'unique_users': unique_users,
                'unique_accounts': unique_accounts,
                'ml_anomalies_detected': len(ml_anomalies),
                'ml_anomaly_percentage': (len(ml_anomalies) / len(transactions)) * 100 if transactions else 0,
                'average_ml_confidence': float(avg_ml_confidence),
                'account_statistics': {
                    'accounts_with_debits_only': len([a for a in gl_account_summaries if a['credit_count'] == 0]),
                    'accounts_with_credits_only': len([a for a in gl_account_summaries if a['debit_count'] == 0]),
                    'accounts_with_both': len([a for a in gl_account_summaries if a['debit_count'] > 0 and a['credit_count'] > 0]),
                    'highest_balance_account': gl_account_summaries[0]['account'] if gl_account_summaries else None,
                    'lowest_balance_account': gl_account_summaries[-1]['account'] if gl_account_summaries else None
                },
                'user_statistics': {
                    'users_with_debits_only': len([u for u in user_summaries if u['credit_count'] == 0]),
                    'users_with_credits_only': len([u for u in user_summaries if u['debit_count'] == 0]),
                    'users_with_both': len([u for u in user_summaries if u['debit_count'] > 0 and u['credit_count'] > 0]),
                    'most_active_user': user_summaries[0]['user'] if user_summaries else None,
                    'highest_amount_user': max(user_summaries, key=lambda x: x['debit_amount'] + x['credit_amount'])['user'] if user_summaries else None
                },
                'ml_insights': {
                    'high_confidence_anomalies': len([r for r in ml_results if r['confidence'] > 0.8]),
                    'medium_confidence_anomalies': len([r for r in ml_results if 0.5 <= r['confidence'] <= 0.8]),
                    'low_confidence_anomalies': len([r for r in ml_results if r['confidence'] < 0.5])
                }
            },
            'chart_data': {
                'account_balances': {a['account']: a['balance'] for a in gl_account_summaries[:20]},  # Top 20 accounts
                'user_activity': {u['user']: u['total_count'] for u in user_summaries[:20]},  # Top 20 users
                'debit_credit_distribution': {
                    'debits': float(total_debits),
                    'credits': float(total_credits)
                },
                'account_transaction_counts': {a['account']: a['total_count'] for a in gl_account_summaries[:20]}
            },
            'export_data': [],
            'ml_results': ml_results,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Cache results
        cache_key = f"general_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        return analysis_results
    
    def run_duplicate_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run duplicate analysis based on business rules
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Duplicate Analysis with Business Rules for {len(transactions)} transactions")
        
        # Define duplicate types based on business rules (ordered from most specific to least specific)
        duplicate_types = {
            'type_6': {
                'name': 'Account Number + Effective Date + Posted Date + User + Source + Amount',
                'fields': ['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount_local_currency'],
                'risk_score': 95
            },
            'type_5': {
                'name': 'Account Number + Effective Date + Amount',
                'fields': ['gl_account', 'document_date', 'amount_local_currency'],
                'risk_score': 90
            },
            'type_4': {
                'name': 'Account Number + Posted Date + Amount',
                'fields': ['gl_account', 'posting_date', 'amount_local_currency'],
                'risk_score': 85
            },
            'type_3': {
                'name': 'Account Number + User + Amount',
                'fields': ['gl_account', 'user_name', 'amount_local_currency'],
                'risk_score': 80
            },
            'type_2': {
                'name': 'Account Number + Source + Amount',
                'fields': ['gl_account', 'source', 'amount_local_currency'],
                'risk_score': 75
            },
            'type_1': {
                'name': 'Account Number + Amount',
                'fields': ['gl_account', 'amount_local_currency'],
                'risk_score': 70
            }
        }
        
        # Find duplicates based on business rules (check most specific first)
        duplicates = []
        duplicate_counts = {duplicate_type: 0 for duplicate_type in duplicate_types.keys()}
        
        # Track which transaction pairs have already been classified to prevent duplicates across types
        classified_pairs = set()
        
        for i, t1 in enumerate(transactions):
            for j, t2 in enumerate(transactions[i+1:], i+1):
                # Create a unique pair identifier
                pair_id = tuple(sorted([str(t1.id), str(t2.id)]))
                
                # Skip if this pair has already been classified
                if pair_id in classified_pairs:
                    continue
                
                duplicate_found = False
                duplicate_type = None
                
                # Check each duplicate type from most specific (type_6) to least specific (type_1)
                for dup_type, config in duplicate_types.items():
                    if self._check_duplicate_criteria(t1, t2, config['fields']):
                        duplicate_found = True
                        duplicate_type = dup_type
                        # Mark this pair as classified so it won't be checked for other types
                        classified_pairs.add(pair_id)
                        break
                
                if duplicate_found:
                    duplicate_counts[duplicate_type] += 1
                    
                    # Calculate similarity score based on matching fields
                    similarity_score = self._calculate_similarity_score(t1, t2, duplicate_types[duplicate_type]['fields'])
                    
                    duplicates.append({
                        'transaction1': {
                            'id': str(t1.id),
                            'document_number': t1.document_number,
                            'amount': float(t1.amount_local_currency),
                            'account': t1.gl_account,
                            'date': t1.posting_date.isoformat() if t1.posting_date else None,
                            'user': t1.user_name,
                            'source': getattr(t1, 'source', ''),
                            'effective_date': t1.document_date.isoformat() if t1.document_date else None
                        },
                        'transaction2': {
                            'id': str(t2.id),
                            'document_number': t2.document_number,
                            'amount': float(t2.amount_local_currency),
                            'account': t2.gl_account,
                            'date': t2.posting_date.isoformat() if t2.posting_date else None,
                            'user': t2.user_name,
                            'source': getattr(t2, 'source', ''),
                            'effective_date': t2.document_date.isoformat() if t2.document_date else None
                        },
                        'duplicate_type': duplicate_type,
                        'duplicate_type_name': duplicate_types[duplicate_type]['name'],
                        'similarity_score': similarity_score,
                        'risk_score': duplicate_types[duplicate_type]['risk_score'],
                        'matching_fields': duplicate_types[duplicate_type]['fields']
                    })
        
        # Group duplicates by type
        duplicates_by_type = {}
        for dup_type, config in duplicate_types.items():
            type_duplicates = [d for d in duplicates if d['duplicate_type'] == dup_type]
            duplicates_by_type[dup_type] = {
                'name': config['name'],
                'count': len(type_duplicates),
                'risk_score': config['risk_score'],
                'duplicates': type_duplicates
            }
        
        # Create analysis results
        analysis_results = {
            'duplicates_found': len(duplicates),
            'duplicate_pairs': duplicates,
            'duplicates_by_type': duplicates_by_type,
            'duplicate_type_summary': {
                'type_1_count': duplicate_counts['type_1'],
                'type_2_count': duplicate_counts['type_2'],
                'type_3_count': duplicate_counts['type_3'],
                'type_4_count': duplicate_counts['type_4'],
                'type_5_count': duplicate_counts['type_5'],
                'type_6_count': duplicate_counts['type_6']
            },
            'audit_recommendations': {
                'high_risk_duplicates': len([d for d in duplicates if d['risk_score'] >= 85]),
                'medium_risk_duplicates': len([d for d in duplicates if 75 <= d['risk_score'] < 85]),
                'low_risk_duplicates': len([d for d in duplicates if d['risk_score'] < 75])
            },
            'compliance_assessment': {
                'total_duplicates': len(duplicates),
                'duplicate_percentage': (len(duplicates) / len(transactions)) * 100 if transactions else 0,
                'duplicate_types_detected': len([k for k, v in duplicate_counts.items() if v > 0])
            },
            'financial_statement_impact': {
                'potential_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates),
                'highest_risk_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates if d['risk_score'] >= 85)
            },
            'chart_data': {
                'duplicate_distribution': duplicate_counts,
                'risk_levels': {
                    'high_risk': len([d for d in duplicates if d['risk_score'] >= 85]),
                    'medium_risk': len([d for d in duplicates if 75 <= d['risk_score'] < 85]),
                    'low_risk': len([d for d in duplicates if d['risk_score'] < 75])
                }
            },
            'export_data': duplicates,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Cache results
        cache_key = f"duplicate_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        # Update SAPGLPosting records with duplicate information
        self._update_transactions_with_duplicate_analysis(transactions, duplicates)
        
        # Cache results
        cache_key = f"duplicate_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        return analysis_results
    
    def _update_transactions_with_duplicate_analysis(self, transactions: List, duplicates: List) -> None:
        """
        Update SAPGLPosting records with duplicate analysis information
        
        Args:
            transactions: List of all transactions
            duplicates: List of duplicate pairs found
        """
        try:
            # Create a set of transaction IDs that are part of duplicates
            duplicate_transaction_ids = set()
            duplicate_details = {}
            
            for duplicate in duplicates:
                t1_id = duplicate['transaction1']['id']
                t2_id = duplicate['transaction2']['id']
                duplicate_transaction_ids.add(t1_id)
                duplicate_transaction_ids.add(t2_id)
                
                # Store duplicate details for each transaction
                duplicate_details[t1_id] = {
                    'duplicate_type': duplicate['duplicate_type'],
                    'duplicate_type_name': duplicate['duplicate_type_name'],
                    'risk_score': duplicate['risk_score'],
                    'similarity_score': duplicate['similarity_score'],
                    'matching_fields': duplicate['matching_fields'],
                    'paired_with': t2_id
                }
                duplicate_details[t2_id] = {
                    'duplicate_type': duplicate['duplicate_type'],
                    'duplicate_type_name': duplicate['duplicate_type_name'],
                    'risk_score': duplicate['risk_score'],
                    'similarity_score': duplicate['similarity_score'],
                    'matching_fields': duplicate['matching_fields'],
                    'paired_with': t1_id
                }
            
            # Update transactions in batches
            from django.db import transaction as db_transaction
            with db_transaction.atomic():
                for transaction in transactions:
                    if str(transaction.id) in duplicate_transaction_ids:
                        details = duplicate_details[str(transaction.id)]
                        transaction.is_duplicate = True
                        transaction.duplicate_type = details['duplicate_type']
                        transaction.duplicate_risk_score = details['risk_score']
                        transaction.duplicate_analysis_details = details
                        transaction.anomaly_types = list(set(transaction.anomaly_types + ['duplicate']))
                    else:
                        # Reset duplicate flags for non-duplicate transactions
                        transaction.is_duplicate = False
                        transaction.duplicate_type = None
                        transaction.duplicate_risk_score = 0.0
                        transaction.duplicate_analysis_details = {}
                    
                    transaction.save()
            
            logger.info(f"Updated {len(duplicate_transaction_ids)} transactions with duplicate analysis information")
            
        except Exception as e:
            logger.error(f"Error updating transactions with duplicate analysis: {e}")
    
    def _update_transactions_with_backdated_analysis(self, transactions: List, backdated_entries: List) -> None:
        """
        Update SAPGLPosting records with backdated analysis information
        
        Args:
            transactions: List of all transactions
            backdated_entries: List of backdated entries found
        """
        try:
            # Create a set of transaction IDs that are backdated
            backdated_transaction_ids = set()
            backdated_details = {}
            
            for entry in backdated_entries:
                t_id = entry['transaction_id']
                backdated_transaction_ids.add(t_id)
                backdated_details[t_id] = {
                    'days_difference': entry['days_difference'],
                    'risk_score': entry['risk_score'],
                    'risk_level': entry['risk_level'],
                    'document_date': entry['document_date'],
                    'posting_date': entry['posting_date']
                }
            
            # Update transactions in batches
            from django.db import transaction as db_transaction
            with db_transaction.atomic():
                for transaction in transactions:
                    if str(transaction.id) in backdated_transaction_ids:
                        details = backdated_details[str(transaction.id)]
                        transaction.is_backdated = True
                        transaction.backdated_days = details['days_difference']
                        transaction.backdated_risk_score = details['risk_score']
                        transaction.backdated_analysis_details = details
                        transaction.anomaly_types = list(set(transaction.anomaly_types + ['backdated']))
                    else:
                        # Reset backdated flags for non-backdated transactions
                        transaction.is_backdated = False
                        transaction.backdated_days = 0
                        transaction.backdated_risk_score = 0.0
                        transaction.backdated_analysis_details = {}
                    
                    transaction.save()
            
            logger.info(f"Updated {len(backdated_transaction_ids)} transactions with backdated analysis information")
            
        except Exception as e:
            logger.error(f"Error updating transactions with backdated analysis: {e}")
    
    def run_backdated_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run backdated analysis based on business rules
        
        Business Rule: Identify all Document numbers for which the Posting Date is after the Effective Date (Document Date).
        Both date fields are required to be present in the GL data for this test.
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Backdated Analysis with Business Rules for {len(transactions)} transactions")
        
        # Find backdated entries based on business rule: posting_date > document_date
        backdated_entries = []
        backdated_by_document = {}
        backdated_by_account = {}
        backdated_by_user = {}
        
        for transaction in transactions:
            # Both date fields must be present (business rule requirement)
            if transaction.document_date and transaction.posting_date:
                days_difference = (transaction.posting_date - transaction.document_date).days
                
                # Check if posting date is after document date (backdated entry)
                if days_difference > 0:
                    # Calculate risk score based on days difference
                    if days_difference > 30:
                        risk_score = 100.0
                        risk_level = 'critical'
                    elif days_difference > 14:
                        risk_score = 85.0
                        risk_level = 'high'
                    elif days_difference > 7:
                        risk_score = 70.0
                        risk_level = 'medium'
                    else:
                        risk_score = 50.0
                        risk_level = 'low'
                    
                    backdated_entry = {
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'document_date': transaction.document_date.isoformat(),
                        'posting_date': transaction.posting_date.isoformat(),
                        'days_difference': days_difference,
                        'amount': float(transaction.amount_local_currency),
                        'account': transaction.gl_account,
                        'user': transaction.user_name,
                        'is_backdated': True,
                        'risk_score': risk_score,
                        'risk_level': risk_level,
                        'transaction_type': transaction.transaction_type,
                        'fiscal_year': transaction.fiscal_year,
                        'posting_period': transaction.posting_period
                    }
                    
                    backdated_entries.append(backdated_entry)
                    
                    # Group by document number
                    if transaction.document_number not in backdated_by_document:
                        backdated_by_document[transaction.document_number] = []
                    backdated_by_document[transaction.document_number].append(backdated_entry)
                    
                    # Group by account
                    if transaction.gl_account not in backdated_by_account:
                        backdated_by_account[transaction.gl_account] = []
                    backdated_by_account[transaction.gl_account].append(backdated_entry)
                    
                    # Group by user
                    if transaction.user_name not in backdated_by_user:
                        backdated_by_user[transaction.user_name] = []
                    backdated_by_user[transaction.user_name].append(backdated_entry)
        
        # Convert grouped data to lists for JSON serialization
        backdated_by_document_list = [
            {
                'document_number': doc_num,
                'entries': entries,
                'count': len(entries),
                'total_amount': sum(e['amount'] for e in entries),
                'max_days_difference': max(e['days_difference'] for e in entries),
                'risk_levels': {
                    'critical': len([e for e in entries if e['risk_level'] == 'critical']),
                    'high': len([e for e in entries if e['risk_level'] == 'high']),
                    'medium': len([e for e in entries if e['risk_level'] == 'medium']),
                    'low': len([e for e in entries if e['risk_level'] == 'low'])
                }
            }
            for doc_num, entries in backdated_by_document.items()
        ]
        
        backdated_by_account_list = [
            {
                'account': account,
                'entries': entries,
                'count': len(entries),
                'total_amount': sum(e['amount'] for e in entries),
                'max_days_difference': max(e['days_difference'] for e in entries),
                'risk_levels': {
                    'critical': len([e for e in entries if e['risk_level'] == 'critical']),
                    'high': len([e for e in entries if e['risk_level'] == 'high']),
                    'medium': len([e for e in entries if e['risk_level'] == 'medium']),
                    'low': len([e for e in entries if e['risk_level'] == 'low'])
                }
            }
            for account, entries in backdated_by_account.items()
        ]
        
        backdated_by_user_list = [
            {
                'user': user,
                'entries': entries,
                'count': len(entries),
                'total_amount': sum(e['amount'] for e in entries),
                'max_days_difference': max(e['days_difference'] for e in entries),
                'risk_levels': {
                    'critical': len([e for e in entries if e['risk_level'] == 'critical']),
                    'high': len([e for e in entries if e['risk_level'] == 'high']),
                    'medium': len([e for e in entries if e['risk_level'] == 'medium']),
                    'low': len([e for e in entries if e['risk_level'] == 'low'])
                }
            }
            for user, entries in backdated_by_user.items()
        ]
        
        # Create analysis results
        analysis_results = {
            'backdated_entries_found': len(backdated_entries),
            'backdated_entries': backdated_entries,
            'backdated_by_document': backdated_by_document_list,
            'backdated_by_account': backdated_by_account_list,
            'backdated_by_user': backdated_by_user_list,
            'audit_recommendations': {
                'critical_risk_backdated': len([b for b in backdated_entries if b['risk_level'] == 'critical']),
                'high_risk_backdated': len([b for b in backdated_entries if b['risk_level'] == 'high']),
                'medium_risk_backdated': len([b for b in backdated_entries if b['risk_level'] == 'medium']),
                'low_risk_backdated': len([b for b in backdated_entries if b['risk_level'] == 'low'])
            },
            'compliance_assessment': {
                'total_backdated': len(backdated_entries),
                'backdated_percentage': (len(backdated_entries) / len(transactions)) * 100 if transactions else 0,
                'documents_with_backdated_entries': len(backdated_by_document),
                'accounts_with_backdated_entries': len(backdated_by_account),
                'users_with_backdated_entries': len(backdated_by_user)
            },
            'financial_statement_impact': {
                'backdated_amount': sum(b['amount'] for b in backdated_entries),
                'critical_risk_amount': sum(b['amount'] for b in backdated_entries if b['risk_level'] == 'critical'),
                'high_risk_amount': sum(b['amount'] for b in backdated_entries if b['risk_level'] == 'high')
            },
            'chart_data': {
                'backdated_distribution': {
                    'by_days_difference': {
                        '1-7_days': len([b for b in backdated_entries if 1 <= b['days_difference'] <= 7]),
                        '8-14_days': len([b for b in backdated_entries if 8 <= b['days_difference'] <= 14]),
                        '15-30_days': len([b for b in backdated_entries if 15 <= b['days_difference'] <= 30]),
                        'over_30_days': len([b for b in backdated_entries if b['days_difference'] > 30])
                    }
                },
                'risk_levels': {
                    'critical': len([b for b in backdated_entries if b['risk_level'] == 'critical']),
                    'high': len([b for b in backdated_entries if b['risk_level'] == 'high']),
                    'medium': len([b for b in backdated_entries if b['risk_level'] == 'medium']),
                    'low': len([b for b in backdated_entries if b['risk_level'] == 'low'])
                }
            },
            'export_data': backdated_entries,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Update SAPGLPosting records with backdated information
        self._update_transactions_with_backdated_analysis(transactions, backdated_entries)
        
        # Cache results
        cache_key = f"backdated_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        return analysis_results
    
    def _check_duplicate_criteria(self, t1, t2, fields: List[str]) -> bool:
        """
        Check if two transactions match the duplicate criteria
        
        Args:
            t1: First transaction
            t2: Second transaction
            fields: List of fields to check for matching
            
        Returns:
            bool: True if transactions match the criteria
        """
        for field in fields:
            if field == 'amount_local_currency':
                if abs(t1.amount_local_currency - t2.amount_local_currency) > 0.01:  # Allow small rounding differences
                    return False
            elif field == 'posting_date':
                if t1.posting_date != t2.posting_date:
                    return False
            elif field == 'document_date':
                if t1.document_date != t2.document_date:
                    return False
            elif field == 'gl_account':
                if t1.gl_account != t2.gl_account:
                    return False
            elif field == 'user_name':
                if t1.user_name != t2.user_name:
                    return False
            elif field == 'source':
                source1 = getattr(t1, 'source', '')
                source2 = getattr(t2, 'source', '')
                if source1 != source2:
                    return False
            else:
                # For any other field, do direct comparison
                if getattr(t1, field, None) != getattr(t2, field, None):
                    return False
        
        return True
    
    def _calculate_similarity_score(self, t1, t2, matching_fields: List[str]) -> float:
        """
        Calculate similarity score based on matching fields
        
        Args:
            t1: First transaction
            t2: Second transaction
            matching_fields: List of fields that matched
            
        Returns:
            float: Similarity score between 0 and 1
        """
        # Define all possible fields for comparison
        all_fields = ['gl_account', 'amount_local_currency', 'posting_date', 'document_date', 
                     'user_name', 'source', 'document_number', 'transaction_type']
        
        # Count matching fields
        matching_count = len(matching_fields)
        total_fields = len(all_fields)
        
        # Calculate base similarity based on matching criteria
        base_similarity = matching_count / total_fields
        
        # Add bonus for exact matches in additional fields
        additional_matches = 0
        for field in all_fields:
            if field not in matching_fields:
                if field == 'amount_local_currency':
                    if abs(t1.amount_local_currency - t2.amount_local_currency) <= 0.01:
                        additional_matches += 1
                elif getattr(t1, field, None) == getattr(t2, field, None):
                    additional_matches += 1
        
        # Calculate final similarity score
        final_similarity = base_similarity + (additional_matches / total_fields) * 0.3
        
        return min(final_similarity, 1.0)  # Cap at 1.0
    
    def run_overall_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run overall analysis with ML model
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Overall Analysis with ML for {len(transactions)} transactions")
        
        # Get results from basic models
        general_results = self.model_manager.predict_with_model('general', transactions)
        duplicate_results = self.model_manager.predict_with_model('duplicate', transactions)
        backdated_results = self.model_manager.predict_with_model('backdated', transactions)
        
        # Get overall analysis predictions
        overall_results = self.model_manager.predict_with_model('overall', transactions, 
                                                               general_results=general_results,
                                                               duplicate_results=duplicate_results,
                                                               backdated_results=backdated_results)
        
        # Calculate statistics
        total_transactions = len(transactions)
        high_risk_count = len([r for r in overall_results if r['risk_level'] in ['HIGH', 'CRITICAL']])
        medium_risk_count = len([r for r in overall_results if r['risk_level'] == 'MEDIUM'])
        low_risk_count = len([r for r in overall_results if r['risk_level'] == 'LOW'])
        
        # Create analysis results
        analysis_results = {
            'total_transactions': total_transactions,
            'risk_distribution': {
                'critical_risk': len([r for r in overall_results if r['risk_level'] == 'CRITICAL']),
                'high_risk': len([r for r in overall_results if r['risk_level'] == 'HIGH']),
                'medium_risk': medium_risk_count,
                'low_risk': low_risk_count
            },
            'overall_risk_score': {
                'average': sum(r['overall_risk_score'] for r in overall_results) / len(overall_results) if overall_results else 0,
                'max': max(r['overall_risk_score'] for r in overall_results) if overall_results else 0,
                'min': min(r['overall_risk_score'] for r in overall_results) if overall_results else 0
            },
            'high_risk_transactions': [
                {
                    'transaction_id': r['transaction_id'],
                    'risk_score': r['overall_risk_score'],
                    'risk_level': r['risk_level'],
                    'recommendations': r['recommendations']
                }
                for r in overall_results if r['risk_level'] in ['HIGH', 'CRITICAL']
            ],
            'risk_insights': {
                'total_high_risk': high_risk_count,
                'high_risk_percentage': (high_risk_count / total_transactions) * 100 if total_transactions > 0 else 0,
                'average_risk_score': sum(r['overall_risk_score'] for r in overall_results) / len(overall_results) if overall_results else 0
            },
            'ml_results': overall_results,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Update SAPGLPosting records with overall risk information
        self._update_transactions_with_overall_analysis(transactions, overall_results)
        
        # Cache results
        cache_key = f"overall_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        return analysis_results
    
    def _update_transactions_with_overall_analysis(self, transactions: List, overall_results: List) -> None:
        """
        Update SAPGLPosting records with overall analysis information
        
        Args:
            transactions: List of all transactions
            overall_results: List of overall analysis results
        """
        try:
            # Create a mapping of transaction ID to overall analysis results
            overall_details = {}
            for result in overall_results:
                t_id = result['transaction_id']
                overall_details[t_id] = {
                    'overall_risk_score': result['overall_risk_score'],
                    'risk_level': result['risk_level'],
                    'recommendations': result.get('recommendations', []),
                    'risk_factors': result.get('risk_factors', {})
                }
            
            # Update transactions in batches
            from django.db import transaction as db_transaction
            with db_transaction.atomic():
                for transaction in transactions:
                    t_id = str(transaction.id)
                    if t_id in overall_details:
                        details = overall_details[t_id]
                        transaction.overall_risk_score = details['overall_risk_score']
                        
                        # Update anomaly summary
                        anomaly_summary = {
                            'overall_risk_score': details['overall_risk_score'],
                            'risk_level': details['risk_level'],
                            'recommendations': details['recommendations'],
                            'risk_factors': details['risk_factors'],
                            'duplicate_info': {
                                'is_duplicate': transaction.is_duplicate,
                                'duplicate_type': transaction.duplicate_type,
                                'duplicate_risk_score': transaction.duplicate_risk_score
                            },
                            'backdated_info': {
                                'is_backdated': transaction.is_backdated,
                                'backdated_days': transaction.backdated_days,
                                'backdated_risk_score': transaction.backdated_risk_score
                            }
                        }
                        transaction.anomaly_analysis_summary = anomaly_summary
                    else:
                        # Reset overall risk for transactions not in results
                        transaction.overall_risk_score = 0.0
                        transaction.anomaly_analysis_summary = {}
                    
                    transaction.save()
            
            logger.info(f"Updated {len(overall_details)} transactions with overall analysis information")
            
        except Exception as e:
            logger.error(f"Error updating transactions with overall analysis: {e}")
    
    def run_risk_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run risk analysis with ML model
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Risk Analysis with ML for {len(transactions)} transactions")
        
        # Get overall analysis results first
        overall_results = self.model_manager.predict_with_model('overall', transactions)
        
        # Get risk analysis predictions
        risk_results = self.model_manager.predict_with_model('risk', transactions, 
                                                            overall_results=overall_results)
        
        # Calculate statistics
        total_transactions = len(transactions)
        risk_classes = {}
        for result in risk_results:
            risk_class = result['risk_class']
            risk_classes[risk_class] = risk_classes.get(risk_class, 0) + 1
        
        # Create analysis results
        analysis_results = {
            'total_transactions': total_transactions,
            'risk_classification': {
                'critical_risk': risk_classes.get(3, 0),
                'high_risk': risk_classes.get(2, 0),
                'medium_risk': risk_classes.get(1, 0),
                'low_risk': risk_classes.get(0, 0)
            },
            'confidence_metrics': {
                'average_confidence': sum(r['confidence'] for r in risk_results) / len(risk_results) if risk_results else 0,
                'high_confidence_predictions': len([r for r in risk_results if r['confidence'] > 0.8]),
                'medium_confidence_predictions': len([r for r in risk_results if 0.5 <= r['confidence'] <= 0.8]),
                'low_confidence_predictions': len([r for r in risk_results if r['confidence'] < 0.5])
            },
            'final_risk_scores': {
                'average': sum(r['final_risk_score'] for r in risk_results) / len(risk_results) if risk_results else 0,
                'max': max(r['final_risk_score'] for r in risk_results) if risk_results else 0,
                'min': min(r['final_risk_score'] for r in risk_results) if risk_results else 0
            },
            'high_risk_transactions': [
                {
                    'transaction_id': r['transaction_id'],
                    'risk_class': r['risk_class'],
                    'confidence': r['confidence'],
                    'final_risk_score': r['final_risk_score']
                }
                for r in risk_results if r['risk_class'] >= 2  # High or Critical
            ],
            'ml_results': risk_results,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Cache results
        cache_key = f"risk_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_results
        
        return analysis_results
    
    def run_comprehensive_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run comprehensive analysis with all ML models
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing all analysis results
        """
        logger.info(f"Running Comprehensive ML Analysis for {len(transactions)} transactions")
        start_time = timezone.now()
        
        # Ensure all models are ready
        if not self.ensure_models_ready(transactions):
            logger.error("Failed to prepare ML models for analysis")
            return {'error': 'Failed to prepare ML models'}
        
        # Run all analyses
        results = {}
        
        # General Analysis
        general_start = timezone.now()
        results['general'] = self.run_general_analysis(transactions)
        results['general']['processing_duration'] = (timezone.now() - general_start).total_seconds()
        
        # Duplicate Analysis
        duplicate_start = timezone.now()
        results['duplicate'] = self.run_duplicate_analysis(transactions)
        results['duplicate']['processing_duration'] = (timezone.now() - duplicate_start).total_seconds()
        
        # Backdated Analysis
        backdated_start = timezone.now()
        results['backdated'] = self.run_backdated_analysis(transactions)
        results['backdated']['processing_duration'] = (timezone.now() - backdated_start).total_seconds()
        
        # Overall Analysis
        overall_start = timezone.now()
        results['overall'] = self.run_overall_analysis(transactions)
        results['overall']['processing_duration'] = (timezone.now() - overall_start).total_seconds()
        
        # Risk Analysis
        risk_start = timezone.now()
        results['risk'] = self.run_risk_analysis(transactions)
        results['risk']['processing_duration'] = (timezone.now() - risk_start).total_seconds()
        
        # Overall processing duration
        total_duration = (timezone.now() - start_time).total_seconds()
        
        # Add summary
        results['summary'] = {
            'total_transactions': len(transactions),
            'total_processing_duration': total_duration,
            'ml_models_used': list(self.model_manager.get_training_status().keys()),
            'analysis_timestamp': timezone.now().isoformat(),
            'cache_hits': len(self._analysis_cache),
            'training_log': self._training_log
        }
        
        logger.info(f"Comprehensive ML Analysis completed in {total_duration:.2f} seconds")
        
        return results
    
    def get_analysis_cache_info(self) -> Dict[str, Any]:
        """Get information about cached analysis results"""
        return {
            'cache_size': len(self._analysis_cache),
            'cache_keys': list(self._analysis_cache.keys()),
            'training_log': self._training_log
        }
    
    def clear_cache(self):
        """Clear analysis cache"""
        self._analysis_cache.clear()
        logger.info("Analysis cache cleared")
    
    def force_retrain_models(self, transactions: List) -> bool:
        """
        Force retraining of all models
        
        Args:
            transactions: List of transactions for training
            
        Returns:
            bool: True if retraining successful
        """
        logger.info("Force retraining all ML models...")
        
        # Clear cache
        self.clear_cache()
        
        # Force retrain each model
        for model_type in ['general', 'duplicate', 'backdated']:
            success = self.model_manager.ensure_model_trained(
                model_type, transactions, force_retrain=True
            )
            if not success:
                logger.error(f"Failed to retrain {model_type} model")
                return False
        
        logger.info("All models retrained successfully")
        return True 