"""
ML Analysis Orchestrator
Centralized system for efficient ML model training and analysis
"""

import os
import django
import uuid
from django.utils import timezone
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date

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
        self._analysis_cache = {}  # Cache for other analyses (not closing entries)
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
        
        # Step 1: Train basic models (general, duplicate, backdated, user, holiday)
        basic_models = ['general', 'duplicate', 'backdated', 'user', 'holiday']
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
            user_results = self.model_manager.predict_with_model('user', transactions)
            holiday_results = self.model_manager.predict_with_model('holiday', transactions)
            
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
                
                # Add risk from user analysis
                user_anomaly_found = any(u.get('transaction_id') == str(t.id) for u in user_results)
                if user_anomaly_found:
                    risk_score += 20.0
                
                # Add risk from holiday analysis
                holiday_anomaly_found = any(h.get('transaction_id') == str(t.id) for h in holiday_results)
                if holiday_anomaly_found:
                    risk_score += 30.0  # Holiday postings are high risk
                
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
            success = overall_model.train(transactions, general_results, duplicate_results, backdated_results, user_results, holiday_results, overall_risk_scores)
            
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
        Run general analysis with ML model
        
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
                'balance_percentage': (abs(trial_balance) / float(total_amount) * 100) if total_amount > 0 else 0
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
        Run comprehensive duplicate analysis based on business rules with enhanced information
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing comprehensive analysis results
        """
        logger.info(f"Running Comprehensive Duplicate Analysis with Business Rules for {len(transactions)} transactions")
        
        # Use enhanced duplicate analyzer for better performance
        try:
            from .enhanced_duplicate_analysis import EnhancedDuplicateAnalyzer
            enhanced_analyzer = EnhancedDuplicateAnalyzer()
            
            logger.info(f"Starting enhanced duplicate analysis for {len(transactions)} transactions...")
            analysis_results = enhanced_analyzer.analyze_duplicates(transactions)
            
            # Cache results
            cache_key = f"duplicate_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
            self._analysis_cache[cache_key] = analysis_results
            
            # Update SAPGLPosting records with duplicate information
            logger.info("Updating transaction records with duplicate analysis results...")
            self._update_transactions_with_duplicate_analysis(transactions, analysis_results.get('duplicate_list', []))
            
            logger.info(f"Enhanced duplicate analysis completed. Found {len(analysis_results.get('duplicate_list', []))} duplicate entries.")
            
            return analysis_results
            
        except ImportError:
            logger.warning("Enhanced duplicate analyzer not available, falling back to basic analysis")
            return self._run_basic_duplicate_analysis(transactions)
        except Exception as e:
            logger.error(f"Enhanced duplicate analysis failed: {e}, falling back to basic analysis")
            return self._run_basic_duplicate_analysis(transactions)
    
    def _run_basic_duplicate_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Fallback basic duplicate analysis using business rules
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing basic analysis results
        """
        logger.info(f"Running Basic Duplicate Analysis for {len(transactions)} transactions")
        
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
        
        # Update SAPGLPosting records with duplicate information
        self._update_transactions_with_duplicate_analysis(transactions, duplicates)
        
        return analysis_results
    
    def _generate_comprehensive_analysis_info(self, transactions: List, duplicates: List) -> Dict[str, Any]:
        """Generate comprehensive analysis information"""
        total_amount = sum(float(t.amount_local_currency) for t in transactions)
        duplicate_amount = sum(d['transaction1']['amount'] for d in duplicates)
        
        return {
            'analysis_id': str(uuid.uuid4()),
            'analysis_date': timezone.now().isoformat(),
            'processing_duration': 0,
            'status': 'COMPLETED',
            'analysis_version': '1.0.0',
            'total_transactions': len(transactions),
            'total_duplicates': len(duplicates),
            'total_amount': total_amount,
            'duplicate_amount': duplicate_amount,
            'duplicate_percentage': (len(duplicates) / len(transactions)) * 100 if transactions else 0
        }
    
    def _generate_comprehensive_breakdowns(self, duplicates: List, transactions: List) -> Dict[str, Any]:
        """Generate comprehensive breakdowns"""
        # Risk distribution
        risk_distribution = {
            'Critical': len([d for d in duplicates if d['risk_score'] >= 90]),
            'High': len([d for d in duplicates if 80 <= d['risk_score'] < 90]),
            'Medium': len([d for d in duplicates if 70 <= d['risk_score'] < 80]),
            'Low': len([d for d in duplicates if d['risk_score'] < 70])
        }
        
        # Compliance issues
        compliance_issues = []
        high_risk_duplicates = [d for d in duplicates if d['risk_score'] >= 80]
        large_amount_duplicates = [d for d in duplicates if d['transaction1']['amount'] > 1000000]
        
        if high_risk_duplicates:
            compliance_issues.append({
                'type': 'high_risk_duplicates',
                'severity': 'HIGH',
                'description': f'Found {len(high_risk_duplicates)} high-risk duplicate transactions',
                'count': len(high_risk_duplicates),
                'total_amount': sum(d['transaction1']['amount'] for d in high_risk_duplicates)
            })
        
        if large_amount_duplicates:
            compliance_issues.append({
                'type': 'large_amount_duplicates',
                'severity': 'HIGH',
                'description': f'Found {len(large_amount_duplicates)} duplicate transactions with amounts over 1M SAR',
                'count': len(large_amount_duplicates),
                'total_amount': sum(d['transaction1']['amount'] for d in large_amount_duplicates)
            })
        
        # High priority recommendations
        high_priority_recommendations = []
        
        critical_duplicates = [d for d in duplicates if d['risk_score'] >= 90]
        if critical_duplicates:
            high_priority_recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Immediate investigation required',
                'description': f'Found {len(critical_duplicates)} critical-risk duplicate transactions',
                'count': len(critical_duplicates),
                'total_amount': sum(d['transaction1']['amount'] for d in critical_duplicates),
                'recommendation': 'Review and investigate these transactions immediately for potential fraud or errors'
            })
        
        high_risk_duplicates = [d for d in duplicates if 80 <= d['risk_score'] < 90]
        if high_risk_duplicates:
            high_priority_recommendations.append({
                'priority': 'HIGH',
                'action': 'Priority investigation',
                'description': f'Found {len(high_risk_duplicates)} high-risk duplicate transactions',
                'count': len(high_risk_duplicates),
                'total_amount': sum(d['transaction1']['amount'] for d in high_risk_duplicates),
                'recommendation': 'Investigate these transactions within 48 hours'
            })
        
        if large_amount_duplicates:
            high_priority_recommendations.append({
                'priority': 'HIGH',
                'action': 'Large amount duplicate review',
                'description': f'Found {len(large_amount_duplicates)} duplicate transactions with amounts over 1M SAR',
                'count': len(large_amount_duplicates),
                'total_amount': sum(d['transaction1']['amount'] for d in large_amount_duplicates),
                'recommendation': 'Review these large amount duplicates for potential financial statement impact'
            })
        
        return {
            'duplicate_by_user': [],
            'duplicate_by_account': [],
            'audit_recommendations': {
                'low_risk_duplicates': len([d for d in duplicates if d['risk_score'] < 75]),
                'high_risk_duplicates': len([d for d in duplicates if d['risk_score'] >= 85]),
                'medium_risk_duplicates': len([d for d in duplicates if 75 <= d['risk_score'] < 85])
            },
            'compliance_assessment': {
                'total_duplicates': len(duplicates),
                'duplicate_percentage': (len(duplicates) / len(transactions)) * 100 if transactions else 0,
                'duplicate_types_detected': len(set(d['duplicate_type'] for d in duplicates))
            },
            'duplicate_by_document': [],
            'financial_statement_impact': {
                'potential_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates),
                'highest_risk_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates if d['risk_score'] >= 85)
            },
            'risk_distribution': risk_distribution,
            'compliance_issues': compliance_issues,
            'high_priority_recommendations': high_priority_recommendations
        }
    
    def _generate_comprehensive_chart_data(self, duplicates: List, duplicate_counts: Dict) -> Dict[str, Any]:
        """Generate comprehensive chart data"""
        # Risk levels chart
        risk_levels = {
            'low_risk': len([d for d in duplicates if d['risk_score'] < 75]),
            'high_risk': len([d for d in duplicates if d['risk_score'] >= 85]),
            'medium_risk': len([d for d in duplicates if 75 <= d['risk_score'] < 85])
        }
        
        # Duplicate distribution chart
        duplicate_distribution = {
            'type_1': duplicate_counts.get('type_1', 0),
            'type_2': duplicate_counts.get('type_2', 0),
            'type_3': duplicate_counts.get('type_3', 0),
            'type_4': duplicate_counts.get('type_4', 0),
            'type_5': duplicate_counts.get('type_5', 0),
            'type_6': duplicate_counts.get('type_6', 0)
        }
        
        return {
            'risk_levels': risk_levels,
            'duplicate_distribution': duplicate_distribution
        }
    
    def _generate_detailed_insights(self, duplicates: List, transactions: List) -> Dict[str, Any]:
        """Generate detailed insights"""
        # Analyze duplicate patterns
        duplicate_patterns = {
            'duplicate_entries': duplicates,
            'duplicate_patterns': {
                'duplicate_by_user': [],
                'duplicate_by_account': [],
                'audit_recommendations': {
                    'low_risk_duplicates': len([d for d in duplicates if d['risk_score'] < 75]),
                    'high_risk_duplicates': len([d for d in duplicates if d['risk_score'] >= 85]),
                    'medium_risk_duplicates': len([d for d in duplicates if 75 <= d['risk_score'] < 85])
                },
                'compliance_assessment': {
                    'total_duplicates': len(duplicates),
                    'duplicate_percentage': (len(duplicates) / len(transactions)) * 100 if transactions else 0,
                    'duplicate_types_detected': len(set(d['duplicate_type'] for d in duplicates))
                },
                'duplicate_by_document': [],
                'financial_statement_impact': {
                    'potential_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates),
                    'highest_risk_duplicate_amount': sum(d['transaction1']['amount'] for d in duplicates if d['risk_score'] >= 85)
                }
            },
            'audit_recommendations': {},
            'detection_methods': [],
            'confidence_scores': {},
            'false_positive_indicators': []
        }
        
        return duplicate_patterns
    
    def _generate_slicer_filters(self, duplicates: List, transactions: List) -> Dict[str, Any]:
        """Generate slicer filters"""
        return {}
    
    def _generate_summary_table(self, duplicates: List) -> List:
        """Generate summary table"""
        return []
    
    def _update_transactions_with_duplicate_analysis(self, transactions: List, duplicates: List) -> None:
        """
        Update SAPGLPosting records with duplicate analysis information
        
        Args:
            transactions: List of all transactions
            duplicates: List of duplicate entries found (from enhanced analyzer)
        """
        try:
            # Create a set of transaction IDs that are part of duplicates
            duplicate_transaction_ids = set()
            duplicate_details = {}
            
            for duplicate in duplicates:
                t_id = duplicate['transaction_id']
                duplicate_transaction_ids.add(t_id)
                
                # Store duplicate details for each transaction
                duplicate_details[t_id] = {
                    'duplicate_type': duplicate['duplicate_type'],
                    'duplicate_criteria': duplicate['duplicate_criteria'],
                    'risk_score': duplicate['risk_score'],
                    'duplicate_count': duplicate['duplicate_count'],
                    'group_total_amount': duplicate['group_total_amount'],
                    'unique_users_in_group': duplicate['unique_users_in_group'],
                    'unique_documents_in_group': duplicate['unique_documents_in_group']
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
    
    def run_unusual_days_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run unusual days analysis with comprehensive weekend and unusual day detection
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Unusual Days Analysis for {len(transactions)} transactions")
        
        # Weekend days (Friday=4, Saturday=5)
        weekend_days = [4, 5]
        
        # 1. Weekend Postings Analysis
        weekend_postings = []
        for transaction in transactions:
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                if day_of_week in weekend_days:
                    weekend_postings.append({
                        'transaction_id': str(transaction.id),
                        'document_number': transaction.document_number,
                        'gl_account': transaction.gl_account,
                        'amount': float(transaction.amount_local_currency),
                        'user_name': transaction.user_name,
                        'posting_date': transaction.posting_date.isoformat(),
                        'day_of_week': day_of_week,
                        'day_name': self._get_day_name(day_of_week),
                        'is_weekend': True,
                        'risk_score': self._calculate_weekend_risk_score(transaction),
                        'transaction_type': transaction.transaction_type,
                        'fiscal_year': transaction.fiscal_year,
                        'posting_period': transaction.posting_period
                    })
        
        # 2. Day of Week Activity Analysis
        day_activity = {}
        for day in range(7):  # 0=Monday, 1=Tuesday, ..., 6=Sunday
            day_transactions = [t for t in transactions if t.posting_date and t.posting_date.weekday() == day]
            
            if day_transactions:
                amounts = [float(t.amount_local_currency) for t in day_transactions]
                debit_transactions = [t for t in day_transactions if t.transaction_type == 'DEBIT']
                credit_transactions = [t for t in day_transactions if t.transaction_type == 'CREDIT']
                
                day_activity[day] = {
                    'day_of_week': day,
                    'day_name': self._get_day_name(day),
                    'total_transactions': len(day_transactions),
                    'total_amount': sum(amounts),
                    'average_amount': sum(amounts) / len(amounts) if amounts else 0,
                    'max_amount': max(amounts) if amounts else 0,
                    'min_amount': min(amounts) if amounts else 0,
                    'debit_count': len(debit_transactions),
                    'credit_count': len(credit_transactions),
                    'debit_amount': sum(float(t.amount_local_currency) for t in debit_transactions),
                    'credit_amount': sum(float(t.amount_local_currency) for t in credit_transactions),
                    'unique_users': len(set(t.user_name for t in day_transactions)),
                    'unique_accounts': len(set(t.gl_account for t in day_transactions)),
                    'is_weekend': day in weekend_days
                }
        
        # 3. User Day Patterns
        user_patterns = {}
        for transaction in transactions:
            user = transaction.user_name
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                
                if user not in user_patterns:
                    user_patterns[user] = {
                        'user_name': user,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'day_breakdown': {i: {'count': 0, 'amount': 0} for i in range(7)},
                        'weekend_transactions': 0,
                        'weekend_amount': 0
                    }
                
                user_patterns[user]['total_transactions'] += 1
                user_patterns[user]['total_amount'] += float(transaction.amount_local_currency)
                user_patterns[user]['day_breakdown'][day_of_week]['count'] += 1
                user_patterns[user]['day_breakdown'][day_of_week]['amount'] += float(transaction.amount_local_currency)
                
                if day_of_week in weekend_days:
                    user_patterns[user]['weekend_transactions'] += 1
                    user_patterns[user]['weekend_amount'] += float(transaction.amount_local_currency)
        
        # 4. FS Line Day Patterns
        fs_line_patterns = {}
        for transaction in transactions:
            if transaction.posting_date:
                day_of_week = transaction.posting_date.weekday()
                fs_line = self._get_fs_line_from_account(transaction.gl_account)
                
                if fs_line not in fs_line_patterns:
                    fs_line_patterns[fs_line] = {
                        'fs_line': fs_line,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'day_breakdown': {i: {'count': 0, 'amount': 0} for i in range(7)},
                        'weekend_transactions': 0,
                        'weekend_amount': 0
                    }
                
                fs_line_patterns[fs_line]['total_transactions'] += 1
                fs_line_patterns[fs_line]['total_amount'] += float(transaction.amount_local_currency)
                fs_line_patterns[fs_line]['day_breakdown'][day_of_week]['count'] += 1
                fs_line_patterns[fs_line]['day_breakdown'][day_of_week]['amount'] += float(transaction.amount_local_currency)
                
                if day_of_week in weekend_days:
                    fs_line_patterns[fs_line]['weekend_transactions'] += 1
                    fs_line_patterns[fs_line]['weekend_amount'] += float(transaction.amount_local_currency)
        
        # 5. Unusual Day Detection
        unusual_days = []
        if day_activity:
            total_transactions = sum(day['total_transactions'] for day in day_activity.values())
            avg_transactions_per_day = total_transactions / len(day_activity) if day_activity else 0
            
            for day_info in day_activity.values():
                day_transactions = day_info['total_transactions']
                
                if avg_transactions_per_day > 0:
                    deviation = abs(day_transactions - avg_transactions_per_day) / avg_transactions_per_day
                    
                    if deviation > 0.1:  # 10% threshold
                        unusual_days.append({
                            'day_of_week': day_info['day_of_week'],
                            'day_name': day_info['day_name'],
                            'transaction_count': day_transactions,
                            'average_transactions': avg_transactions_per_day,
                            'deviation_percentage': deviation * 100,
                            'is_weekend': day_info['is_weekend'],
                            'unusual_type': 'high_activity' if day_transactions > avg_transactions_per_day else 'low_activity'
                        })
        
        # 6. Risk Assessment
        total_transactions = len(transactions)
        weekend_count = len(weekend_postings)
        unusual_count = len(unusual_days)
        
        weekend_risk = (weekend_count / total_transactions * 100) if total_transactions > 0 else 0
        unusual_pattern_risk = (unusual_count / 7 * 100) if unusual_count > 0 else 0
        
        # High value weekend transactions
        high_value_weekend = [p for p in weekend_postings if p['amount'] > 1000000]
        high_value_risk = (len(high_value_weekend) / total_transactions * 100) if total_transactions > 0 else 0
        
        # Overall risk assessment
        overall_risk_score = (
            weekend_risk * 0.4 +
            unusual_pattern_risk * 0.3 +
            high_value_risk * 0.3
        )
        
        risk_assessment = {
            'overall_risk_score': overall_risk_score,
            'weekend_risk_score': weekend_risk,
            'unusual_pattern_risk_score': unusual_pattern_risk,
            'high_value_weekend_risk_score': high_value_risk,
            'risk_level': self._get_risk_level(overall_risk_score),
            'recommendations': self._get_weekend_risk_recommendations(overall_risk_score, weekend_count, unusual_count)
        }
        
        # 7. Chart Data
        chart_data = {}
        if day_activity:
            days = list(day_activity.keys())
            day_names = [day_activity[day]['day_name'] for day in days]
            transaction_counts = [day_activity[day]['total_transactions'] for day in days]
            amounts = [day_activity[day]['total_amount'] for day in days]
            
            chart_data['gl_activity_by_day'] = {
                'labels': day_names,
                'transaction_counts': transaction_counts,
                'amounts': amounts,
                'weekend_days': [i for i, day in enumerate(days) if day_activity[day]['is_weekend']]
            }
        
        if user_patterns:
            chart_data['user_day_patterns'] = {
                'users': [p['user_name'] for p in user_patterns.values()],
                'weekend_transactions': [p['weekend_transactions'] for p in user_patterns.values()],
                'weekend_amounts': [p['weekend_amount'] for p in user_patterns.values()],
                'total_transactions': [p['total_transactions'] for p in user_patterns.values()]
            }
        
        if fs_line_patterns:
            chart_data['fs_line_day_patterns'] = {
                'fs_lines': [p['fs_line'] for p in fs_line_patterns.values()],
                'weekend_transactions': [p['weekend_transactions'] for p in fs_line_patterns.values()],
                'weekend_amounts': [p['weekend_amount'] for p in fs_line_patterns.values()],
                'total_transactions': [p['total_transactions'] for p in fs_line_patterns.values()]
            }
        
        # 8. Export Data
        export_data = []
        for posting in weekend_postings:
            export_data.append({
                'type': 'weekend_posting',
                'transaction_id': posting['transaction_id'],
                'document_number': posting['document_number'],
                'gl_account': posting['gl_account'],
                'amount': posting['amount'],
                'user_name': posting['user_name'],
                'posting_date': posting['posting_date'],
                'day_name': posting['day_name'],
                'risk_score': posting['risk_score']
            })
        
        for unusual in unusual_days:
            export_data.append({
                'type': 'unusual_day',
                'day_name': unusual['day_name'],
                'transaction_count': unusual['transaction_count'],
                'deviation_percentage': unusual['deviation_percentage'],
                'unusual_type': unusual['unusual_type']
            })
        
        # Create analysis result
        analysis_result = {
            'analysis_info': {
                'total_transactions': total_transactions,
                'weekend_transactions': weekend_count,
                'unusual_days_detected': unusual_count,
                'analysis_date': timezone.now().isoformat(),
                'processing_duration': 0
            },
            'weekend_postings': weekend_postings,
            'day_of_week_activity': day_activity,
            'user_day_patterns': list(user_patterns.values()),
            'fs_line_day_patterns': list(fs_line_patterns.values()),
            'unusual_days': unusual_days,
            'risk_assessment': risk_assessment,
            'chart_data': chart_data,
            'export_data': export_data
        }
        
        # Cache results
        cache_key = f"unusual_days_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_result
        
        return analysis_result
    
    def run_holiday_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run holiday analysis with comprehensive holiday detection using holiday_utils
        
        This test identifies Journal Entries posted on days identified as holidays by the Audit Practitioner(s).
        Holiday dates are predefined using the holiday_utils module with Saudi Arabian holidays as default.
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Holiday Analysis for {len(transactions)} transactions")
        
        # Get data file information for date range and country
        data_file = None
        if transactions:
            data_file = transactions[0].data_file
        
        # Default to Saudi Arabian holidays
        country_code = 'saudiarabian'
        start_date = None
        end_date = None
        
        if data_file:
            # Use fiscal year dates from data file
            if data_file.audit_start_date and data_file.audit_end_date:
                start_date = data_file.audit_start_date
                end_date = data_file.audit_end_date
            else:
                # Fallback to fiscal year
                start_date = date(data_file.fiscal_year, 1, 1)
                end_date = date(data_file.fiscal_year, 12, 31)
        
        # Import holiday utilities
        from .holiday_utils import get_holidays, is_holiday
        
        # Get holidays for the date range
        try:
            holidays = get_holidays(country_code, start_date, end_date, include_observances=True)
            holiday_dates = {h.date for h in holidays}
            holiday_info = {h.date: {'name': h.name, 'type': h.holiday_type} for h in holidays}
            logger.info(f"Retrieved {len(holidays)} holidays for {country_code} from {start_date} to {end_date}")
        except Exception as e:
            logger.error(f"Error retrieving holidays: {e}")
            holidays = []
            holiday_dates = set()
            holiday_info = {}
        
        # 1. Holiday Postings Detection
        holiday_postings = []
        for transaction in transactions:
            if transaction.posting_date and transaction.posting_date in holiday_dates:
                holiday_data = holiday_info.get(transaction.posting_date, {})
                
                # Calculate risk score based on holiday type and transaction characteristics
                risk_score = self._calculate_holiday_risk_score(transaction, holiday_data)
                
                holiday_posting = {
                    'transaction_id': str(transaction.id),
                    'document_number': transaction.document_number,
                    'gl_account': transaction.gl_account,
                    'amount': float(transaction.amount_local_currency),
                    'user_name': transaction.user_name,
                    'posting_date': transaction.posting_date.isoformat(),
                    'holiday_name': holiday_data.get('name', 'Unknown Holiday'),
                    'holiday_type': holiday_data.get('type', 'Unknown'),
                    'is_holiday': True,
                    'risk_score': risk_score,
                    'risk_level': self._get_risk_level(risk_score),
                    'transaction_type': transaction.transaction_type,
                    'fiscal_year': transaction.fiscal_year,
                    'posting_period': transaction.posting_period,
                    'fs_line': self._get_fs_line_from_account(transaction.gl_account)
                }
                
                holiday_postings.append(holiday_posting)
        
        # 2. Holiday Postings by FS Line
        fs_line_holiday = {}
        for posting in holiday_postings:
            fs_line = posting['fs_line']
            if fs_line not in fs_line_holiday:
                fs_line_holiday[fs_line] = {
                    'fs_line': fs_line,
                    'total_holiday_postings': 0,
                    'total_amount': 0,
                    'unique_users': set(),
                    'unique_accounts': set(),
                    'holiday_types': set(),
                    'entries': []
                }
            
            fs_line_holiday[fs_line]['total_holiday_postings'] += 1
            fs_line_holiday[fs_line]['total_amount'] += posting['amount']
            fs_line_holiday[fs_line]['unique_users'].add(posting['user_name'])
            fs_line_holiday[fs_line]['unique_accounts'].add(posting['gl_account'])
            fs_line_holiday[fs_line]['holiday_types'].add(posting['holiday_type'])
            fs_line_holiday[fs_line]['entries'].append(posting)
        
        # Convert sets to lists for JSON serialization
        for fs_line in fs_line_holiday:
            fs_line_holiday[fs_line]['unique_users'] = list(fs_line_holiday[fs_line]['unique_users'])
            fs_line_holiday[fs_line]['unique_accounts'] = list(fs_line_holiday[fs_line]['unique_accounts'])
            fs_line_holiday[fs_line]['holiday_types'] = list(fs_line_holiday[fs_line]['holiday_types'])
        
        # 3. Holiday Postings by Account
        account_holiday = {}
        for posting in holiday_postings:
            account = posting['gl_account']
            if account not in account_holiday:
                account_holiday[account] = {
                    'account': account,
                    'total_holiday_postings': 0,
                    'total_amount': 0,
                    'unique_users': set(),
                    'holiday_types': set(),
                    'entries': []
                }
            
            account_holiday[account]['total_holiday_postings'] += 1
            account_holiday[account]['total_amount'] += posting['amount']
            account_holiday[account]['unique_users'].add(posting['user_name'])
            account_holiday[account]['holiday_types'].add(posting['holiday_type'])
            account_holiday[account]['entries'].append(posting)
        
        # Convert sets to lists for JSON serialization
        for account in account_holiday:
            account_holiday[account]['unique_users'] = list(account_holiday[account]['unique_users'])
            account_holiday[account]['holiday_types'] = list(account_holiday[account]['holiday_types'])
        
        # 4. Holiday Postings by User
        user_holiday = {}
        for posting in holiday_postings:
            user = posting['user_name']
            if user not in user_holiday:
                user_holiday[user] = {
                    'user': user,
                    'total_holiday_postings': 0,
                    'total_amount': 0,
                    'unique_accounts': set(),
                    'holiday_types': set(),
                    'entries': []
                }
            
            user_holiday[user]['total_holiday_postings'] += 1
            user_holiday[user]['total_amount'] += posting['amount']
            user_holiday[user]['unique_accounts'].add(posting['gl_account'])
            user_holiday[user]['holiday_types'].add(posting['holiday_type'])
            user_holiday[user]['entries'].append(posting)
        
        # Convert sets to lists for JSON serialization
        for user in user_holiday:
            user_holiday[user]['unique_accounts'] = list(user_holiday[user]['unique_accounts'])
            user_holiday[user]['holiday_types'] = list(user_holiday[user]['holiday_types'])
        
        # 5. GL Activity by Holiday
        gl_activity_by_holiday = {}
        for posting in holiday_postings:
            holiday_name = posting['holiday_name']
            if holiday_name not in gl_activity_by_holiday:
                gl_activity_by_holiday[holiday_name] = {
                    'holiday_name': holiday_name,
                    'holiday_type': posting['holiday_type'],
                    'total_transactions': 0,
                    'total_amount': 0,
                    'unique_users': set(),
                    'unique_accounts': set(),
                    'unique_fs_lines': set(),
                    'entries': []
                }
            
            gl_activity_by_holiday[holiday_name]['total_transactions'] += 1
            gl_activity_by_holiday[holiday_name]['total_amount'] += posting['amount']
            gl_activity_by_holiday[holiday_name]['unique_users'].add(posting['user_name'])
            gl_activity_by_holiday[holiday_name]['unique_accounts'].add(posting['gl_account'])
            gl_activity_by_holiday[holiday_name]['unique_fs_lines'].add(posting['fs_line'])
            gl_activity_by_holiday[holiday_name]['entries'].append(posting)
        
        # Convert sets to lists for JSON serialization
        for holiday_name in gl_activity_by_holiday:
            gl_activity_by_holiday[holiday_name]['unique_users'] = list(gl_activity_by_holiday[holiday_name]['unique_users'])
            gl_activity_by_holiday[holiday_name]['unique_accounts'] = list(gl_activity_by_holiday[holiday_name]['unique_accounts'])
            gl_activity_by_holiday[holiday_name]['unique_fs_lines'] = list(gl_activity_by_holiday[holiday_name]['unique_fs_lines'])
        
        # 6. Risk Assessment
        total_transactions = len(transactions)
        holiday_count = len(holiday_postings)
        high_value_holiday = [p for p in holiday_postings if p['amount'] > 1000000]
        
        holiday_risk = (holiday_count / total_transactions * 100) if total_transactions > 0 else 0
        high_value_holiday_risk = (len(high_value_holiday) / total_transactions * 100) if total_transactions > 0 else 0
        
        # Unusual holiday patterns
        unusual_pattern_risk = 0
        if holiday_count > 0:
            # Check for unusual patterns like multiple users on same holiday
            holiday_user_counts = {}
            for posting in holiday_postings:
                holiday = posting['holiday_name']
                user = posting['user_name']
                if holiday not in holiday_user_counts:
                    holiday_user_counts[holiday] = set()
                holiday_user_counts[holiday].add(user)
            
            # Calculate risk based on multiple users on same holiday
            for holiday, users in holiday_user_counts.items():
                if len(users) > 3:  # More than 3 users on same holiday
                    unusual_pattern_risk += 10
        
        # Overall risk assessment
        overall_risk_score = (
            holiday_risk * 0.4 +
            high_value_holiday_risk * 0.4 +
            unusual_pattern_risk * 0.2
        )
        
        risk_assessment = {
            'overall_risk_score': overall_risk_score,
            'holiday_risk_score': holiday_risk,
            'high_value_holiday_risk_score': high_value_holiday_risk,
            'unusual_pattern_risk_score': unusual_pattern_risk,
            'risk_level': self._get_risk_level(overall_risk_score),
            'recommendations': self._get_holiday_risk_recommendations(overall_risk_score, holiday_count, len(high_value_holiday))
        }
        
        # 7. Chart Data
        chart_data = {}
        
        # Holiday postings by FS line
        if fs_line_holiday:
            fs_lines = list(fs_line_holiday.keys())
            holiday_counts = [fs_line_holiday[fs]['total_holiday_postings'] for fs in fs_lines]
            holiday_amounts = [fs_line_holiday[fs]['total_amount'] for fs in fs_lines]
            
            chart_data['holiday_by_fs_line'] = {
                'labels': fs_lines,
                'counts': holiday_counts,
                'amounts': holiday_amounts
            }
        
        # Holiday postings by account
        if account_holiday:
            accounts = list(account_holiday.keys())
            holiday_counts = [account_holiday[acc]['total_holiday_postings'] for acc in accounts]
            holiday_amounts = [account_holiday[acc]['total_amount'] for acc in accounts]
            
            chart_data['holiday_by_account'] = {
                'labels': accounts,
                'counts': holiday_counts,
                'amounts': holiday_amounts
            }
        
        # Holiday postings by user
        if user_holiday:
            users = list(user_holiday.keys())
            holiday_counts = [user_holiday[user]['total_holiday_postings'] for user in users]
            holiday_amounts = [user_holiday[user]['total_amount'] for user in users]
            
            chart_data['holiday_by_user'] = {
                'labels': users,
                'counts': holiday_counts,
                'amounts': holiday_amounts
            }
        
        # GL Activity by Holiday
        if gl_activity_by_holiday:
            holidays = list(gl_activity_by_holiday.keys())
            transaction_counts = [gl_activity_by_holiday[h]['total_transactions'] for h in holidays]
            amounts = [gl_activity_by_holiday[h]['total_amount'] for h in holidays]
            
            chart_data['gl_activity_by_holiday'] = {
                'labels': holidays,
                'transaction_counts': transaction_counts,
                'amounts': amounts
            }
        
        # 8. Export Data
        export_data = []
        
        for posting in holiday_postings:
            export_data.append({
                'type': 'holiday_posting',
                'transaction_id': posting['transaction_id'],
                'document_number': posting['document_number'],
                'gl_account': posting['gl_account'],
                'amount': posting['amount'],
                'user_name': posting['user_name'],
                'posting_date': posting['posting_date'],
                'holiday_name': posting['holiday_name'],
                'holiday_type': posting['holiday_type'],
                'risk_score': posting['risk_score'],
                'fs_line': posting['fs_line']
            })
        
        # Create analysis result
        analysis_result = {
            'analysis_info': {
                'total_transactions': total_transactions,
                'holiday_postings_count': holiday_count,
                'holiday_percentage': (holiday_count / total_transactions * 100) if total_transactions > 0 else 0,
                'unique_holidays': len(set(p['holiday_name'] for p in holiday_postings)),
                'country_code': country_code,
                'fiscal_year': data_file.fiscal_year if data_file else None,
                'start_date': start_date.isoformat() if start_date else None,
                'end_date': end_date.isoformat() if end_date else None,
                'analysis_date': timezone.now().isoformat(),
                'processing_duration': 0
            },
            'holiday_postings': holiday_postings,
            'holiday_by_fs_line': list(fs_line_holiday.values()),
            'holiday_by_account': list(account_holiday.values()),
            'holiday_by_user': list(user_holiday.values()),
            'gl_activity_by_holiday': gl_activity_by_holiday,
            'risk_assessment': risk_assessment,
            'chart_data': chart_data,
            'export_data': export_data
        }
        
        # Update SAPGLPosting records with holiday information
        self._update_transactions_with_holiday_analysis(transactions, holiday_postings)
        
        # Cache results
        cache_key = f"holiday_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
        self._analysis_cache[cache_key] = analysis_result
        
        return analysis_result
    
    def _calculate_holiday_risk_score(self, transaction, holiday_data: Dict) -> float:
        """Calculate risk score for holiday posting"""
        risk_score = 0.0
        
        # Base risk for holiday posting
        risk_score += 30.0
        
        # Risk based on holiday type
        holiday_type = holiday_data.get('type', '')
        if 'Public holiday' in holiday_type:
            risk_score += 20.0  # Higher risk for public holidays
        elif 'Observance' in holiday_type:
            risk_score += 10.0  # Lower risk for observances
        
        # Risk based on transaction amount
        amount = float(transaction.amount_local_currency)
        if amount > 1000000:  # Over 1M SAR
            risk_score += 30.0
        elif amount > 100000:  # Over 100K SAR
            risk_score += 15.0
        
        # Risk based on account type
        if transaction.is_expense_account:
            risk_score += 10.0  # Higher risk for expense accounts
        
        # Risk based on user activity
        # This could be enhanced with user behavior analysis
        
        return min(risk_score, 100.0)  # Cap at 100
    
    def _get_holiday_risk_recommendations(self, risk_score: float, holiday_count: int, high_value_count: int) -> List[str]:
        """Get risk-based recommendations for holiday analysis"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.append("CRITICAL: Immediate investigation required for holiday postings")
            recommendations.append("Review all high-value holiday transactions for potential fraud")
            recommendations.append("Implement additional controls for holiday period transactions")
        elif risk_score >= 60:
            recommendations.append("HIGH: Priority investigation of holiday postings recommended")
            recommendations.append("Review holiday posting patterns and user behavior")
            recommendations.append("Consider implementing holiday-specific approval workflows")
        elif risk_score >= 30:
            recommendations.append("MEDIUM: Monitor holiday posting patterns")
            recommendations.append("Review high-value holiday transactions")
            recommendations.append("Consider holiday-specific controls")
        else:
            recommendations.append("LOW: Normal holiday posting activity detected")
            recommendations.append("Continue monitoring for unusual patterns")
        
        if holiday_count > 0:
            recommendations.append(f"Found {holiday_count} transactions posted on holidays")
        
        if high_value_count > 0:
            recommendations.append(f"Found {high_value_count} high-value transactions on holidays")
        
        return recommendations
    
    def _update_transactions_with_holiday_analysis(self, transactions: List, holiday_postings: List) -> None:
        """
        Update SAPGLPosting records with holiday analysis information
        
        Args:
            transactions: List of all transactions
            holiday_postings: List of holiday postings found
        """
        try:
            # Create a set of transaction IDs that are holiday postings
            holiday_transaction_ids = set()
            holiday_details = {}
            
            for posting in holiday_postings:
                t_id = posting['transaction_id']
                holiday_transaction_ids.add(t_id)
                holiday_details[t_id] = {
                    'holiday_name': posting['holiday_name'],
                    'holiday_type': posting['holiday_type'],
                    'risk_score': posting['risk_score'],
                    'risk_level': posting['risk_level']
                }
            
            # Update transactions in batches
            from django.db import transaction as db_transaction
            with db_transaction.atomic():
                for transaction in transactions:
                    if str(transaction.id) in holiday_transaction_ids:
                        details = holiday_details[str(transaction.id)]
                        transaction.is_holiday_posting = True
                        transaction.holiday_name = details['holiday_name']
                        transaction.holiday_type = details['holiday_type']
                        transaction.holiday_risk_score = details['risk_score']
                        transaction.holiday_analysis_details = details
                        transaction.anomaly_types = list(set(transaction.anomaly_types + ['holiday']))
                    else:
                        # Reset holiday flags for non-holiday transactions
                        transaction.is_holiday_posting = False
                        transaction.holiday_name = None
                        transaction.holiday_type = None
                        transaction.holiday_risk_score = 0.0
                        transaction.holiday_analysis_details = {}
                    
                    transaction.save()
            
            logger.info(f"Updated {len(holiday_transaction_ids)} transactions with holiday analysis information")
            
        except Exception as e:
            logger.error(f"Error updating transactions with holiday analysis: {e}")
    
    def run_closing_entries_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run closing entries analysis with comprehensive month-end closing detection
        """
        logger.info(f"Running Closing Entries Analysis for {len(transactions)} transactions")
        
        # Configuration
        days_before_month_end = 5  # Default 5 days before
        days_after_month_end = 5   # Default 5 days after
        
        # 1. Closing Entries Detection
        closing_entries = []
        for transaction in transactions:
            if transaction.posting_date:
                try:
                    if self._is_within_closing_window(transaction.posting_date, days_before_month_end, days_after_month_end):
                        logger.debug(f"Processing closing entry for transaction ID: {transaction.id}, document: {transaction.document_number}")
                        
                        # Calculate required variables BEFORE creating closing_entry
                        month_end_date = self._get_month_end_date(transaction.posting_date)
                        month_end_date_str = month_end_date.isoformat() if month_end_date else ''
                        days_from_month_end = self._get_days_from_month_end(transaction.posting_date)
                        fs_line = self._get_fs_line_from_account(transaction.gl_account)
                        
                        closing_entry = {
                            'transaction_id': str(transaction.id),
                            'document_number': transaction.document_number,
                            'gl_account': transaction.gl_account,
                            'amount': float(transaction.amount_local_currency),
                            'user_name': transaction.user_name,
                            'posting_date': transaction.posting_date.isoformat(),
                            'month_end_date': month_end_date_str,
                            'days_from_month_end': days_from_month_end,
                            'is_post_close': self._is_post_close_entry(transaction.posting_date),
                            'closing_window_type': self._get_closing_window_type(transaction.posting_date, days_before_month_end, days_after_month_end),
                            'risk_score': self._calculate_closing_risk_score(transaction),
                            'transaction_type': transaction.transaction_type,
                            'fiscal_year': transaction.fiscal_year,
                            'posting_period': transaction.posting_period,
                            'fs_line': fs_line
                        }
                        
                        # UUID validation
                        tid = closing_entry['transaction_id']
                        try:
                            uuid.UUID(str(tid))
                        except ValueError:
                            logger.warning(f"Skipping invalid transaction_id (not UUID): {tid}")
                            continue
                        
                        closing_entries.append(closing_entry)
                        logger.debug(f"Added closing entry for transaction ID: {transaction.id}")
                except Exception as e:
                    logger.error(f"Error processing transaction {transaction.id} for closing entries analysis: {e}")
                    continue
        
        # 2. Post-Close Flag Analysis
        post_close_analysis = {
            'total_post_close_entries': 0,
            'post_close_by_fs_line': {},
            'post_close_by_user': {},
            'post_close_by_month': {},
            'high_value_post_close': [],
            'post_close_patterns': {}
        }
        
        for entry in closing_entries:
            if entry['is_post_close']:
                post_close_analysis['total_post_close_entries'] += 1
                
                # By FS Line
                fs_line = entry['fs_line']
                if fs_line not in post_close_analysis['post_close_by_fs_line']:
                    post_close_analysis['post_close_by_fs_line'][fs_line] = {
                        'count': 0,
                        'total_amount': 0,
                        'entries': []
                    }
                post_close_analysis['post_close_by_fs_line'][fs_line]['count'] += 1
                post_close_analysis['post_close_by_fs_line'][fs_line]['total_amount'] += entry['amount']
                post_close_analysis['post_close_by_fs_line'][fs_line]['entries'].append(entry)
                
                # By User
                user_name = entry['user_name']
                if user_name not in post_close_analysis['post_close_by_user']:
                    post_close_analysis['post_close_by_user'][user_name] = {
                        'count': 0,
                        'total_amount': 0,
                        'entries': []
                    }
                post_close_analysis['post_close_by_user'][user_name]['count'] += 1
                post_close_analysis['post_close_by_user'][user_name]['total_amount'] += entry['amount']
                post_close_analysis['post_close_by_user'][user_name]['entries'].append(entry)
                
                # By Month
                month_key = f"{entry['posting_date'][:7]}"  # YYYY-MM
                if month_key not in post_close_analysis['post_close_by_month']:
                    post_close_analysis['post_close_by_month'][month_key] = {
                        'count': 0,
                        'total_amount': 0
                    }
                post_close_analysis['post_close_by_month'][month_key]['count'] += 1
                post_close_analysis['post_close_by_month'][month_key]['total_amount'] += entry['amount']
                
                # High value post-close entries
                if entry['amount'] > 1000000:
                    post_close_analysis['high_value_post_close'].append(entry)
        
        # 3. Closing Entries by FS Line
        fs_line_closing = {}
        for entry in closing_entries:
            fs_line = entry['fs_line']
            if fs_line not in fs_line_closing:
                fs_line_closing[fs_line] = {
                    'fs_line': fs_line,
                    'total_closing_entries': 0,
                    'total_amount': 0,
                    'pre_close_count': 0,
                    'month_end_count': 0,
                    'post_close_count': 0,
                    'pre_close_amount': 0,
                    'month_end_amount': 0,
                    'post_close_amount': 0,
                    'unique_users': set(),
                    'entries': []
                }
            
            fs_line_closing[fs_line]['total_closing_entries'] += 1
            fs_line_closing[fs_line]['total_amount'] += entry['amount']
            fs_line_closing[fs_line]['unique_users'].add(entry['user_name'])
            fs_line_closing[fs_line]['entries'].append(entry)
            
            # Categorize by closing window type
            window_type = entry['closing_window_type']
            if window_type == 'pre_close':
                fs_line_closing[fs_line]['pre_close_count'] += 1
                fs_line_closing[fs_line]['pre_close_amount'] += entry['amount']
            elif window_type == 'month_end':
                fs_line_closing[fs_line]['month_end_count'] += 1
                fs_line_closing[fs_line]['month_end_amount'] += entry['amount']
            elif window_type == 'post_close':
                fs_line_closing[fs_line]['post_close_count'] += 1
                fs_line_closing[fs_line]['post_close_amount'] += entry['amount']
        
        # Convert sets to lists for JSON serialization
        for fs_line in fs_line_closing:
            fs_line_closing[fs_line]['unique_users'] = list(fs_line_closing[fs_line]['unique_users'])
        
        # 4. Closing Entries by User
        user_closing = {}
        for entry in closing_entries:
            user_name = entry['user_name']
            if user_name not in user_closing:
                user_closing[user_name] = {
                    'user_name': user_name,
                    'total_closing_entries': 0,
                    'total_amount': 0,
                    'pre_close_count': 0,
                    'month_end_count': 0,
                    'post_close_count': 0,
                    'pre_close_amount': 0,
                    'month_end_amount': 0,
                    'post_close_amount': 0,
                    'unique_fs_lines': set(),
                    'entries': []
                }
            
            user_closing[user_name]['total_closing_entries'] += 1
            user_closing[user_name]['total_amount'] += entry['amount']
            user_closing[user_name]['unique_fs_lines'].add(entry['fs_line'])
            user_closing[user_name]['entries'].append(entry)
            
            # Categorize by closing window type
            window_type = entry['closing_window_type']
            if window_type == 'pre_close':
                user_closing[user_name]['pre_close_count'] += 1
                user_closing[user_name]['pre_close_amount'] += entry['amount']
            elif window_type == 'month_end':
                user_closing[user_name]['month_end_count'] += 1
                user_closing[user_name]['month_end_amount'] += entry['amount']
            elif window_type == 'post_close':
                user_closing[user_name]['post_close_count'] += 1
                user_closing[user_name]['post_close_amount'] += entry['amount']
        
        # Convert sets to lists for JSON serialization
        for user_name in user_closing:
            user_closing[user_name]['unique_fs_lines'] = list(user_closing[user_name]['unique_fs_lines'])
        
        # 5. Month-end Activity Patterns
        month_end_patterns = {}
        for transaction in transactions:
            if transaction.posting_date:
                month_key = f"{transaction.posting_date.year}-{transaction.posting_date.month:02d}"
                if month_key not in month_end_patterns:
                    month_end_patterns[month_key] = {
                        'month': month_key,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'closing_entries': 0,
                        'closing_amount': 0,
                        'post_close_entries': 0,
                        'post_close_amount': 0,
                        'unique_users': set(),
                        'unique_accounts': set()
                    }
                
                month_end_patterns[month_key]['total_transactions'] += 1
                month_end_patterns[month_key]['total_amount'] += float(transaction.amount_local_currency)
                month_end_patterns[month_key]['unique_users'].add(transaction.user_name)
                month_end_patterns[month_key]['unique_accounts'].add(transaction.gl_account)
                
                # Check if it's a closing entry
                if self._is_within_closing_window(transaction.posting_date, days_before_month_end, days_after_month_end):
                    month_end_patterns[month_key]['closing_entries'] += 1
                    month_end_patterns[month_key]['closing_amount'] += float(transaction.amount_local_currency)
                    
                    # Check if it's post-close
                    if self._is_post_close_entry(transaction.posting_date):
                        month_end_patterns[month_key]['post_close_entries'] += 1
                        month_end_patterns[month_key]['post_close_amount'] += float(transaction.amount_local_currency)
        
        # Convert sets to lists for JSON serialization
        for month_key in month_end_patterns:
            month_end_patterns[month_key]['unique_users'] = list(month_end_patterns[month_key]['unique_users'])
            month_end_patterns[month_key]['unique_accounts'] = list(month_end_patterns[month_key]['unique_accounts'])
        
        # 6. Risk Assessment
        total_transactions = len(transactions)
        closing_count = len(closing_entries)
        post_close_count = post_close_analysis['total_post_close_entries']
        high_value_post_close = len(post_close_analysis['high_value_post_close'])
        
        closing_entries_risk = (closing_count / total_transactions * 100) if total_transactions > 0 else 0
        post_close_risk = (post_close_count / total_transactions * 100) if total_transactions > 0 else 0
        high_value_post_close_risk = (high_value_post_close / total_transactions * 100) if total_transactions > 0 else 0
        
        # Unusual closing patterns
        unusual_pattern_risk = 0
        if closing_count > 0:
            post_close_ratio = post_close_count / closing_count
            if post_close_ratio > 0.3:  # More than 30% post-close entries
                unusual_pattern_risk = post_close_ratio * 100
        
        # Overall risk assessment
        overall_risk_score = (
            closing_entries_risk * 0.3 +
            post_close_risk * 0.25 +
            high_value_post_close_risk * 0.25 +
            unusual_pattern_risk * 0.2
        )
        
        risk_assessment = {
            'overall_risk_score': overall_risk_score,
            'closing_entries_risk_score': closing_entries_risk,
            'post_close_risk_score': post_close_risk,
            'high_value_post_close_risk_score': high_value_post_close_risk,
            'unusual_pattern_risk_score': unusual_pattern_risk,
            'risk_level': self._get_risk_level(overall_risk_score),
            'recommendations': self._get_closing_risk_recommendations(overall_risk_score, closing_count, post_close_count, high_value_post_close)
        }
        
        # 7. Chart Data
        chart_data = {}
        
        # Post Close Flag by FSLI
        if post_close_analysis['post_close_by_fs_line']:
            fs_lines = list(post_close_analysis['post_close_by_fs_line'].keys())
            post_close_counts = [post_close_analysis['post_close_by_fs_line'][fs]['count'] for fs in fs_lines]
            post_close_amounts = [post_close_analysis['post_close_by_fs_line'][fs]['total_amount'] for fs in fs_lines]
            
            chart_data['post_close_by_fs_line'] = {
                'labels': fs_lines,
                'counts': post_close_counts,
                'amounts': post_close_amounts
            }
        
        # Post Close Flag by User
        if post_close_analysis['post_close_by_user']:
            users = list(post_close_analysis['post_close_by_user'].keys())
            post_close_counts = [post_close_analysis['post_close_by_user'][user]['count'] for user in users]
            post_close_amounts = [post_close_analysis['post_close_by_user'][user]['total_amount'] for user in users]
            
            chart_data['post_close_by_user'] = {
                'labels': users,
                'counts': post_close_counts,
                'amounts': post_close_amounts
            }
        
        # Month-end Activity Patterns
        if month_end_patterns:
            months = sorted(month_end_patterns.keys())
            total_transactions = [month_end_patterns[month]['total_transactions'] for month in months]
            closing_entries_count = [month_end_patterns[month]['closing_entries'] for month in months]
            post_close_entries = [month_end_patterns[month]['post_close_entries'] for month in months]
            
            chart_data['month_end_patterns'] = {
                'labels': months,
                'total_transactions': total_transactions,
                'closing_entries': closing_entries_count,
                'post_close_entries': post_close_entries
            }
        
        # 8. Export Data
        export_data = []
        
        # Validate closing_entries before processing
        valid_closing_entries = []
        for entry in closing_entries:
            if isinstance(entry, dict) and 'transaction_id' in entry:
                valid_closing_entries.append(entry)
            elif isinstance(entry, (int, str)):
                # Handle case where entry is just an ID - try to find the full entry
                logger.warning(f"Found closing entry ID instead of full entry: {entry} (type: {type(entry)})")
                # Try to find the corresponding transaction and create a basic entry
                try:
                    transaction = next((t for t in transactions if str(t.id) == str(entry)), None)
                    if transaction:
                        basic_entry = {
                            'transaction_id': str(transaction.id),
                            'document_number': transaction.document_number,
                            'gl_account': transaction.gl_account,
                            'amount': float(transaction.amount_local_currency),
                            'user_name': transaction.user_name,
                            'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else '',
                            'closing_window_type': 'unknown',
                            'is_post_close': False,
                            'days_from_month_end': 0,
                            'risk_score': 0.0,
                            'fs_line': self._get_fs_line_from_account(transaction.gl_account)
                        }
                        valid_closing_entries.append(basic_entry)
                        logger.info(f"Created basic closing entry for transaction ID: {entry}")
                    else:
                        logger.warning(f"Could not find transaction with ID: {entry}")
                except Exception as e:
                    logger.error(f"Error creating basic closing entry for ID {entry}: {e}")
            else:
                logger.warning(f"Skipping invalid closing entry: {entry} (type: {type(entry)})")
        
        for entry in valid_closing_entries:
            try:
                export_data.append({
                    'type': 'closing_entry',
                    'transaction_id': entry.get('transaction_id', ''),
                    'document_number': entry.get('document_number', ''),
                    'gl_account': entry.get('gl_account', ''),
                    'amount': entry.get('amount', 0.0),
                    'user_name': entry.get('user_name', ''),
                    'posting_date': entry.get('posting_date', ''),
                    'closing_window_type': entry.get('closing_window_type', ''),
                    'is_post_close': entry.get('is_post_close', False),
                    'days_from_month_end': entry.get('days_from_month_end', 0),
                    'risk_score': entry.get('risk_score', 0.0),
                    'fs_line': entry.get('fs_line', '')
                })
            except Exception as e:
                logger.error(f"Error processing closing entry for export: {e}")
                continue
        
        # Post-close analysis export
        for fs_line, data in post_close_analysis['post_close_by_fs_line'].items():
            export_data.append({
                'type': 'post_close_by_fs_line',
                'fs_line': fs_line,
                'count': data['count'],
                'total_amount': data['total_amount']
            })
        
        for user_name, data in post_close_analysis['post_close_by_user'].items():
            export_data.append({
                'type': 'post_close_by_user',
                'user_name': user_name,
                'count': data['count'],
                'total_amount': data['total_amount']
            })
        
        # Create analysis result
        analysis_result = {
            'analysis_info': {
                'total_transactions': total_transactions,
                'closing_entries_count': closing_count,
                'days_before_month_end': days_before_month_end,
                'days_after_month_end': days_after_month_end,
                'analysis_date': timezone.now().isoformat(),
                'processing_duration': 0
            },
            'closing_entries': closing_entries,
            'post_close_analysis': post_close_analysis,
            'fs_line_closing': fs_line_closing,
            'user_closing': user_closing,
            'month_end_patterns': month_end_patterns,
            'risk_assessment': risk_assessment,
            'chart_data': chart_data,
            'export_data': export_data
        }
        
        return analysis_result
    
    def run_user_analysis(self, transactions: List) -> Dict[str, Any]:
        """
        Run user analysis with comprehensive user behavior analysis
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running User Analysis for {len(transactions)} transactions")
        
        # Get unique users
        unique_users = list(set(t.user_name for t in transactions))
        
        # 1. User Transaction Summary
        user_transaction_summary = {}
        for user in unique_users:
            user_transactions = [t for t in transactions if t.user_name == user]
            total_amount = sum(float(t.amount_local_currency) for t in user_transactions)
            debit_amount = sum(float(t.amount_local_currency) for t in user_transactions if t.transaction_type == 'DEBIT')
            credit_amount = sum(float(t.amount_local_currency) for t in user_transactions if t.transaction_type == 'CREDIT')
            
            user_transaction_summary[user] = {
                'user': user,
                'total_transactions': len(user_transactions),
                'total_amount': total_amount,
                'debit_amount': debit_amount,
                'credit_amount': credit_amount,
                'balance': debit_amount - credit_amount,
                'unique_accounts': len(set(t.gl_account for t in user_transactions)),
                'average_transaction_amount': float(total_amount) / len(user_transactions) if user_transactions else 0,
                'transaction_types': {
                    'debit_count': len([t for t in user_transactions if t.transaction_type == 'DEBIT']),
                    'credit_count': len([t for t in user_transactions if t.transaction_type == 'CREDIT'])
                }
            }
        
        # 2. User Debit Analysis
        user_debit_analysis = {}
        for user in unique_users:
            user_transactions = [t for t in transactions if t.user_name == user]
            debit_transactions = [t for t in user_transactions if t.transaction_type == 'DEBIT']
            
            if debit_transactions:
                debit_amounts = [float(t.amount_local_currency) for t in debit_transactions]
                user_debit_analysis[user] = {
                    'user': user,
                    'total_debit_amount': sum(debit_amounts),
                    'debit_count': len(debit_transactions),
                    'average_debit_amount': float(sum(debit_amounts)) / len(debit_amounts),
                    'max_debit_amount': max(debit_amounts),
                    'min_debit_amount': min(debit_amounts),
                    'debit_accounts': list(set(t.gl_account for t in debit_transactions))
                }
            else:
                user_debit_analysis[user] = {
                    'user': user,
                    'total_debit_amount': 0,
                    'debit_count': 0,
                    'average_debit_amount': 0,
                    'max_debit_amount': 0,
                    'min_debit_amount': 0,
                    'debit_accounts': []
                }
        
        # 3. User Account Distribution
        user_account_distribution = {}
        for user in unique_users:
            user_transactions = [t for t in transactions if t.user_name == user]
            account_counts = {}
            
            for transaction in user_transactions:
                account = transaction.gl_account
                if account not in account_counts:
                    account_counts[account] = {
                        'count': 0,
                        'total_amount': 0,
                        'debit_amount': 0,
                        'credit_amount': 0
                    }
                
                amount = float(transaction.amount_local_currency)
                account_counts[account]['count'] += 1
                account_counts[account]['total_amount'] += amount
                
                if transaction.transaction_type == 'DEBIT':
                    account_counts[account]['debit_amount'] += amount
                else:
                    account_counts[account]['credit_amount'] += amount
            
            user_account_distribution[user] = {
                'user': user,
                'total_accounts': len(account_counts),
                'account_details': [
                    {
                        'account': account,
                        'count': details['count'],
                        'total_amount': details['total_amount'],
                        'debit_amount': details['debit_amount'],
                        'credit_amount': details['credit_amount']
                    }
                    for account, details in account_counts.items()
                ]
            }
        
        # 4. User Anomalies Detection
        user_anomalies = {}
        for user in unique_users:
            user_transactions = [t for t in transactions if t.user_name == user]
            user_summary = user_transaction_summary[user]
            
            anomalies = []
            
            # High transaction count anomaly
            if user_summary['total_transactions'] > 100:  # Threshold for high activity
                anomalies.append({
                    'type': 'high_transaction_count',
                    'severity': 'medium',
                    'description': f'User has {user_summary["total_transactions"]} transactions',
                    'value': user_summary['total_transactions']
                })
            
            # High amount anomaly
            if user_summary['total_amount'] > 1000000:  # Threshold for high amount
                anomalies.append({
                    'type': 'high_amount',
                    'severity': 'high',
                    'description': f'User total amount: {user_summary["total_amount"]:,.2f}',
                    'value': user_summary['total_amount']
                })
            
            # Unusual balance anomaly
            balance_ratio = abs(user_summary['balance']) / user_summary['total_amount'] if user_summary['total_amount'] > 0 else 0
            if balance_ratio > 0.8:  # Threshold for unusual balance
                anomalies.append({
                    'type': 'unusual_balance',
                    'severity': 'medium',
                    'description': f'Unusual balance ratio: {balance_ratio:.2%}',
                    'value': balance_ratio
                })
            
            # Account concentration anomaly
            if user_summary['unique_accounts'] == 1 and user_summary['total_transactions'] > 10:
                anomalies.append({
                    'type': 'account_concentration',
                    'severity': 'low',
                    'description': f'User uses only 1 account for {user_summary["total_transactions"]} transactions',
                    'value': user_summary['unique_accounts']
                })
            
            user_anomalies[user] = {
                'user': user,
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'risk_level': 'high' if len([a for a in anomalies if a['severity'] == 'high']) > 0 else 'medium' if len(anomalies) > 0 else 'low'
            }
        
        # 5. User Risk Assessment
        user_risk_assessment = {}
        for user in unique_users:
            user_transactions = [t for t in transactions if t.user_name == user]
            user_summary = user_transaction_summary[user]
            user_anomaly = user_anomalies[user]
            
            # Calculate risk score based on various factors
            risk_score = 0
            
            # Transaction count risk
            if user_summary['total_transactions'] > 100:
                risk_score += 20
            elif user_summary['total_transactions'] > 50:
                risk_score += 10
            
            # Amount risk
            if user_summary['total_amount'] > 1000000:
                risk_score += 30
            elif user_summary['total_amount'] > 500000:
                risk_score += 20
            elif user_summary['total_amount'] > 100000:
                risk_score += 10
            
            # Balance risk
            balance_ratio = abs(user_summary['balance']) / float(user_summary['total_amount']) if user_summary['total_amount'] > 0 else 0
            if balance_ratio > 0.8:
                risk_score += 15
            elif balance_ratio > 0.5:
                risk_score += 10
            
            # Anomaly risk
            risk_score += len(user_anomaly['anomalies']) * 5
            
            # Account diversity risk (lower diversity = higher risk)
            account_diversity = user_summary['unique_accounts'] / float(user_summary['total_transactions']) if user_summary['total_transactions'] > 0 else 0
            if account_diversity < 0.1:
                risk_score += 15
            elif account_diversity < 0.2:
                risk_score += 10
            
            # Cap risk score at 100
            risk_score = min(risk_score, 100)
            
            # Determine risk level
            if risk_score >= 80:
                risk_level = 'critical'
            elif risk_score >= 60:
                risk_level = 'high'
            elif risk_score >= 40:
                risk_level = 'medium'
            else:
                risk_level = 'low'
            
            user_risk_assessment[user] = {
                'user': user,
                'risk_score': risk_score,
                'risk_level': risk_level,
                'risk_factors': {
                    'transaction_count_risk': 20 if user_summary['total_transactions'] > 100 else 10 if user_summary['total_transactions'] > 50 else 0,
                    'amount_risk': 30 if user_summary['total_amount'] > 1000000 else 20 if user_summary['total_amount'] > 500000 else 10 if user_summary['total_amount'] > 100000 else 0,
                    'balance_risk': 15 if balance_ratio > 0.8 else 10 if balance_ratio > 0.5 else 0,
                    'anomaly_risk': len(user_anomaly['anomalies']) * 5,
                    'account_diversity_risk': 15 if account_diversity < 0.1 else 10 if account_diversity < 0.2 else 0
                },
                'recommendations': [
                    'Review high transaction volume' if user_summary['total_transactions'] > 100 else None,
                    'Investigate large amounts' if user_summary['total_amount'] > 1000000 else None,
                    'Check unusual balance patterns' if balance_ratio > 0.8 else None,
                    'Review account concentration' if account_diversity < 0.1 else None
                ]
            }
            # Remove None values from recommendations
            user_risk_assessment[user]['recommendations'] = [r for r in user_risk_assessment[user]['recommendations'] if r is not None]
        
        # 6. Chart Data Generation
        chart_data = {
            'user_activity': {user: user_transaction_summary[user]['total_transactions'] for user in unique_users},
            'user_amounts': {user: float(user_transaction_summary[user]['total_amount']) for user in unique_users},
            'user_risk_scores': {user: user_risk_assessment[user]['risk_score'] for user in unique_users},
            'risk_distribution': {
                'critical': len([u for u in unique_users if user_risk_assessment[u]['risk_level'] == 'critical']),
                'high': len([u for u in unique_users if user_risk_assessment[u]['risk_level'] == 'high']),
                'medium': len([u for u in unique_users if user_risk_assessment[u]['risk_level'] == 'medium']),
                'low': len([u for u in unique_users if user_risk_assessment[u]['risk_level'] == 'low'])
            },
            'anomaly_distribution': {
                'high_anomalies': len([u for u in unique_users if user_anomalies[u]['anomaly_count'] > 3]),
                'medium_anomalies': len([u for u in unique_users if 1 <= user_anomalies[u]['anomaly_count'] <= 3]),
                'no_anomalies': len([u for u in unique_users if user_anomalies[u]['anomaly_count'] == 0])
            }
        }
        
        # 7. Export Data
        export_data = []
        for user in unique_users:
            export_data.append({
                'user': user,
                'transaction_summary': user_transaction_summary[user],
                'debit_analysis': user_debit_analysis[user],
                'account_distribution': user_account_distribution[user],
                'anomalies': user_anomalies[user],
                'risk_assessment': user_risk_assessment[user]
            })
        
        # Create analysis results
        analysis_results = {
            'total_users': len(unique_users),
            'user_transaction_summary': list(user_transaction_summary.values()),
            'user_debit_analysis': list(user_debit_analysis.values()),
            'user_account_distribution': list(user_account_distribution.values()),
            'user_anomalies': list(user_anomalies.values()),
            'user_risk_assessment': list(user_risk_assessment.values()),
            'statistical_summary': {
                'average_transactions_per_user': sum(u['total_transactions'] for u in user_transaction_summary.values()) / len(unique_users) if unique_users else 0,
                'average_amount_per_user': sum(float(u['total_amount']) for u in user_transaction_summary.values()) / len(unique_users) if unique_users else 0,
                'high_risk_users': len([u for u in user_risk_assessment.values() if u['risk_level'] in ['high', 'critical']]),
                'users_with_anomalies': len([u for u in user_anomalies.values() if u['anomaly_count'] > 0])
            },
            'chart_data': chart_data,
            'export_data': export_data,
            'processing_duration': 0.0  # Will be set by caller
        }
        
        # Cache results
        cache_key = f"user_{len(transactions)}_{hash(tuple(t.id for t in transactions))}"
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
                if abs(float(t1.amount_local_currency) - float(t2.amount_local_currency)) > 0.01:  # Allow small rounding differences
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
                    if abs(float(t1.amount_local_currency) - float(t2.amount_local_currency)) <= 0.01:
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
        
        # Validate basic model results
        if not general_results:
            logger.warning("General analysis returned empty results")
            general_results = []
        if not duplicate_results:
            logger.warning("Duplicate analysis returned empty results")
            duplicate_results = []
        if not backdated_results:
            logger.warning("Backdated analysis returned empty results")
            backdated_results = []
        
        # Get user analysis results if available
        try:
            user_results = self.model_manager.predict_with_model('user', transactions)
        except Exception as e:
            logger.warning(f"Could not get user results for overall analysis: {e}")
            user_results = []
        
        # Get unusual days analysis results
        try:
            unusual_days_results = self.run_unusual_days_analysis(transactions)
            unusual_days_transactions = unusual_days_results.get('unusual_transactions', [])
        except Exception as e:
            logger.warning(f"Could not get unusual days results for overall analysis: {e}")
            unusual_days_transactions = []
        
        # Get closing entries analysis results
        try:
            closing_entries_results = self.run_closing_entries_analysis(transactions)
            closing_entries_transactions = closing_entries_results.get('closing_entries', [])
            logger.info(f"Overall analysis: Found {len(closing_entries_transactions)} closing entries")
            
            # Ensure we have valid closing entries (not just IDs)
            valid_closing_entries = []
            for entry in closing_entries_transactions:
                if isinstance(entry, dict) and 'transaction_id' in entry:
                    valid_closing_entries.append(entry)
                else:
                    logger.warning(f"Skipping invalid closing entry in overall analysis: {entry} (type: {type(entry)})")
            
            closing_entries_transactions = valid_closing_entries
            logger.info(f"Overall analysis: Using {len(closing_entries_transactions)} valid closing entries")
            
        except Exception as e:
            logger.warning(f"Could not get closing entries results for overall analysis: {e}")
            closing_entries_transactions = []
        
        # Get overall analysis predictions with all results
        overall_results = self.model_manager.predict_with_model('overall', transactions, 
                                                               general_results=general_results,
                                                               duplicate_results=duplicate_results,
                                                               backdated_results=backdated_results,
                                                               user_results=user_results,
                                                               unusual_days_results=unusual_days_transactions,
                                                               closing_entries_results=closing_entries_transactions)
        
        # Validate overall results
        if not overall_results:
            logger.warning("Overall analysis returned empty results. Creating default results.")
            overall_results = []
            for transaction in transactions:
                overall_results.append({
                    'transaction_id': str(transaction.id),
                    'overall_risk_score': 50.0,  # Default medium risk
                    'risk_level': 'MEDIUM',
                    'recommendations': ['Analysis completed with default risk assessment']
                })
        
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
    
    def run_risk_analysis(self, transactions: List, overall_results: List = None) -> Dict[str, Any]:
        """
        Run risk analysis with ML model
        
        Args:
            transactions: List of transactions to analyze
            overall_results: Optional overall analysis results to reuse (prevents duplicate calls)
            
        Returns:
            Dict containing analysis results
        """
        logger.info(f"Running Risk Analysis with ML for {len(transactions)} transactions")
        
        # Get overall analysis results first (only if not provided)
        if overall_results is None:
            logger.info("Risk Analysis: Calling overall model prediction (no results provided)")
            overall_results = self.model_manager.predict_with_model('overall', transactions)
        else:
            logger.info(f"Risk Analysis: Reusing {len(overall_results)} overall results (preventing duplicate calls)")
        
        # Check if we have enough data for risk analysis
        if len(transactions) < 10:
            logger.warning(f"Risk analysis requires at least 10 transactions, but only {len(transactions)} provided. Creating default results.")
            risk_results = []
            for transaction in transactions:
                risk_results.append({
                    'transaction_id': str(transaction.id),
                    'risk_class': 1,  # Default medium risk class
                    'risk_probability': 0.7,  # Default confidence
                    'risk_score': 50.0  # Default risk score
                })
        else:
            # Get risk analysis predictions
            risk_results = self.model_manager.predict_with_model('risk', transactions, 
                                                                overall_results=overall_results)
            
            # Validate risk results
            if not risk_results:
                logger.warning("Risk analysis returned empty results. Creating default results.")
                risk_results = []
                for transaction in transactions:
                    risk_results.append({
                        'transaction_id': str(transaction.id),
                        'risk_class': 1,  # Default medium risk class
                        'risk_probability': 0.7,  # Default confidence
                        'risk_score': 50.0  # Default risk score
                    })
        
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
                'average_confidence': sum(r.get('risk_probability', 0.8) for r in risk_results) / len(risk_results) if risk_results else 0,
                'high_confidence_predictions': len([r for r in risk_results if r.get('risk_probability', 0.8) > 0.8]),
                'medium_confidence_predictions': len([r for r in risk_results if 0.5 <= r.get('risk_probability', 0.8) <= 0.8]),
                'low_confidence_predictions': len([r for r in risk_results if r.get('risk_probability', 0.8) < 0.5])
            },
            'final_risk_scores': {
                'average': sum(r.get('risk_score', 0) for r in risk_results) / len(risk_results) if risk_results else 0,
                'max': max(r.get('risk_score', 0) for r in risk_results) if risk_results else 0,
                'min': min(r.get('risk_score', 0) for r in risk_results) if risk_results else 0
            },
            'high_risk_transactions': [
                {
                    'transaction_id': r['transaction_id'],
                    'risk_class': r.get('risk_class', 0),
                    'confidence': r.get('risk_probability', 0.8),
                    'final_risk_score': r.get('risk_score', 0)
                }
                for r in risk_results if r.get('risk_class', 0) >= 2  # High or Critical
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
        
        # Risk Analysis (reuse overall results to prevent duplicate calls)
        risk_start = timezone.now()
        # Extract overall results from the overall analysis
        overall_results = results['overall'].get('ml_results', [])
        logger.info(f"Running Risk Analysis with {len(overall_results)} overall results from previous analysis")
        results['risk'] = self.run_risk_analysis(transactions, overall_results=overall_results)
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
            'training_log': self._training_log
        }
    

    

    
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
    
    def _get_day_name(self, day_of_week: int) -> str:
        """Get day name from day of week number"""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return days[day_of_week] if 0 <= day_of_week <= 6 else 'Unknown'
    
    def _get_fs_line_from_account(self, account_id: str) -> str:
        """Get FS line from account ID"""
        if not account_id:
            return 'Unknown'
        
        try:
            # Simple FS line mapping based on account ID patterns
            if account_id.startswith('1'):  # Assets
                return 'Assets'
            elif account_id.startswith('2'):  # Liabilities
                return 'Liabilities'
            elif account_id.startswith('3'):  # Equity
                return 'Equity'
            elif account_id.startswith('4'):  # Revenue
                return 'Revenue'
            elif account_id.startswith('5'):  # Expenses
                return 'Expenses'
            else:
                return 'Other'
        except:
            return 'Unknown'
    
    def _calculate_weekend_risk_score(self, transaction) -> float:
        """Calculate risk score for weekend posting"""
        risk_score = 50.0  # Base risk for weekend posting
        
        # Increase risk for high value transactions
        if float(transaction.amount_local_currency) > 1000000:
            risk_score += 30.0
        
        # Increase risk for unusual transaction types
        if transaction.transaction_type == 'DEBIT':
            risk_score += 10.0
        
        # Increase risk for month-end or year-end
        if transaction.posting_date:
            if transaction.posting_date.day >= 25:
                risk_score += 10.0
            if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
                risk_score += 10.0
        
        return min(risk_score, 100.0)
    
    def _get_weekend_risk_recommendations(self, risk_score: float, weekend_count: int, unusual_count: int) -> List[str]:
        """Get recommendations based on weekend risk assessment"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.extend([
                'Immediate investigation of weekend postings required',
                'Review all weekend transactions for validity',
                'Implement weekend posting restrictions',
                'Monitor user activity patterns closely'
            ])
        elif risk_score >= 60:
            recommendations.extend([
                'Investigate weekend posting patterns',
                'Review unusual day activity',
                'Consider implementing posting time restrictions',
                'Monitor high-value weekend transactions'
            ])
        elif risk_score >= 30:
            recommendations.extend([
                'Monitor weekend posting trends',
                'Review unusual day patterns',
                'Consider implementing controls for weekend postings'
            ])
        else:
            recommendations.append('Normal weekend posting patterns detected')
        
        if weekend_count > 0:
            recommendations.append(f'Found {weekend_count} weekend transactions requiring review')
        
        if unusual_count > 0:
            recommendations.append(f'Detected {unusual_count} unusual day patterns')
        
        return recommendations
    
    def _is_within_closing_window(self, posting_date, days_before, days_after):
        """Check if posting date is within closing window"""
        month_end = self._get_month_end_date(posting_date)
        days_from_end = self._get_days_from_month_end(posting_date)
        
        # Check if within the closing window (days_before before month end to days_after after month end)
        return -days_before <= days_from_end <= days_after
    
    def _get_month_end_date(self, posting_date):
        """Get the last day of the month for the posting date"""
        from calendar import monthrange
        year = posting_date.year
        month = posting_date.month
        month_range = monthrange(year, month)
        if isinstance(month_range, tuple) and len(month_range) >= 2:
            last_day = month_range[1]
        else:
            # Fallback: calculate last day manually
            if month == 12:
                last_day = 31
            elif month in [4, 6, 9, 11]:
                last_day = 30
            elif month == 2:
                # Leap year calculation
                if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            else:
                last_day = 31
        return datetime(year, month, last_day).date()
    
    def _get_days_from_month_end(self, posting_date):
        """Calculate days from month end (negative = before, positive = after)"""
        month_end = self._get_month_end_date(posting_date)
        return (posting_date - month_end).days
    
    def _is_post_close_entry(self, posting_date):
        """Check if entry is posted after month end"""
        days_from_end = self._get_days_from_month_end(posting_date)
        return days_from_end > 0
    
    def _get_closing_window_type(self, posting_date, days_before, days_after):
        """Get the type of closing window entry"""
        days_from_end = self._get_days_from_month_end(posting_date)
        
        if days_from_end < 0:
            return 'pre_close'
        elif days_from_end == 0:
            return 'month_end'
        else:
            return 'post_close'
    
    def _calculate_closing_risk_score(self, transaction) -> float:
        """Calculate risk score for closing entry"""
        risk_score = 30.0  # Base risk for closing entry
        
        # Increase risk for high value transactions
        if float(transaction.amount_local_currency) > 1000000:
            risk_score += 30.0
        
        # Increase risk for post-close entries
        if self._is_post_close_entry(transaction.posting_date):
            risk_score += 25.0
        
        # Increase risk for unusual transaction types
        if transaction.transaction_type == 'DEBIT':
            risk_score += 10.0
        
        # Increase risk for month-end or year-end
        if transaction.posting_date:
            if transaction.posting_date.day >= 25:
                risk_score += 5.0
            if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
                risk_score += 10.0
        
        return min(risk_score, 100.0)
    
    def _get_closing_risk_recommendations(self, risk_score: float, closing_count: int, post_close_count: int, high_value_post_close: int) -> List[str]:
        """Get recommendations based on closing entries risk assessment"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.extend([
                'Immediate investigation of closing entries required',
                'Review all post-close entries for validity',
                'Implement closing entry controls',
                'Monitor high-value closing transactions closely'
            ])
        elif risk_score >= 60:
            recommendations.extend([
                'Investigate closing entry patterns',
                'Review post-close entry procedures',
                'Consider implementing closing entry restrictions',
                'Monitor unusual closing patterns'
            ])
        elif risk_score >= 30:
            recommendations.extend([
                'Monitor closing entry trends',
                'Review closing procedures',
                'Consider implementing closing entry controls'
            ])
        else:
            recommendations.append('Normal closing entry patterns detected')
        
        if closing_count > 0:
            recommendations.append(f'Found {closing_count} closing entries requiring review')
        
        if post_close_count > 0:
            recommendations.append(f'Found {post_close_count} post-close entries requiring investigation')
        
        if high_value_post_close > 0:
            recommendations.append(f'Found {high_value_post_close} high-value post-close entries requiring immediate review')
        
        return recommendations
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Get risk level based on score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW' 
    
    def get_transactions_by_data_file(self, data_file_name: str) -> List:
        """
        Get transactions from database by data file name
        
        Args:
            data_file_name: Name of the data file
            
        Returns:
            List of SAPGLPosting objects
        """
        from .models import SAPGLPosting, DataFile
        
        try:
            # Find the data file
            data_file = DataFile.objects.filter(file_name=data_file_name).first()
            if not data_file:
                logger.warning(f"Data file not found: {data_file_name}")
                return []
            
            # Get all transactions for this data file
            transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
            logger.info(f"Retrieved {len(transactions)} transactions for data file: {data_file_name}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for data file {data_file_name}: {e}")
            return []
    
    def get_transactions_by_engagement(self, engagement_id: str) -> List:
        """
        Get transactions from database by engagement ID
        
        Args:
            engagement_id: Engagement ID
            
        Returns:
            List of SAPGLPosting objects
        """
        from .models import SAPGLPosting, DataFile
        
        try:
            # Find data files for this engagement
            data_files = DataFile.objects.filter(engagement_id=engagement_id)
            if not data_files.exists():
                logger.warning(f"No data files found for engagement: {engagement_id}")
                return []
            
            # Get all transactions for all data files in this engagement
            transactions = list(SAPGLPosting.objects.filter(data_file__in=data_files))
            logger.info(f"Retrieved {len(transactions)} transactions for engagement: {engagement_id}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for engagement {engagement_id}: {e}")
            return []
    
    def get_transactions_by_client(self, client_name: str) -> List:
        """
        Get transactions from database by client name
        
        Args:
            client_name: Client name
            
        Returns:
            List of SAPGLPosting objects
        """
        from .models import SAPGLPosting, DataFile
        
        try:
            # Find data files for this client
            data_files = DataFile.objects.filter(client_name__icontains=client_name)
            if not data_files.exists():
                logger.warning(f"No data files found for client: {client_name}")
                return []
            
            # Get all transactions for all data files for this client
            transactions = list(SAPGLPosting.objects.filter(data_file__in=data_files))
            logger.info(f"Retrieved {len(transactions)} transactions for client: {client_name}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for client {client_name}: {e}")
            return []
    
