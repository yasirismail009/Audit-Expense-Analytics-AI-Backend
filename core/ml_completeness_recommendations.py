#!/usr/bin/env python
"""
ML-based Completeness Test Recommendations and Fixes
Uses scikit-learn for intelligent analysis and recommendations
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import LocalOutlierFactor
from collections import defaultdict, Counter
import logging
from typing import Dict, List, Tuple, Any, Optional
from django.utils import timezone

logger = logging.getLogger(__name__)


class CompletenessMLAnalyzer:
    """
    ML-powered analyzer for completeness test recommendations and fixes
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.models = {}
        self.feature_importance = {}
        
    def extract_completeness_features(self, gl_postings, trial_balance_records, coa_records):
        """
        Extract comprehensive features for ML analysis
        """
        logger.info("🔍 Extracting completeness features for ML analysis...")
        
        features = {}
        
        # =======================================================================
        # BASIC STATISTICAL FEATURES
        # =======================================================================
        
        # GL Postings features
        gl_count = len(gl_postings)
        features['gl_transaction_count'] = gl_count
        features['gl_accounts_count'] = len(set(p.gl_account for p in gl_postings))
        features['gl_users_count'] = len(set(p.user_name for p in gl_postings if p.user_name))
        features['gl_profit_centers_count'] = len(set(p.profit_center for p in gl_postings if p.profit_center))
        
        # Amount statistics
        amounts = [float(p.amount_local_currency or 0) for p in gl_postings]
        features['gl_total_amount'] = sum(amounts)
        features['gl_mean_amount'] = np.mean(amounts) if amounts else 0
        features['gl_std_amount'] = np.std(amounts) if amounts else 0
        features['gl_min_amount'] = min(amounts) if amounts else 0
        features['gl_max_amount'] = max(amounts) if amounts else 0
        
        # Debit/Credit analysis
        debit_amounts = [abs(a) for a in amounts if a > 0]
        credit_amounts = [abs(a) for a in amounts if a < 0]
        features['gl_debit_total'] = sum(debit_amounts)
        features['gl_credit_total'] = sum(credit_amounts)
        features['gl_debit_credit_ratio'] = features['gl_debit_total'] / features['gl_credit_total'] if features['gl_credit_total'] > 0 else 0
        
        # =======================================================================
        # DATA QUALITY FEATURES
        # =======================================================================
        
        # Missing data analysis
        missing_document_numbers = sum(1 for p in gl_postings if not p.document_number or p.document_number.strip() == '')
        missing_user_names = sum(1 for p in gl_postings if not p.user_name or p.user_name.strip() == '')
        missing_profit_centers = sum(1 for p in gl_postings if not p.profit_center or p.profit_center.strip() == '')
        
        features['missing_document_rate'] = missing_document_numbers / gl_count if gl_count > 0 else 0
        features['missing_user_rate'] = missing_user_names / gl_count if gl_count > 0 else 0
        features['missing_profit_center_rate'] = missing_profit_centers / gl_count if gl_count > 0 else 0
        
        # Data consistency features
        features['unique_document_numbers'] = len(set(p.document_number for p in gl_postings if p.document_number))
        features['document_reuse_rate'] = gl_count / features['unique_document_numbers'] if features['unique_document_numbers'] > 0 else 0
        
        # =======================================================================
        # TRIAL BALANCE FEATURES
        # =======================================================================
        
        if trial_balance_records:
            tb_count = len(trial_balance_records)
            features['tb_records_count'] = tb_count
            features['tb_gl_ratio'] = gl_count / tb_count if tb_count > 0 else 0
            
            # TB amount analysis
            tb_amounts = [float(tb.amount or 0) for tb in trial_balance_records]
            features['tb_total_amount'] = sum(tb_amounts)
            features['tb_gl_amount_ratio'] = features['gl_total_amount'] / features['tb_total_amount'] if features['tb_total_amount'] > 0 else 0
        else:
            features['tb_records_count'] = 0
            features['tb_gl_ratio'] = 0
            features['tb_total_amount'] = 0
            features['tb_gl_amount_ratio'] = 0
        
        # =======================================================================
        # CHART OF ACCOUNTS FEATURES
        # =======================================================================
        
        if coa_records:
            coa_count = len(coa_records)
            features['coa_records_count'] = coa_count
            features['coa_gl_ratio'] = gl_count / coa_count if coa_count > 0 else 0
            
            # COA account type analysis
            account_types = [coa.account_type for coa in coa_records if coa.account_type]
            features['coa_account_types_count'] = len(set(account_types))
        else:
            features['coa_records_count'] = 0
            features['coa_gl_ratio'] = 0
            features['coa_account_types_count'] = 0
        
        # =======================================================================
        # TEMPORAL FEATURES
        # =======================================================================
        
        # Date analysis
        posting_dates = [p.posting_date for p in gl_postings if p.posting_date]
        if posting_dates:
            date_range = max(posting_dates) - min(posting_dates)
            features['date_range_days'] = date_range.days
            features['transactions_per_day'] = gl_count / date_range.days if date_range.days > 0 else 0
        else:
            features['date_range_days'] = 0
            features['transactions_per_day'] = 0
        
        # =======================================================================
        # BALANCE VERIFICATION FEATURES
        # =======================================================================
        
        # Document balance analysis
        document_balances = defaultdict(lambda: {'debit': 0, 'credit': 0})
        for posting in gl_postings:
            doc_num = posting.document_number or 'NO_DOCUMENT'
            amount = float(posting.amount_local_currency or 0)
            if amount > 0:
                document_balances[doc_num]['debit'] += amount
            else:
                document_balances[doc_num]['credit'] += abs(amount)
        
        balanced_docs = 0
        total_variance = 0
        for doc_num, balance in document_balances.items():
            variance = abs(balance['debit'] - balance['credit'])
            if variance < 0.01:  # Tolerance for floating point
                balanced_docs += 1
            total_variance += variance
        
        features['document_balance_rate'] = balanced_docs / len(document_balances) if document_balances else 0
        features['total_document_variance'] = total_variance
        features['average_document_variance'] = total_variance / len(document_balances) if document_balances else 0
        
        logger.info(f"📊 Extracted {len(features)} features for ML analysis")
        return features
    
    def analyze_completeness_patterns(self, features: Dict, historical_data: List[Dict] = None):
        """
        Analyze completeness patterns using ML algorithms
        """
        logger.info("🧠 Analyzing completeness patterns with ML...")
        
        # Convert features to array for ML processing
        feature_names = list(features.keys())
        feature_values = np.array(list(features.values())).reshape(1, -1)
        
        # =======================================================================
        # ANOMALY DETECTION
        # =======================================================================
        
        recommendations = []
        risk_factors = []
        
        # 1. Data Quality Anomaly Detection
        if features['missing_document_rate'] > 0.1:  # More than 10% missing documents
            risk_factors.append({
                'type': 'DATA_QUALITY',
                'severity': 'HIGH',
                'issue': 'High missing document rate',
                'value': f"{features['missing_document_rate']:.1%}",
                'recommendation': 'Review and fix missing document numbers in GL postings'
            })
        
        if features['missing_user_rate'] > 0.2:  # More than 20% missing users
            risk_factors.append({
                'type': 'DATA_QUALITY',
                'severity': 'MEDIUM',
                'issue': 'High missing user rate',
                'value': f"{features['missing_user_rate']:.1%}",
                'recommendation': 'Ensure all GL postings have user information'
            })
        
        # 2. Balance Verification Issues
        if features['document_balance_rate'] < 0.95:  # Less than 95% balanced documents
            risk_factors.append({
                'type': 'BALANCE_VERIFICATION',
                'severity': 'HIGH',
                'issue': 'Low document balance rate',
                'value': f"{features['document_balance_rate']:.1%}",
                'recommendation': 'Review unbalanced documents and correct posting errors'
            })
        
        if features['total_document_variance'] > 1000:  # High variance
            risk_factors.append({
                'type': 'BALANCE_VERIFICATION',
                'severity': 'MEDIUM',
                'issue': 'High document variance',
                'value': f"${features['total_document_variance']:,.2f}",
                'recommendation': 'Investigate high-variance documents for posting errors'
            })
        
        # 3. Data Consistency Issues
        if features['document_reuse_rate'] > 10:  # High document reuse
            risk_factors.append({
                'type': 'DATA_CONSISTENCY',
                'severity': 'LOW',
                'issue': 'High document reuse rate',
                'value': f"{features['document_reuse_rate']:.1f}x",
                'recommendation': 'Verify document numbering sequence and uniqueness'
            })
        
        # 4. Amount Analysis
        if features['gl_debit_credit_ratio'] < 0.8 or features['gl_debit_credit_ratio'] > 1.2:
            risk_factors.append({
                'type': 'AMOUNT_ANALYSIS',
                'severity': 'MEDIUM',
                'issue': 'Debit/Credit imbalance',
                'value': f"Ratio: {features['gl_debit_credit_ratio']:.2f}",
                'recommendation': 'Review debit and credit entries for balance'
            })
        
        # =======================================================================
        # ML-BASED RECOMMENDATIONS
        # =======================================================================
        
        # Use Isolation Forest for anomaly detection
        if historical_data and len(historical_data) > 10:
            try:
                # Prepare historical data
                historical_features = []
                for data in historical_data:
                    if isinstance(data, dict):
                        historical_features.append(list(data.values()))
                
                if len(historical_features) > 10:
                    historical_array = np.array(historical_features)
                    
                    # Train Isolation Forest
                    iso_forest = IsolationForest(contamination=0.1, random_state=42)
                    iso_forest.fit(historical_array)
                    
                    # Predict anomaly
                    anomaly_score = iso_forest.decision_function(feature_values)
                    is_anomaly = iso_forest.predict(feature_values)
                    
                    if is_anomaly[0] == -1:  # Anomaly detected
                        risk_factors.append({
                            'type': 'ML_ANOMALY',
                            'severity': 'HIGH',
                            'issue': 'ML-detected anomaly',
                            'value': f"Score: {anomaly_score[0]:.3f}",
                            'recommendation': 'This dataset shows unusual patterns compared to historical data'
                        })
            except Exception as e:
                logger.warning(f"ML anomaly detection failed: {e}")
        
        # =======================================================================
        # CLUSTERING ANALYSIS
        # =======================================================================
        
        # Use DBSCAN for pattern clustering
        try:
            if historical_data and len(historical_data) > 5:
                historical_features = []
                for data in historical_data:
                    if isinstance(data, dict):
                        historical_features.append(list(data.values()))
                
                if len(historical_features) > 5:
                    historical_array = np.array(historical_features)
                    
                    # Apply DBSCAN clustering
                    dbscan = DBSCAN(eps=0.5, min_samples=2)
                    clusters = dbscan.fit_predict(historical_array)
                    
                    # Check if current data fits existing patterns
                    current_cluster = dbscan.fit_predict(feature_values)
                    if current_cluster[0] == -1:  # Noise/outlier
                        risk_factors.append({
                            'type': 'CLUSTERING_ANALYSIS',
                            'severity': 'MEDIUM',
                            'issue': 'Data pattern mismatch',
                            'value': 'Outlier cluster',
                            'recommendation': 'This dataset does not match typical patterns'
                        })
        except Exception as e:
            logger.warning(f"Clustering analysis failed: {e}")
        
        # =======================================================================
        # GENERATE RECOMMENDATIONS
        # =======================================================================
        
        # Categorize recommendations by severity
        high_risk = [rf for rf in risk_factors if rf['severity'] == 'HIGH']
        medium_risk = [rf for rf in risk_factors if rf['severity'] == 'MEDIUM']
        low_risk = [rf for rf in risk_factors if rf['severity'] == 'LOW']
        
        # Generate action items
        action_items = []
        
        if high_risk:
            action_items.append({
                'priority': 'CRITICAL',
                'title': 'Immediate Action Required',
                'items': [rf['recommendation'] for rf in high_risk],
                'count': len(high_risk)
            })
        
        if medium_risk:
            action_items.append({
                'priority': 'HIGH',
                'title': 'Review and Address',
                'items': [rf['recommendation'] for rf in medium_risk],
                'count': len(medium_risk)
            })
        
        if low_risk:
            action_items.append({
                'priority': 'MEDIUM',
                'title': 'Monitor and Improve',
                'items': [rf['recommendation'] for rf in low_risk],
                'count': len(low_risk)
            })
        
        # Calculate overall risk score
        risk_score = (
            len(high_risk) * 3 + 
            len(medium_risk) * 2 + 
            len(low_risk) * 1
        ) / max(1, len(risk_factors)) if risk_factors else 0
        
        # Generate summary
        analysis_summary = {
            'total_issues': len(risk_factors),
            'high_risk_issues': len(high_risk),
            'medium_risk_issues': len(medium_risk),
            'low_risk_issues': len(low_risk),
            'risk_score': risk_score,
            'overall_status': 'PASS' if risk_score < 2 else 'REVIEW' if risk_score < 4 else 'FAIL',
            'risk_factors': risk_factors,
            'action_items': action_items,
            'ml_insights': {
                'feature_count': len(features),
                'anomaly_detection': 'enabled' if historical_data else 'disabled',
                'clustering_analysis': 'enabled' if historical_data else 'disabled'
            }
        }
        
        logger.info(f"📊 ML Analysis completed: {len(risk_factors)} issues found, Risk Score: {risk_score:.2f}")
        return analysis_summary
    
    def generate_fixes(self, analysis_summary: Dict) -> List[Dict]:
        """
        Generate specific fixes based on ML analysis
        """
        logger.info("🔧 Generating ML-based fixes...")
        
        fixes = []
        
        for risk_factor in analysis_summary['risk_factors']:
            fix = {
                'issue_type': risk_factor['type'],
                'severity': risk_factor['severity'],
                'description': risk_factor['issue'],
                'current_value': risk_factor['value'],
                'recommended_action': risk_factor['recommendation'],
                'implementation_steps': [],
                'expected_improvement': '',
                'validation_criteria': ''
            }
            
            # Generate specific implementation steps based on issue type
            if risk_factor['type'] == 'DATA_QUALITY':
                fix['implementation_steps'] = [
                    '1. Run data quality validation queries',
                    '2. Identify records with missing required fields',
                    '3. Contact data source for missing information',
                    '4. Implement data validation rules',
                    '5. Re-run completeness test after fixes'
                ]
                fix['expected_improvement'] = 'Reduce missing data rate to <5%'
                fix['validation_criteria'] = 'Missing data rate < 5%'
                
            elif risk_factor['type'] == 'BALANCE_VERIFICATION':
                fix['implementation_steps'] = [
                    '1. Identify unbalanced documents',
                    '2. Review posting logic and calculations',
                    '3. Correct posting errors',
                    '4. Implement balance validation rules',
                    '5. Verify document balance after corrections'
                ]
                fix['expected_improvement'] = 'Achieve 95%+ document balance rate'
                fix['validation_criteria'] = 'Document balance rate >= 95%'
                
            elif risk_factor['type'] == 'DATA_CONSISTENCY':
                fix['implementation_steps'] = [
                    '1. Review document numbering sequence',
                    '2. Check for duplicate document numbers',
                    '3. Implement unique document validation',
                    '4. Standardize document numbering format',
                    '5. Verify document uniqueness'
                ]
                fix['expected_improvement'] = 'Ensure unique document numbering'
                fix['validation_criteria'] = 'No duplicate document numbers'
                
            elif risk_factor['type'] == 'AMOUNT_ANALYSIS':
                fix['implementation_steps'] = [
                    '1. Review debit and credit entries',
                    '2. Check for posting sign errors',
                    '3. Verify account type classifications',
                    '4. Implement amount validation rules',
                    '5. Re-balance entries if necessary'
                ]
                fix['expected_improvement'] = 'Achieve balanced debit/credit totals'
                fix['validation_criteria'] = 'Debit total = Credit total (within tolerance)'
                
            elif risk_factor['type'] == 'ML_ANOMALY':
                fix['implementation_steps'] = [
                    '1. Compare with historical patterns',
                    '2. Investigate unusual data characteristics',
                    '3. Validate data source and processing',
                    '4. Check for data transformation errors',
                    '5. Document any legitimate changes'
                ]
                fix['expected_improvement'] = 'Understand and validate data patterns'
                fix['validation_criteria'] = 'Data patterns align with business expectations'
                
            elif risk_factor['type'] == 'CLUSTERING_ANALYSIS':
                fix['implementation_steps'] = [
                    '1. Analyze data pattern differences',
                    '2. Compare with similar engagements',
                    '3. Validate data completeness',
                    '4. Check for missing data categories',
                    '5. Ensure data represents full business cycle'
                ]
                fix['expected_improvement'] = 'Data patterns match expected business model'
                fix['validation_criteria'] = 'Data fits within expected pattern clusters'
            
            fixes.append(fix)
        
        logger.info(f"🔧 Generated {len(fixes)} ML-based fixes")
        return fixes
    
    def predict_completeness_score(self, features: Dict, historical_scores: List[float] = None) -> Dict:
        """
        Predict completeness score using ML regression
        """
        logger.info("🎯 Predicting completeness score with ML...")
        
        # Simple linear regression prediction based on key features
        score_factors = {
            'document_balance_rate': 0.3,  # 30% weight
            'missing_document_rate': -0.2,  # Negative impact
            'missing_user_rate': -0.1,      # Negative impact
            'tb_gl_ratio': 0.2,            # 20% weight
            'coa_gl_ratio': 0.1,           # 10% weight
            'gl_debit_credit_ratio': 0.1    # 10% weight
        }
        
        predicted_score = 100.0  # Start with perfect score
        
        for factor, weight in score_factors.items():
            if factor in features:
                if 'rate' in factor and factor != 'document_balance_rate':
                    # For missing data rates, subtract the missing percentage
                    predicted_score += weight * (1 - features[factor]) * 100
                elif factor == 'document_balance_rate':
                    # For balance rate, use the rate directly
                    predicted_score += weight * features[factor] * 100
                elif 'ratio' in factor:
                    # For ratios, penalize if far from 1.0
                    ratio = features[factor]
                    if ratio > 0:
                        deviation = abs(ratio - 1.0)
                        predicted_score -= weight * deviation * 50
                else:
                    # For other factors, use directly
                    predicted_score += weight * features[factor]
        
        # Ensure score is within bounds
        predicted_score = max(0, min(100, predicted_score))
        
        # Add confidence based on data quality
        confidence = 100
        if features.get('missing_document_rate', 0) > 0.1:
            confidence -= 20
        if features.get('missing_user_rate', 0) > 0.2:
            confidence -= 15
        if features.get('document_balance_rate', 0) < 0.9:
            confidence -= 25
        
        confidence = max(0, min(100, confidence))
        
        prediction_result = {
            'predicted_score': round(predicted_score, 2),
            'confidence': round(confidence, 2),
            'prediction_factors': {
                factor: round(weight * features.get(factor, 0), 3) 
                for factor, weight in score_factors.items() 
                if factor in features
            },
            'prediction_timestamp': timezone.now(),
            'model_type': 'ML_COMPLETENESS_PREDICTOR'
        }
        
        logger.info(f"🎯 Predicted completeness score: {predicted_score:.2f}% (confidence: {confidence:.2f}%)")
        return prediction_result


def analyze_completeness_with_ml(gl_postings, trial_balance_records, coa_records, historical_data=None):
    """
    Main function to analyze completeness using ML
    """
    logger.info("🚀 Starting ML-based completeness analysis...")
    
    analyzer = CompletenessMLAnalyzer()
    
    # Extract features
    features = analyzer.extract_completeness_features(gl_postings, trial_balance_records, coa_records)
    
    # Analyze patterns
    analysis = analyzer.analyze_completeness_patterns(features, historical_data)
    
    # Generate fixes
    fixes = analyzer.generate_fixes(analysis)
    
    # Predict score
    prediction = analyzer.predict_completeness_score(features)
    
    # Combine results
    ml_results = {
        'analysis': analysis,
        'fixes': fixes,
        'prediction': prediction,
        'features': features,
        'timestamp': timezone.now()
    }
    
    logger.info("✅ ML-based completeness analysis completed")
    return ml_results
