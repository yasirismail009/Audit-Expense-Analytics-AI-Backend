"""
AI-Powered Risk Recommendation System
Advanced machine learning and NLP-based risk assessment and recommendations
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from decimal import Decimal
import warnings
warnings.filterwarnings('ignore')

from django.db.models import Q, Count, Sum, Avg, Max, Min
from django.utils import timezone
from django.core.cache import cache

from .models import (
    SAPGLPosting, RiskScoringDocument, DuplicateAnalysisResult, 
    BackdatedAnalysisResult, UserAnalysisResult, HolidayAnalysisResult,
    UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult, 
    OverallAnalysisResult, GeneralAnalysisResult, ManualEntryAnalysisResult
)

logger = logging.getLogger(__name__)

class AIRiskRecommendationEngine:
    """
    Advanced AI-powered risk recommendation engine
    Uses multiple ML models and NLP techniques for comprehensive risk assessment
    """
    
    def __init__(self, data_file_id: str):
        self.data_file_id = data_file_id
        self.scaler = StandardScaler()
        self.models = {}
        self.feature_importance = {}
        self.risk_thresholds = {
            'low': 0.3,
            'medium': 0.6,
            'high': 0.8,
            'critical': 0.9
        }
        
    def generate_comprehensive_risk_assessment(self) -> Dict[str, Any]:
        """
        Generate comprehensive AI-powered risk assessment
        """
        try:
            # Get all analysis results
            analysis_results = self._get_all_analysis_results()
            
            # Extract features for ML models
            features = self._extract_advanced_features(analysis_results)
            
            # Train and apply ML models
            ml_predictions = self._apply_ml_models(features)
            
            # Generate NLP-based insights
            nlp_insights = self._generate_nlp_insights(analysis_results)
            
            # Create anomaly clusters
            anomaly_clusters = self._create_anomaly_clusters(features)
            
            # Generate intelligent recommendations
            recommendations = self._generate_ai_recommendations(
                analysis_results, ml_predictions, nlp_insights, anomaly_clusters
            )
            
            # Calculate advanced risk metrics
            risk_metrics = self._calculate_advanced_risk_metrics(
                analysis_results, ml_predictions
            )
            
            return {
                'ai_risk_assessment': {
                    'overall_ai_risk_score': risk_metrics['overall_ai_risk_score'],
                    'ai_confidence_score': risk_metrics['ai_confidence_score'],
                    'risk_level': risk_metrics['risk_level'],
                    'risk_trends': risk_metrics['risk_trends'],
                    'anomaly_clusters': anomaly_clusters,
                    'ml_predictions': ml_predictions,
                    'nlp_insights': nlp_insights,
                },
                'ai_recommendations': recommendations,
                'feature_importance': self.feature_importance,
                'model_performance': self._get_model_performance(),
                'generated_at': timezone.now().isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Error in AI risk assessment: {e}")
            return {'error': str(e)}
    
    def _get_all_analysis_results(self) -> Dict[str, Any]:
        """Get all analysis results for the data file"""
        from .models import DataFile
        
        data_file = DataFile.objects.get(id=self.data_file_id)
        
        return {
            'general_analysis': GeneralAnalysisResult.objects.filter(data_file=data_file).first(),
            'duplicate_analysis': DuplicateAnalysisResult.objects.filter(data_file=data_file).first(),
            'backdated_analysis': BackdatedAnalysisResult.objects.filter(data_file=data_file).first(),
            'user_analysis': UserAnalysisResult.objects.filter(data_file=data_file).first(),
            'holiday_analysis': HolidayAnalysisResult.objects.filter(data_file=data_file).first(),
            'unusual_days_analysis': UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first(),
            'closing_entries_analysis': ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first(),
            'overall_analysis': OverallAnalysisResult.objects.filter(data_file=data_file).first(),
            'manual_entry_analysis': ManualEntryAnalysisResult.objects.filter(data_file=data_file).first(),
            'risk_scoring': RiskScoringDocument.objects.filter(data_file=data_file).first(),
        }
    
    def _extract_advanced_features(self, analysis_results: Dict[str, Any]) -> pd.DataFrame:
        """Extract advanced features for ML models"""
        features = []
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file_id=self.data_file_id)
        
        for transaction in transactions:
            feature_vector = self._extract_transaction_features(transaction, analysis_results)
            features.append(feature_vector)
        
        return pd.DataFrame(features)
    
    def _extract_transaction_features(self, transaction: SAPGLPosting, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive features for a single transaction"""
        
        # Basic transaction features
        features = {
            'amount': float(transaction.amount_local_currency),
            'amount_log': np.log(float(transaction.amount_local_currency) + 1),
            'is_high_value': 1 if transaction.is_high_value else 0,
            'is_manual_entry': 1 if transaction.is_manual_entry else 0,
            'is_period_end_adjustment': 1 if transaction.is_period_end_adjustment else 0,
            'is_expense_account': 1 if transaction.is_expense_account else 0,
            'has_arabic_text': 1 if transaction.has_arabic_text else 0,
            'is_cleared': 1 if transaction.is_cleared else 0,
            
            # Risk scores from existing analyses
            'overall_risk_score': transaction.overall_risk_score,
            'expense_risk_score': transaction.expense_risk_score,
            'duplicate_risk_score': transaction.duplicate_risk_score,
            'backdated_risk_score': transaction.backdated_risk_score,
            'holiday_risk_score': transaction.holiday_risk_score,
            'unusual_days_risk_score': transaction.unusual_days_risk_score,
            'user_anomaly_risk_score': transaction.user_anomaly_risk_score,
            'closing_entry_risk_score': transaction.closing_entry_risk_score,
            
            # Temporal features
            'day_of_week': transaction.posting_date.weekday(),
            'day_of_month': transaction.posting_date.day,
            'month': transaction.posting_date.month,
            'quarter': (transaction.posting_date.month - 1) // 3 + 1,
            'is_month_end': 1 if transaction.posting_date.day >= 25 else 0,
            'is_quarter_end': 1 if transaction.posting_date.month in [3, 6, 9, 12] and transaction.posting_date.day >= 25 else 0,
            'is_year_end': 1 if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25 else 0,
            
            # Account features
            'account_first_digit': int(transaction.gl_account[0]) if transaction.gl_account else 0,
            'account_category': self._get_account_category(transaction.gl_account),
            
            # User features
            'user_activity_level': self._get_user_activity_level(transaction.user_name),
            'user_risk_profile': self._get_user_risk_profile(transaction.user_name),
        }
        
        # Add anomaly flags
        features.update({
            'is_duplicate': 1 if transaction.is_duplicate else 0,
            'is_backdated': 1 if transaction.is_backdated else 0,
            'is_holiday_posting': 1 if transaction.is_holiday_posting else 0,
            'is_unusual_days_posting': 1 if transaction.is_unusual_days_posting else 0,
            'is_user_anomaly': 1 if transaction.is_user_anomaly else 0,
            'is_closing_entry': 1 if transaction.is_closing_entry else 0,
        })
        
        return features
    
    def _get_account_category(self, gl_account: str) -> int:
        """Get account category based on GL account number"""
        if not gl_account:
            return 0
        
        first_digit = int(gl_account[0])
        if first_digit in [1, 2, 3, 4]:  # Assets, Liabilities, Equity, Revenue
            return 1
        elif first_digit in [5, 6, 7]:  # Expenses
            return 2
        else:
            return 0
    
    def _get_user_activity_level(self, user_name: str) -> float:
        """Calculate user activity level"""
        if not user_name:
            return 0.0
        
        # Get user's transaction count and total amount
        user_transactions = SAPGLPosting.objects.filter(
            data_file_id=self.data_file_id,
            user_name=user_name
        )
        
        transaction_count = user_transactions.count()
        total_amount = user_transactions.aggregate(
            total=Sum('amount_local_currency')
        )['total'] or Decimal('0.00')
        
        # Normalize based on overall statistics
        all_transactions = SAPGLPosting.objects.filter(data_file_id=self.data_file_id)
        avg_count = all_transactions.values('user_name').annotate(
            count=Count('id')
        ).aggregate(avg=Avg('count'))['avg'] or 1
        
        avg_amount = all_transactions.aggregate(
            avg=Avg('amount_local_currency')
        )['avg'] or Decimal('0.00')
        
        activity_score = (transaction_count / avg_count + float(total_amount) / float(avg_amount)) / 2
        return min(activity_score, 10.0)  # Cap at 10
    
    def _get_user_risk_profile(self, user_name: str) -> float:
        """Calculate user risk profile"""
        if not user_name:
            return 0.0
        
        user_transactions = SAPGLPosting.objects.filter(
            data_file_id=self.data_file_id,
            user_name=user_name
        )
        
        # Calculate risk indicators
        high_value_count = user_transactions.filter(
            amount_local_currency__gt=Decimal('1000000.00')
        ).count()
        
        manual_entries = user_transactions.filter(
            is_manual_entry=True
        ).count()
        
        holiday_postings = user_transactions.filter(
            is_holiday_posting=True
        ).count()
        
        total_transactions = user_transactions.count()
        
        if total_transactions == 0:
            return 0.0
        
        risk_score = (
            (high_value_count / total_transactions) * 0.4 +
            (manual_entries / total_transactions) * 0.3 +
            (holiday_postings / total_transactions) * 0.3
        ) * 100
        
        return min(risk_score, 100.0)
    
    def _apply_ml_models(self, features: pd.DataFrame) -> Dict[str, Any]:
        """Apply multiple ML models for risk prediction"""
        
        # Prepare features
        feature_columns = [col for col in features.columns if col not in ['transaction_id']]
        X = features[feature_columns].fillna(0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train Random Forest for risk classification
        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        
        # Create synthetic labels for training (based on existing risk scores)
        y = features['overall_risk_score'].apply(
            lambda x: 'high' if x > 70 else 'medium' if x > 40 else 'low'
        )
        
        rf_model.fit(X_scaled, y)
        
        # Store model and feature importance
        self.models['random_forest'] = rf_model
        self.feature_importance['random_forest'] = dict(zip(feature_columns, rf_model.feature_importances_))
        
        # Make predictions
        predictions = rf_model.predict(X_scaled)
        probabilities = rf_model.predict_proba(X_scaled)
        
        # Train Gradient Boosting for regression
        gb_model = GradientBoostingRegressor(n_estimators=100, random_state=42)
        gb_model.fit(X_scaled, features['overall_risk_score'])
        
        self.models['gradient_boosting'] = gb_model
        self.feature_importance['gradient_boosting'] = dict(zip(feature_columns, gb_model.feature_importances_))
        
        # Predict risk scores
        predicted_scores = gb_model.predict(X_scaled)
        
        return {
            'risk_classifications': predictions.tolist(),
            'risk_probabilities': probabilities.tolist(),
            'predicted_risk_scores': predicted_scores.tolist(),
            'model_confidence': self._calculate_model_confidence(probabilities),
        }
    
    def _calculate_model_confidence(self, probabilities: np.ndarray) -> float:
        """Calculate model confidence based on prediction probabilities"""
        # Use the maximum probability for each prediction as confidence
        max_probs = np.max(probabilities, axis=1)
        return float(np.mean(max_probs))
    
    def _generate_nlp_insights(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate NLP-based insights from analysis results"""
        
        insights = {
            'risk_patterns': [],
            'anomaly_descriptions': [],
            'trend_analysis': [],
            'key_findings': [],
        }
        
        # Analyze risk patterns
        if analysis_results['risk_scoring']:
            risk_doc = analysis_results['risk_scoring']
            insights['risk_patterns'].extend([
                f"Overall risk score: {risk_doc.overall_risk_score:.2f}",
                f"Risk level: {risk_doc.get_risk_level()}",
                f"High-risk transactions: {risk_doc.high_risk_transactions}",
                f"Critical-risk transactions: {risk_doc.critical_risk_transactions}",
            ])
        
        # Analyze anomalies
        anomaly_counts = {
            'duplicates': 0,
            'backdated': 0,
            'holiday_postings': 0,
            'unusual_days': 0,
            'manual_entries': 0,
            'closing_entries': 0,
        }
        
        transactions = SAPGLPosting.objects.filter(data_file_id=self.data_file_id)
        
        anomaly_counts['duplicates'] = transactions.filter(is_duplicate=True).count()
        anomaly_counts['backdated'] = transactions.filter(is_backdated=True).count()
        anomaly_counts['holiday_postings'] = transactions.filter(is_holiday_posting=True).count()
        anomaly_counts['unusual_days'] = transactions.filter(is_unusual_days_posting=True).count()
        anomaly_counts['manual_entries'] = transactions.filter(is_manual_entry=True).count()
        anomaly_counts['closing_entries'] = transactions.filter(is_closing_entry=True).count()
        
        # Generate anomaly descriptions
        for anomaly_type, count in anomaly_counts.items():
            if count > 0:
                percentage = (count / transactions.count()) * 100
                insights['anomaly_descriptions'].append(
                    f"Found {count} {anomaly_type.replace('_', ' ')} ({percentage:.1f}% of transactions)"
                )
        
        # Analyze trends
        monthly_trends = transactions.values('posting_date__month').annotate(
            count=Count('id'),
            avg_amount=Avg('amount_local_currency'),
            avg_risk=Avg('overall_risk_score')
        ).order_by('posting_date__month')
        
        for trend in monthly_trends:
            insights['trend_analysis'].append(
                f"Month {trend['posting_date__month']}: {trend['count']} transactions, "
                f"avg amount: {trend['avg_amount']:.2f}, avg risk: {trend['avg_risk']:.2f}"
            )
        
        # Key findings
        total_transactions = transactions.count()
        high_value_transactions = transactions.filter(is_high_value=True).count()
        high_risk_transactions = transactions.filter(overall_risk_score__gt=70).count()
        
        insights['key_findings'].extend([
            f"Total transactions analyzed: {total_transactions}",
            f"High-value transactions (>1M SAR): {high_value_transactions} ({high_value_transactions/total_transactions*100:.1f}%)",
            f"High-risk transactions (>70 score): {high_risk_transactions} ({high_risk_transactions/total_transactions*100:.1f}%)",
        ])
        
        return insights
    
    def _create_anomaly_clusters(self, features: pd.DataFrame) -> Dict[str, Any]:
        """Create clusters of similar anomalies"""
        
        # Select anomaly-related features
        anomaly_features = [
            'is_duplicate', 'is_backdated', 'is_holiday_posting', 
            'is_unusual_days_posting', 'is_user_anomaly', 'is_closing_entry',
            'is_manual_entry', 'is_high_value'
        ]
        
        X_anomaly = features[anomaly_features].fillna(0)
        
        # Apply DBSCAN clustering
        dbscan = DBSCAN(eps=0.3, min_samples=5)
        clusters = dbscan.fit_predict(X_anomaly)
        
        # Analyze clusters
        cluster_analysis = {}
        for cluster_id in set(clusters):
            if cluster_id == -1:  # Noise points
                continue
            
            cluster_mask = clusters == cluster_id
            cluster_data = X_anomaly[cluster_mask]
            
            cluster_analysis[f'cluster_{cluster_id}'] = {
                'size': int(cluster_mask.sum()),
                'anomaly_profile': cluster_data.mean().to_dict(),
                'common_characteristics': self._identify_cluster_characteristics(cluster_data),
            }
        
        return {
            'total_clusters': len(cluster_analysis),
            'noise_points': int((clusters == -1).sum()),
            'clusters': cluster_analysis,
        }
    
    def _identify_cluster_characteristics(self, cluster_data: pd.DataFrame) -> List[str]:
        """Identify common characteristics of a cluster"""
        characteristics = []
        
        # Check which anomaly types are most common in this cluster
        anomaly_means = cluster_data.mean()
        
        if anomaly_means['is_duplicate'] > 0.5:
            characteristics.append("High duplicate rate")
        if anomaly_means['is_backdated'] > 0.5:
            characteristics.append("High backdated rate")
        if anomaly_means['is_holiday_posting'] > 0.5:
            characteristics.append("High holiday posting rate")
        if anomaly_means['is_manual_entry'] > 0.5:
            characteristics.append("High manual entry rate")
        if anomaly_means['is_high_value'] > 0.5:
            characteristics.append("High-value transactions")
        
        return characteristics
    
    def _generate_ai_recommendations(self, analysis_results: Dict[str, Any], 
                                   ml_predictions: Dict[str, Any], 
                                   nlp_insights: Dict[str, Any],
                                   anomaly_clusters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate AI-powered recommendations"""
        
        recommendations = {
            'immediate_actions': [],
            'investigation_priorities': [],
            'audit_procedures': [],
            'risk_mitigation': [],
            'compliance_actions': [],
            'monitoring_recommendations': [],
        }
        
        # Analyze overall risk level
        overall_risk_score = ml_predictions.get('predicted_risk_scores', [0])
        avg_risk_score = np.mean(overall_risk_score)
        
        if avg_risk_score > 80:
            recommendations['immediate_actions'].extend([
                "🔴 CRITICAL: Immediate investigation required for high-risk transactions",
                "🔴 CRITICAL: Review all manual entries and period-end adjustments",
                "🔴 CRITICAL: Verify authorization for high-value transactions",
            ])
        elif avg_risk_score > 60:
            recommendations['immediate_actions'].extend([
                "🟡 HIGH: Enhanced review of flagged transactions recommended",
                "🟡 HIGH: Investigate unusual posting patterns",
                "🟡 HIGH: Review user access controls",
            ])
        
        # Analyze anomaly clusters
        if anomaly_clusters['total_clusters'] > 0:
            recommendations['investigation_priorities'].append(
                f"🔍 Investigate {anomaly_clusters['total_clusters']} identified anomaly clusters"
            )
            
            for cluster_id, cluster_info in anomaly_clusters['clusters'].items():
                if cluster_info['size'] > 10:  # Large clusters need attention
                    recommendations['investigation_priorities'].append(
                        f"🔍 Priority: Investigate cluster {cluster_id} ({cluster_info['size']} transactions)"
                    )
        
        # Specific recommendations based on analysis results
        transactions = SAPGLPosting.objects.filter(data_file_id=self.data_file_id)
        
        # Duplicate analysis recommendations
        duplicate_count = transactions.filter(is_duplicate=True).count()
        if duplicate_count > 0:
            recommendations['audit_procedures'].append(
                f"📋 Audit Procedure: Review {duplicate_count} duplicate transactions for potential fraud"
            )
        
        # Manual entry recommendations
        manual_count = transactions.filter(is_manual_entry=True).count()
        if manual_count > 0:
            recommendations['risk_mitigation'].append(
                f"🛡️ Risk Mitigation: Implement approval workflow for {manual_count} manual entries"
            )
        
        # Holiday posting recommendations
        holiday_count = transactions.filter(is_holiday_posting=True).count()
        if holiday_count > 0:
            recommendations['compliance_actions'].append(
                f"📋 Compliance: Review {holiday_count} holiday postings for business justification"
            )
        
        # High-value transaction recommendations
        high_value_count = transactions.filter(is_high_value=True).count()
        if high_value_count > 0:
            recommendations['monitoring_recommendations'].append(
                f"👁️ Monitoring: Implement real-time monitoring for high-value transactions ({high_value_count} found)"
            )
        
        return recommendations
    
    def _calculate_advanced_risk_metrics(self, analysis_results: Dict[str, Any], 
                                       ml_predictions: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate advanced risk metrics"""
        
        transactions = SAPGLPosting.objects.filter(data_file_id=self.data_file_id)
        
        # Calculate overall AI risk score
        predicted_scores = ml_predictions.get('predicted_risk_scores', [])
        overall_ai_risk_score = np.mean(predicted_scores) if predicted_scores else 0
        
        # Calculate AI confidence score
        ai_confidence_score = ml_predictions.get('model_confidence', 0)
        
        # Determine risk level
        if overall_ai_risk_score >= 80:
            risk_level = 'CRITICAL'
        elif overall_ai_risk_score >= 60:
            risk_level = 'HIGH'
        elif overall_ai_risk_score >= 30:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        # Calculate risk trends
        monthly_risks = transactions.values('posting_date__month').annotate(
            avg_risk=Avg('overall_risk_score')
        ).order_by('posting_date__month')
        
        risk_trends = []
        for month_data in monthly_risks:
            risk_trends.append({
                'month': month_data['posting_date__month'],
                'avg_risk': float(month_data['avg_risk'] or 0)
            })
        
        return {
            'overall_ai_risk_score': float(overall_ai_risk_score),
            'ai_confidence_score': float(ai_confidence_score),
            'risk_level': risk_level,
            'risk_trends': risk_trends,
        }
    
    def _get_model_performance(self) -> Dict[str, Any]:
        """Get model performance metrics"""
        return {
            'models_trained': list(self.models.keys()),
            'feature_importance': self.feature_importance,
            'training_timestamp': timezone.now().isoformat(),
        }


class AIRiskRecommendationAPI:
    """
    API interface for AI risk recommendations
    """
    
    @staticmethod
    def generate_risk_recommendations(data_file_id: str) -> Dict[str, Any]:
        """Generate AI-powered risk recommendations for a data file"""
        try:
            engine = AIRiskRecommendationEngine(data_file_id)
            return engine.generate_comprehensive_risk_assessment()
        except Exception as e:
            logger.error(f"Error generating AI risk recommendations: {e}")
            return {'error': str(e)}
    
    @staticmethod
    def get_risk_summary(data_file_id: str) -> Dict[str, Any]:
        """Get a summary of AI risk assessment"""
        try:
            engine = AIRiskRecommendationEngine(data_file_id)
            assessment = engine.generate_comprehensive_risk_assessment()
            
            if 'error' in assessment:
                return assessment
            
            return {
                'ai_risk_score': assessment['ai_risk_assessment']['overall_ai_risk_score'],
                'risk_level': assessment['ai_risk_assessment']['risk_level'],
                'confidence_score': assessment['ai_risk_assessment']['ai_confidence_score'],
                'key_recommendations': assessment['ai_recommendations']['immediate_actions'][:3],
                'anomaly_clusters': assessment['ai_risk_assessment']['anomaly_clusters']['total_clusters'],
            }
        except Exception as e:
            logger.error(f"Error getting AI risk summary: {e}")
            return {'error': str(e)}
    
    @staticmethod
    def get_detailed_recommendations(data_file_id: str) -> Dict[str, Any]:
        """Get detailed AI recommendations"""
        try:
            engine = AIRiskRecommendationEngine(data_file_id)
            assessment = engine.generate_comprehensive_risk_assessment()
            
            if 'error' in assessment:
                return assessment
            
            return {
                'recommendations': assessment['ai_recommendations'],
                'insights': assessment['ai_risk_assessment']['nlp_insights'],
                'clusters': assessment['ai_risk_assessment']['anomaly_clusters'],
                'model_performance': assessment['model_performance'],
            }
        except Exception as e:
            logger.error(f"Error getting detailed AI recommendations: {e}")
            return {'error': str(e)}


# Import required for sklearn
from sklearn.ensemble import GradientBoostingRegressor
