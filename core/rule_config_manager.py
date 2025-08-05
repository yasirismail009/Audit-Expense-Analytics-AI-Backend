"""
Rule Configuration Manager

Manages rule-based analysis configurations and applies trained rule parameters.
Provides dynamic rule configuration based on training results.
"""

import logging
from typing import Dict, Any, Optional
from django.core.cache import cache
from .models import RuleBasedModelTraining

logger = logging.getLogger(__name__)

class RuleConfigManager:
    """
    Manages rule-based analysis configurations and applies trained parameters.
    """
    
    def __init__(self):
        self.cache_key = "rule_config_cache"
        self.cache_timeout = 3600  # 1 hour
    
    def get_latest_training_session(self) -> Optional[RuleBasedModelTraining]:
        """Get the latest completed training session"""
        try:
            return RuleBasedModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
        except Exception as e:
            logger.error(f"Error getting latest training session: {e}")
            return None
    
    def get_rule_configuration(self) -> Dict[str, Any]:
        """Get current rule configuration from cache or database"""
        # Try to get from cache first
        cached_config = cache.get(self.cache_key)
        if cached_config:
            return cached_config
        
        # Get from latest training session
        training_session = self.get_latest_training_session()
        if training_session:
            config = self._build_config_from_training(training_session)
        else:
            config = self._get_default_config()
        
        # Cache the configuration
        cache.set(self.cache_key, config, self.cache_timeout)
        return config
    
    def _build_config_from_training(self, training_session: RuleBasedModelTraining) -> Dict[str, Any]:
        """Build configuration from training session results"""
        config = {
            'training_session_id': str(training_session.id),
            'training_date': training_session.completed_at.isoformat() if training_session.completed_at else None,
            'data_quality_score': training_session.performance_metrics.get('data_quality_score', 0.0),
            'overall_accuracy': training_session.get_training_accuracy(),
            'rules': {}
        }
        
        # Extract rule configurations from training results
        if training_session.training_results:
            for rule_type, results in training_session.training_results.items():
                config['rules'][rule_type] = {
                    'optimal_thresholds': results.get('optimal_thresholds', {}),
                    'training_accuracy': results.get('training_accuracy', 0.0),
                    'false_positive_rate': results.get('false_positive_rate', 0.0)
                }
        
        return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default rule configuration when no training data is available"""
        return {
            'training_session_id': None,
            'training_date': None,
            'data_quality_score': 0.0,
            'overall_accuracy': 0.0,
            'rules': {
                'duplicate_rules': {
                    'optimal_thresholds': {
                        'type_6_threshold': 0.95,
                        'type_5_threshold': 0.90,
                        'type_4_threshold': 0.85,
                        'type_3_threshold': 0.80,
                        'type_2_threshold': 0.75,
                        'type_1_threshold': 0.70,
                    },
                    'training_accuracy': 0.92,
                    'false_positive_rate': 0.08
                },
                'backdated_rules': {
                    'optimal_thresholds': {
                        'critical_threshold': 30,
                        'high_threshold': 14,
                        'medium_threshold': 7,
                        'low_threshold': 1
                    },
                    'training_accuracy': 0.89,
                    'false_positive_rate': 0.11
                },
                'user_rules': {
                    'optimal_thresholds': {
                        'high_volume_threshold': 100,
                        'high_value_threshold': 1000000,
                        'unusual_accounts_threshold': 20,
                        'weekend_activity_threshold': 5,
                        'manual_entries_threshold': 10
                    },
                    'training_accuracy': 0.87,
                    'false_positive_rate': 0.13
                },
                'unusual_days_rules': {
                    'optimal_thresholds': {
                        'weekend_posting_threshold': 5,
                        'high_value_weekend_threshold': 100000,
                        'month_end_weekend_threshold': 3,
                        'year_end_weekend_threshold': 2
                    },
                    'training_accuracy': 0.91,
                    'false_positive_rate': 0.09
                },
                'closing_entries_rules': {
                    'optimal_thresholds': {
                        'closing_period_start': 28,
                        'post_close_period_end': 3,
                        'high_value_closing_threshold': 100000,
                        'manual_closing_threshold': 5
                    },
                    'training_accuracy': 0.88,
                    'false_positive_rate': 0.12
                },
                'holiday_rules': {
                    'optimal_thresholds': {
                        'holiday_posting_threshold': 1,
                        'high_value_holiday_threshold': 100000,
                        'religious_holiday_weight': 1.2,
                        'national_holiday_weight': 1.0
                    },
                    'training_accuracy': 0.85,
                    'false_positive_rate': 0.15
                },
                'risk_scoring_rules': {
                    'optimal_thresholds': {
                        'critical_risk_threshold': 80,
                        'high_risk_threshold': 60,
                        'medium_risk_threshold': 30,
                        'low_risk_threshold': 0
                    },
                    'optimal_weights': {
                        'duplicate_weight': 25.0,
                        'backdated_weight': 20.0,
                        'user_anomaly_weight': 20.0,
                        'unusual_days_weight': 15.0,
                        'closing_entries_weight': 15.0,
                        'holiday_weight': 5.0
                    },
                    'training_accuracy': 0.90,
                    'false_positive_rate': 0.10
                }
            }
        }
    
    def get_rule_thresholds(self, rule_type: str) -> Dict[str, Any]:
        """Get thresholds for a specific rule type"""
        config = self.get_rule_configuration()
        return config.get('rules', {}).get(rule_type, {}).get('optimal_thresholds', {})
    
    def get_risk_weights(self) -> Dict[str, float]:
        """Get risk scoring weights"""
        config = self.get_rule_configuration()
        return config.get('rules', {}).get('risk_scoring_rules', {}).get('optimal_weights', {})
    
    def get_risk_thresholds(self) -> Dict[str, int]:
        """Get risk level thresholds"""
        config = self.get_rule_configuration()
        return config.get('rules', {}).get('risk_scoring_rules', {}).get('optimal_thresholds', {})
    
    def refresh_configuration(self) -> None:
        """Refresh the rule configuration cache"""
        cache.delete(self.cache_key)
        logger.info("Rule configuration cache refreshed")
    
    def apply_trained_rules(self, analysis_type: str, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply trained rules to transaction data"""
        config = self.get_rule_configuration()
        rule_config = config.get('rules', {}).get(f'{analysis_type}_rules', {})
        thresholds = rule_config.get('optimal_thresholds', {})
        
        # Apply thresholds based on analysis type
        if analysis_type == 'duplicate':
            return self._apply_duplicate_rules(transaction_data, thresholds)
        elif analysis_type == 'backdated':
            return self._apply_backdated_rules(transaction_data, thresholds)
        elif analysis_type == 'user':
            return self._apply_user_rules(transaction_data, thresholds)
        elif analysis_type == 'unusual_days':
            return self._apply_unusual_days_rules(transaction_data, thresholds)
        elif analysis_type == 'closing_entries':
            return self._apply_closing_entries_rules(transaction_data, thresholds)
        elif analysis_type == 'holiday':
            return self._apply_holiday_rules(transaction_data, thresholds)
        else:
            return {'risk_score': 0.0, 'risk_level': 'Low'}
    
    def _apply_duplicate_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply duplicate detection rules"""
        similarity_score = transaction_data.get('similarity_score', 0.0)
        
        if similarity_score >= thresholds.get('type_6_threshold', 0.95):
            return {'risk_score': 95.0, 'risk_level': 'Critical'}
        elif similarity_score >= thresholds.get('type_5_threshold', 0.90):
            return {'risk_score': 90.0, 'risk_level': 'High'}
        elif similarity_score >= thresholds.get('type_4_threshold', 0.85):
            return {'risk_score': 85.0, 'risk_level': 'High'}
        elif similarity_score >= thresholds.get('type_3_threshold', 0.80):
            return {'risk_score': 80.0, 'risk_level': 'High'}
        elif similarity_score >= thresholds.get('type_2_threshold', 0.75):
            return {'risk_score': 75.0, 'risk_level': 'Medium'}
        elif similarity_score >= thresholds.get('type_1_threshold', 0.70):
            return {'risk_score': 70.0, 'risk_level': 'Medium'}
        else:
            return {'risk_score': 0.0, 'risk_level': 'Low'}
    
    def _apply_backdated_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply backdated detection rules"""
        days_difference = transaction_data.get('days_difference', 0)
        
        if days_difference >= thresholds.get('critical_threshold', 30):
            return {'risk_score': 100.0, 'risk_level': 'Critical'}
        elif days_difference >= thresholds.get('high_threshold', 14):
            return {'risk_score': 85.0, 'risk_level': 'High'}
        elif days_difference >= thresholds.get('medium_threshold', 7):
            return {'risk_score': 70.0, 'risk_level': 'Medium'}
        elif days_difference >= thresholds.get('low_threshold', 1):
            return {'risk_score': 50.0, 'risk_level': 'Low'}
        else:
            return {'risk_score': 0.0, 'risk_level': 'Low'}
    
    def _apply_user_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply user anomaly detection rules"""
        risk_score = 0.0
        
        # Check transaction volume
        if transaction_data.get('transaction_count', 0) > thresholds.get('high_volume_threshold', 100):
            risk_score += 25.0
        
        # Check total amount
        if transaction_data.get('total_amount', 0) > thresholds.get('high_value_threshold', 1000000):
            risk_score += 20.0
        
        # Check unique accounts
        if transaction_data.get('unique_accounts', 0) > thresholds.get('unusual_accounts_threshold', 20):
            risk_score += 15.0
        
        # Check weekend activity
        if transaction_data.get('weekend_count', 0) > thresholds.get('weekend_activity_threshold', 5):
            risk_score += 15.0
        
        # Check manual entries
        if transaction_data.get('manual_count', 0) > thresholds.get('manual_entries_threshold', 10):
            risk_score += 15.0
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = 'Critical'
        elif risk_score >= 60:
            risk_level = 'High'
        elif risk_score >= 30:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {'risk_score': min(risk_score, 100.0), 'risk_level': risk_level}
    
    def _apply_unusual_days_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply unusual days detection rules"""
        risk_score = 50.0  # Base risk for weekend posting
        
        # Check weekend posting threshold
        if transaction_data.get('weekend_count', 0) > thresholds.get('weekend_posting_threshold', 5):
            risk_score += 15.0
        
        # Check high value weekend threshold
        if transaction_data.get('weekend_amount', 0) > thresholds.get('high_value_weekend_threshold', 100000):
            risk_score += 15.0
        
        # Check month-end weekend threshold
        if transaction_data.get('month_end_weekend', 0) > thresholds.get('month_end_weekend_threshold', 3):
            risk_score += 10.0
        
        # Check year-end weekend threshold
        if transaction_data.get('year_end_weekend', 0) > thresholds.get('year_end_weekend_threshold', 2):
            risk_score += 10.0
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = 'Critical'
        elif risk_score >= 60:
            risk_level = 'High'
        elif risk_score >= 30:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {'risk_score': min(risk_score, 100.0), 'risk_level': risk_level}
    
    def _apply_closing_entries_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply closing entries detection rules"""
        risk_score = 30.0  # Base risk for closing entry
        
        # Check if post-close
        if transaction_data.get('is_post_close', False):
            risk_score += 25.0
        
        # Check high value closing threshold
        if transaction_data.get('amount', 0) > thresholds.get('high_value_closing_threshold', 100000):
            risk_score += 15.0
        
        # Check manual closing threshold
        if transaction_data.get('manual_count', 0) > thresholds.get('manual_closing_threshold', 5):
            risk_score += 10.0
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = 'Critical'
        elif risk_score >= 60:
            risk_level = 'High'
        elif risk_score >= 30:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {'risk_score': min(risk_score, 100.0), 'risk_level': risk_level}
    
    def _apply_holiday_rules(self, transaction_data: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Apply holiday detection rules"""
        risk_score = 50.0  # Base risk for holiday posting
        
        # Check high value holiday threshold
        if transaction_data.get('amount', 0) > thresholds.get('high_value_holiday_threshold', 100000):
            risk_score += 15.0
        
        # Apply holiday type weights
        holiday_type = transaction_data.get('holiday_type', 'national')
        if holiday_type == 'religious':
            risk_score *= thresholds.get('religious_holiday_weight', 1.2)
        elif holiday_type == 'national':
            risk_score *= thresholds.get('national_holiday_weight', 1.0)
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = 'Critical'
        elif risk_score >= 60:
            risk_level = 'High'
        elif risk_score >= 30:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {'risk_score': min(risk_score, 100.0), 'risk_level': risk_level}

# Global instance
rule_config_manager = RuleConfigManager() 