"""
Centralized Risk Configuration Management System
Manages all risk thresholds, scoring parameters, and configuration settings
for the Journal Entry Testing (JET) system.
"""

import json
import logging
from django.conf import settings
from django.core.cache import cache
from typing import Dict, Any, Optional, List
from decimal import Decimal

logger = logging.getLogger(__name__)

class RiskConfigManager:
    """
    Centralized risk configuration management for JET system.
    
    Manages:
    - Risk thresholds for all analysis types
    - Scoring weights and parameters
    - Management override indicators
    - Period-end adjustment thresholds
    - Manual entry detection rules
    """
    
    def __init__(self):
        self.config_cache_key = 'jet_risk_config'
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from cache or default values"""
        cached_config = cache.get(self.config_cache_key)
        if cached_config:
            return cached_config
        
        # Default configuration
        default_config = {
            'risk_thresholds': {
                'critical': 80,
                'high': 60,
                'medium': 30,
                'low': 0
            },
            'scoring_weights': {
                'duplicate_analysis': 25.0,
                'backdated_analysis': 20.0,
                'user_analysis': 20.0,
                'unusual_days_analysis': 15.0,
                'closing_entries_analysis': 15.0,
                'holiday_analysis': 5.0,
                'manual_entry_analysis': 30.0  # High weight for management override risk
            },
            'manual_entry_detection': {
                'document_type_indicators': [
                    'MANUAL', 'ADJUSTMENT', 'CORRECTION', 'REVERSAL', 'AJUST'
                ],
                'text_indicators': [
                    'manual', 'adjustment', 'correction', 'reversal', 'ajust', 'manual entry'
                ],
                'suspicious_accounts': [
                    '999999', '888888', '777777', '666666', '555555'
                ],
                'suspicious_users': [
                    'ADMIN', 'SYSTEM', 'MANUAL', 'ADJUST', 'CORRECT'
                ]
            },
            'period_end_adjustments': {
                'month_end_days': [28, 29, 30, 31],
                'month_start_days': [1, 2, 3],
                'fiscal_year_end_periods': [12, 16],
                'quarter_end_months': [3, 6, 9, 12]
            },
            'management_override_indicators': {
                'high_value_threshold': 1000000,  # 1M SAR
                'weekend_posting_risk': True,
                'holiday_posting_risk': True,
                'backdated_threshold_days': 7,
                'unusual_account_risk': True,
                'multiple_user_activity': True
            },
            'sampling_strategy': {
                'high_risk_coverage': 1.0,      # 100% of high-risk entries
                'medium_risk_coverage': 0.5,    # 50% of medium-risk entries
                'low_risk_coverage': 0.1,       # 10% of low-risk entries
                'minimum_sample_size': 100,
                'maximum_sample_size': 10000
            },
            'compliance_thresholds': {
                'management_override_risk_high': 0.25,  # 25% of entries
                'period_end_adjustment_risk_high': 0.30,  # 30% of entries
                'manual_entry_risk_high': 0.20,  # 20% of entries
                'overall_risk_high': 0.40       # 40% of entries
            }
        }
        
        # Cache the default configuration
        cache.set(self.config_cache_key, default_config, timeout=3600)  # 1 hour
        return default_config
    
    def get_risk_threshold(self, risk_level: str) -> int:
        """Get risk threshold for a specific risk level"""
        return self.config['risk_thresholds'].get(risk_level.lower(), 0)
    
    def get_scoring_weight(self, analysis_type: str) -> float:
        """Get scoring weight for a specific analysis type"""
        return self.config['scoring_weights'].get(analysis_type, 0.0)
    
    def get_manual_entry_indicators(self) -> Dict[str, List[str]]:
        """Get manual entry detection indicators"""
        return self.config['manual_entry_detection']
    
    def get_period_end_settings(self) -> Dict[str, Any]:
        """Get period-end adjustment settings"""
        return self.config['period_end_adjustments']
    
    def get_management_override_settings(self) -> Dict[str, Any]:
        """Get management override detection settings"""
        return self.config['management_override_indicators']
    
    def get_sampling_strategy(self) -> Dict[str, float]:
        """Get sampling strategy configuration"""
        return self.config['sampling_strategy']
    
    def get_compliance_thresholds(self) -> Dict[str, float]:
        """Get compliance risk thresholds"""
        return self.config['compliance_thresholds']
    
    def update_config(self, new_config: Dict[str, Any]) -> bool:
        """Update configuration with new values"""
        try:
            # Validate new configuration
            if not self._validate_config(new_config):
                logger.error("Invalid configuration provided")
                return False
            
            # Update configuration
            self.config.update(new_config)
            
            # Update cache
            cache.set(self.config_cache_key, self.config, timeout=3600)
            
            logger.info("Risk configuration updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error updating risk configuration: {e}")
            return False
    
    def _validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration values"""
        try:
            # Validate risk thresholds
            if 'risk_thresholds' in config:
                thresholds = config['risk_thresholds']
                if not all(isinstance(v, int) and 0 <= v <= 100 for v in thresholds.values()):
                    return False
            
            # Validate scoring weights
            if 'scoring_weights' in config:
                weights = config['scoring_weights']
                if not all(isinstance(v, (int, float)) and 0 <= v <= 100 for v in weights.values()):
                    return False
            
            # Validate thresholds
            if 'management_override_indicators' in config:
                indicators = config['management_override_indicators']
                if 'high_value_threshold' in indicators:
                    if not isinstance(indicators['high_value_threshold'], (int, float)) or indicators['high_value_threshold'] < 0:
                        return False
            
            return True
            
        except Exception:
            return False
    
    def calculate_risk_level(self, risk_score: float) -> str:
        """Calculate risk level based on risk score"""
        if risk_score >= self.get_risk_threshold('critical'):
            return 'CRITICAL'
        elif risk_score >= self.get_risk_threshold('high'):
            return 'HIGH'
        elif risk_score >= self.get_risk_threshold('medium'):
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def calculate_sampling_size(self, total_entries: int, risk_distribution: Dict[str, int]) -> Dict[str, int]:
        """Calculate sampling sizes based on risk distribution and strategy"""
        strategy = self.get_sampling_strategy()
        
        sampling_sizes = {}
        
        for risk_level, count in risk_distribution.items():
            if risk_level.upper() == 'CRITICAL' or risk_level.upper() == 'HIGH':
                coverage = strategy['high_risk_coverage']
            elif risk_level.upper() == 'MEDIUM':
                coverage = strategy['medium_risk_coverage']
            else:
                coverage = strategy['low_risk_coverage']
            
            sample_size = int(count * coverage)
            
            # Apply minimum and maximum constraints
            sample_size = max(sample_size, strategy['minimum_sample_size'])
            sample_size = min(sample_size, strategy['maximum_sample_size'])
            
            sampling_sizes[risk_level] = sample_size
        
        return sampling_sizes
    
    def assess_compliance_risk(self, risk_metrics: Dict[str, Any]) -> Dict[str, str]:
        """Assess compliance risk based on current metrics"""
        thresholds = self.get_compliance_thresholds()
        assessment = {}
        
        for metric, threshold in thresholds.items():
            current_value = risk_metrics.get(metric, 0)
            
            if current_value > threshold:
                assessment[metric] = 'HIGH'
            elif current_value > threshold * 0.5:
                assessment[metric] = 'MEDIUM'
            else:
                assessment[metric] = 'LOW'
        
        return assessment
    
    def get_export_config(self) -> Dict[str, Any]:
        """Get configuration for export purposes"""
        return {
            'risk_thresholds': self.config['risk_thresholds'],
            'scoring_weights': self.config['scoring_weights'],
            'sampling_strategy': self.config['sampling_strategy'],
            'compliance_thresholds': self.config['compliance_thresholds']
        }
    
    def reset_to_defaults(self) -> bool:
        """Reset configuration to default values"""
        try:
            cache.delete(self.config_cache_key)
            self.config = self._load_config()
            logger.info("Risk configuration reset to defaults")
            return True
        except Exception as e:
            logger.error(f"Error resetting configuration: {e}")
            return False

# Global instance
risk_config_manager = RiskConfigManager()
