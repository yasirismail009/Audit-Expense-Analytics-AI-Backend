"""
Unified Analysis Utilities
Provides consistent anomaly lists and chart data for all analysis types
"""

from django.core.cache import cache
from django.db.models import Q, Count, Sum, Avg
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class UnifiedAnalysisUtils:
    """Utilities for unified analysis structure"""
    
    @staticmethod
    def create_standardized_anomaly_list(transactions, analysis_type, risk_scores=None):
        """Create standardized anomaly list for any analysis type"""
        if not transactions:
            return []
        
        anomaly_list = []
        for i, transaction in enumerate(transactions):
            # Get risk score if provided, otherwise calculate basic risk
            risk_score = risk_scores[i] if risk_scores and i < len(risk_scores) else 50
            
            anomaly = {
                'id': str(transaction.id),
                'analysis_type': analysis_type,
                'document_number': transaction.document_number,
                'gl_account': transaction.gl_account,
                'user_name': transaction.user_name,
                'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
                'document_date': transaction.document_date.isoformat() if transaction.document_date else None,
                'amount': float(transaction.amount_local_currency),
                'transaction_type': transaction.transaction_type,
                'document_type': transaction.document_type,
                'risk_score': risk_score,
                'risk_level': UnifiedAnalysisUtils._get_risk_level(risk_score),
                'detection_method': 'rule_based',
                'confidence_score': 0.85,
                'audit_priority': UnifiedAnalysisUtils._get_audit_priority(risk_score),
                'compliance_impact': UnifiedAnalysisUtils._get_compliance_impact(risk_score),
                'financial_impact': UnifiedAnalysisUtils._get_financial_impact(transaction.amount_local_currency)
            }
            anomaly_list.append(anomaly)
        
        return anomaly_list
    
    @staticmethod
    def create_standardized_chart_data(transactions, analysis_type):
        """Create standardized chart data for any analysis type"""
        if not transactions:
            return UnifiedAnalysisUtils._empty_chart_data()
        
        # Amount distribution
        amount_ranges = [
            {'min': 0, 'max': 1000, 'label': '0-1K', 'color': '#4BC0C0'},
            {'min': 1000, 'max': 10000, 'label': '1K-10K', 'color': '#FFCE56'},
            {'min': 10000, 'max': 100000, 'label': '10K-100K', 'color': '#FF9F40'},
            {'min': 100000, 'max': 1000000, 'label': '100K-1M', 'color': '#FF6384'},
            {'min': 1000000, 'max': float('inf'), 'label': '1M+', 'color': '#9966FF'}
        ]
        
        amount_distribution = []
        for range_info in amount_ranges:
            count = len([t for t in transactions 
                        if range_info['min'] <= float(t.amount_local_currency) < range_info['max']])
            amount_distribution.append({
                'range': range_info['label'],
                'count': count,
                'percentage': (count / len(transactions) * 100) if transactions else 0,
                'color': range_info['color']
            })
        
        # Account distribution
        account_counts = {}
        for transaction in transactions:
            account = transaction.gl_account
            if account not in account_counts:
                account_counts[account] = {'count': 0, 'amount': 0}
            account_counts[account]['count'] += 1
            account_counts[account]['amount'] += float(transaction.amount_local_currency)
        
        top_accounts = sorted(account_counts.items(), key=lambda x: x[1]['amount'], reverse=True)[:10]
        
        # User distribution
        user_counts = {}
        for transaction in transactions:
            user = transaction.user_name
            if user not in user_counts:
                user_counts[user] = {'count': 0, 'amount': 0}
            user_counts[user]['count'] += 1
            user_counts[user]['amount'] += float(transaction.amount_local_currency)
        
        top_users = sorted(user_counts.items(), key=lambda x: x[1]['amount'], reverse=True)[:10]
        
        # Date distribution (monthly)
        monthly_counts = {}
        for transaction in transactions:
            if transaction.posting_date:
                month_key = transaction.posting_date.strftime('%Y-%m')
                if month_key not in monthly_counts:
                    monthly_counts[month_key] = {'count': 0, 'amount': 0}
                monthly_counts[month_key]['count'] += 1
                monthly_counts[month_key]['amount'] += float(transaction.amount_local_currency)
        
        monthly_data = sorted(monthly_counts.items())
        
        return {
            'amount_distribution': {
                'labels': [item['range'] for item in amount_distribution],
                'data': [item['count'] for item in amount_distribution],
                'colors': [item['color'] for item in amount_distribution],
                'title': f'{analysis_type} - Amount Distribution'
            },
            'top_accounts': {
                'labels': [account for account, _ in top_accounts],
                'data': [data['amount'] for _, data in top_accounts],
                'title': f'{analysis_type} - Top Accounts by Amount'
            },
            'top_users': {
                'labels': [user for user, _ in top_users],
                'data': [data['amount'] for _, data in top_users],
                'title': f'{analysis_type} - Top Users by Amount'
            },
            'monthly_trend': {
                'labels': [month for month, _ in monthly_data],
                'data': [data['count'] for _, data in monthly_data],
                'title': f'{analysis_type} - Monthly Transaction Trend'
            }
        }
    
    @staticmethod
    def create_standardized_risk_assessment(anomaly_list, total_transactions):
        """Create standardized risk assessment"""
        if not anomaly_list:
            return {
                'overall_risk_score': 0,
                'overall_risk_level': 'LOW',
                'risk_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0},
                'risk_factors': [],
                'total_anomalies': 0,
                'anomaly_percentage': 0
            }
        
        # Calculate risk distribution
        risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        total_risk_score = 0
        risk_factors = []
        
        for anomaly in anomaly_list:
            risk_score = anomaly.get('risk_score', 0)
            risk_level = anomaly.get('risk_level', 'low').lower()
            risk_distribution[risk_level] = risk_distribution.get(risk_level, 0) + 1
            total_risk_score += risk_score
        
        # Calculate overall risk score
        overall_risk_score = total_risk_score / len(anomaly_list) if anomaly_list else 0
        overall_risk_level = UnifiedAnalysisUtils._get_risk_level(overall_risk_score)
        
        # Identify risk factors
        high_value_count = len([a for a in anomaly_list if a.get('amount', 0) > 1000000])
        if high_value_count > 0:
            risk_factors.append({
                'factor': 'High-value anomalies',
                'count': high_value_count,
                'impact': 'HIGH'
            })
        
        critical_count = risk_distribution.get('critical', 0)
        if critical_count > 0:
            risk_factors.append({
                'factor': 'Critical risk anomalies',
                'count': critical_count,
                'impact': 'CRITICAL'
            })
        
        return {
            'overall_risk_score': overall_risk_score,
            'overall_risk_level': overall_risk_level,
            'risk_distribution': risk_distribution,
            'risk_factors': risk_factors,
            'total_anomalies': len(anomaly_list),
            'anomaly_percentage': (len(anomaly_list) / total_transactions * 100) if total_transactions > 0 else 0
        }
    
    @staticmethod
    def create_standardized_audit_recommendations(anomaly_list, analysis_type):
        """Create standardized audit recommendations"""
        recommendations = {
            'high_priority': [],
            'medium_priority': [],
            'low_priority': []
        }
        
        if not anomaly_list:
            return recommendations
        
        # High priority recommendations
        critical_count = len([a for a in anomaly_list if a.get('risk_level') == 'critical'])
        if critical_count > 0:
            recommendations['high_priority'].append({
                'action': 'Immediate investigation required',
                'description': f'Found {critical_count} critical-risk {analysis_type} anomalies',
                'priority': 'CRITICAL',
                'timeline': 'Immediate'
            })
        
        high_value_count = len([a for a in anomaly_list if a.get('amount', 0) > 1000000])
        if high_value_count > 0:
            recommendations['high_priority'].append({
                'action': 'High-value anomaly review',
                'description': f'Found {high_value_count} high-value {analysis_type} anomalies',
                'priority': 'HIGH',
                'timeline': 'Within 48 hours'
            })
        
        # Medium priority recommendations
        if len(anomaly_list) > 10:
            recommendations['medium_priority'].append({
                'action': 'Systematic review',
                'description': f'Review {len(anomaly_list)} {analysis_type} anomalies for patterns',
                'priority': 'MEDIUM',
                'timeline': 'Within 1 week'
            })
        
        # Low priority recommendations
        recommendations['low_priority'].append({
            'action': 'Process improvement',
            'description': f'Consider process improvements to reduce {analysis_type} anomalies',
            'priority': 'LOW',
            'timeline': 'Ongoing'
        })
        
        return recommendations
    
    @staticmethod
    def create_standardized_compliance_assessment(anomaly_list, analysis_type):
        """Create standardized compliance assessment"""
        compliance_issues = []
        
        if not anomaly_list:
            return {'compliance_issues': compliance_issues, 'overall_compliance': 'COMPLIANT'}
        
        # Check for high-value anomalies
        high_value_anomalies = [a for a in anomaly_list if a.get('amount', 0) > 1000000]
        if high_value_anomalies:
            compliance_issues.append({
                'issue': 'High-value anomalies detected',
                'severity': 'HIGH',
                'description': f'Found {len(high_value_anomalies)} high-value {analysis_type} anomalies',
                'regulatory_impact': 'May require regulatory reporting',
                'recommendation': 'Review and investigate immediately'
            })
        
        # Check for critical risk anomalies
        critical_anomalies = [a for a in anomaly_list if a.get('risk_level') == 'critical']
        if critical_anomalies:
            compliance_issues.append({
                'issue': 'Critical risk anomalies detected',
                'severity': 'CRITICAL',
                'description': f'Found {len(critical_anomalies)} critical-risk {analysis_type} anomalies',
                'regulatory_impact': 'May indicate control weaknesses',
                'recommendation': 'Immediate investigation and remediation required'
            })
        
        # Determine overall compliance
        if any(issue['severity'] == 'CRITICAL' for issue in compliance_issues):
            overall_compliance = 'NON_COMPLIANT'
        elif any(issue['severity'] == 'HIGH' for issue in compliance_issues):
            overall_compliance = 'AT_RISK'
        else:
            overall_compliance = 'COMPLIANT'
        
        return {
            'compliance_issues': compliance_issues,
            'overall_compliance': overall_compliance,
            'total_issues': len(compliance_issues)
        }
    
    @staticmethod
    def _get_risk_level(risk_score):
        """Get risk level based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    @staticmethod
    def _get_audit_priority(risk_score):
        """Get audit priority based on risk score"""
        if risk_score >= 80:
            return 'IMMEDIATE'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    @staticmethod
    def _get_compliance_impact(risk_score):
        """Get compliance impact based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    @staticmethod
    def _get_financial_impact(amount):
        """Get financial impact based on amount"""
        amount_float = float(amount)
        if amount_float > 1000000:
            return 'CRITICAL'
        elif amount_float > 100000:
            return 'HIGH'
        elif amount_float > 10000:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    @staticmethod
    def _empty_chart_data():
        """Return empty chart data structure"""
        return {
            'amount_distribution': {
                'labels': [],
                'data': [],
                'colors': [],
                'title': 'No Data Available'
            },
            'top_accounts': {
                'labels': [],
                'data': [],
                'title': 'No Data Available'
            },
            'top_users': {
                'labels': [],
                'data': [],
                'title': 'No Data Available'
            },
            'monthly_trend': {
                'labels': [],
                'data': [],
                'title': 'No Data Available'
            }
        }
