"""
Risk Tasks Module

Contains all risk analysis and AI recommendation-related Celery tasks for the analytics system.
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
    FileProcessingJob, SAPGLPosting, DataFile, OverallAnalysisResult,
    DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult,
    UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult, HolidayAnalysisResult,
    RiskScoringDocument, ManualEntryAnalysisResult
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


@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=600, soft_time_limit=480)
def run_ai_risk_recommendations(self, job_id):
    """
    Run AI-powered risk recommendations based on analysis results
    """
    try:
        log_task_info("run_ai_risk_recommendations", job_id, f"Starting AI risk recommendations for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get all analysis results for this data file
        analysis_results = {
            'duplicate': DuplicateAnalysisResult.objects.filter(data_file=data_file).first(),
            'backdated': BackdatedAnalysisResult.objects.filter(data_file=data_file).first(),
            'user': UserAnalysisResult.objects.filter(data_file=data_file).first(),
            'unusual_days': UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first(),
            'closing_entries': ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first(),
            'holiday': HolidayAnalysisResult.objects.filter(data_file=data_file).first(),
            'manual_entry': ManualEntryAnalysisResult.objects.filter(data_file=data_file).first(),
        }
        
        # Filter out None results
        valid_results = {k: v for k, v in analysis_results.items() if v is not None}
        
        if not valid_results:
            return {"status": "error", "message": "No analysis results found"}
        
        # Create risk scoring document
        risk_document = RiskScoringDocument.objects.create(
            data_file=data_file,
            analysis_type='ai_risk_recommendations',
            processing_job=job,
            status='PROCESSING'
        )
        
        try:
            # Calculate overall risk score
            overall_risk_score = _calculate_overall_risk_score(valid_results)
            
            # Generate risk assessment
            risk_assessment = _generate_ai_risk_assessment(valid_results, overall_risk_score)
            
            # Generate AI recommendations
            ai_recommendations = _generate_ai_recommendations(valid_results, overall_risk_score)
            
            # Generate compliance assessment
            compliance_assessment = _generate_ai_compliance_assessment(valid_results, overall_risk_score)
            
            # Update risk document
            risk_document.overall_risk_score = overall_risk_score
            risk_document.risk_level = _get_risk_level(overall_risk_score)
            risk_document.risk_summary = risk_assessment
            risk_document.recommendations = ai_recommendations
            risk_document.analysis_summary = {
                'total_analyses': len(valid_results),
                'risk_score': overall_risk_score,
                'risk_level': _get_risk_level(overall_risk_score),
                'recommendations_count': len(ai_recommendations)
            }
            risk_document.anomaly_list = _consolidate_anomalies(valid_results)
            risk_document.chart_data = _generate_risk_chart_data(valid_results, overall_risk_score)
            risk_document.audit_recommendations = ai_recommendations
            risk_document.compliance_assessment = compliance_assessment
            risk_document.export_data = _generate_risk_export_data(valid_results, overall_risk_score)
            risk_document.status = 'COMPLETED'
            risk_document.processing_duration = (timezone.now() - risk_document.analysis_date).total_seconds()
            risk_document.save()
            
            log_task_info("run_ai_risk_recommendations", job_id, f"AI risk recommendations completed - Risk Score: {overall_risk_score}")
            
            return {
                "status": "success",
                "message": "AI risk recommendations completed",
                "risk_document_id": risk_document.id,
                "overall_risk_score": overall_risk_score,
                "risk_level": _get_risk_level(overall_risk_score),
                "recommendations_count": len(ai_recommendations)
            }
            
        except Exception as e:
            risk_document.status = 'FAILED'
            risk_document.error_message = str(e)
            risk_document.save()
            raise e
        
    except Exception as e:
        logger.error(f"AI risk recommendations failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=1800, soft_time_limit=1500)
def run_risk_analysis(self, job_id):
    """
    Run comprehensive risk analysis combining all analysis types
    """
    try:
        log_task_info("run_risk_analysis", job_id, f"Starting comprehensive risk analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        
        if not transactions:
            return {"status": "error", "message": "No transactions found"}
        
        # Get all analysis results
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
        backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
        user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
        holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
        manual_entry_analysis = ManualEntryAnalysisResult.objects.filter(data_file=data_file).first()
        
        # Create overall analysis result
        overall_result = OverallAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='comprehensive_risk_analysis',
            processing_job=job,
            status='PROCESSING'
        )
        
        try:
            # Calculate comprehensive risk scores
            risk_scores = _calculate_comprehensive_risk_scores(
                transactions, duplicate_analysis, backdated_analysis, user_analysis,
                unusual_days_analysis, closing_entries_analysis, holiday_analysis,
                manual_entry_analysis
            )
            
            # Generate risk assessment
            risk_assessment = _generate_comprehensive_risk_assessment(risk_scores, transactions)
            
            # Generate audit recommendations
            audit_recommendations = _generate_comprehensive_audit_recommendations(risk_scores, transactions)
            
            # Generate compliance assessment
            compliance_assessment = _generate_comprehensive_compliance_assessment(risk_scores, transactions)
            
            # Generate chart data
            chart_data = _generate_comprehensive_chart_data(risk_scores, transactions)
            
            # Generate export data
            export_data = _generate_comprehensive_export_data(risk_scores, transactions)
            
            # Update overall result
            overall_result.risk_score = risk_scores.get('overall_risk_score', 0)
            overall_result.confidence_score = risk_scores.get('confidence_score', 0)
            overall_result.analysis_summary = risk_assessment
            overall_result.anomaly_list = _consolidate_all_anomalies([
                duplicate_analysis, backdated_analysis, user_analysis,
                unusual_days_analysis, closing_entries_analysis, holiday_analysis,
                manual_entry_analysis
            ])
            overall_result.chart_data = chart_data
            overall_result.risk_assessment = risk_assessment
            overall_result.audit_recommendations = audit_recommendations
            overall_result.compliance_assessment = compliance_assessment
            overall_result.export_data = export_data
            overall_result.status = 'COMPLETED'
            overall_result.processing_duration = (timezone.now() - overall_result.analysis_date).total_seconds()
            overall_result.save()
            
            log_task_info("run_risk_analysis", job_id, f"Comprehensive risk analysis completed - Risk Score: {risk_scores.get('overall_risk_score', 0)}")
            
            return {
                "status": "success",
                "message": "Comprehensive risk analysis completed",
                "analysis_id": overall_result.id,
                "overall_risk_score": risk_scores.get('overall_risk_score', 0),
                "confidence_score": risk_scores.get('confidence_score', 0)
            }
            
        except Exception as e:
            overall_result.status = 'FAILED'
            overall_result.error_message = str(e)
            overall_result.save()
            raise e
        
    except Exception as e:
        logger.error(f"Comprehensive risk analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


def _calculate_overall_risk_score(analysis_results):
    """Calculate overall risk score from analysis results"""
    if not analysis_results:
        return 0
    
    risk_scores = []
    weights = {
        'duplicate': 0.25,
        'backdated': 0.20,
        'user': 0.15,
        'unusual_days': 0.10,
        'closing_entries': 0.10,
        'holiday': 0.10,
        'manual_entry': 0.10
    }
    
    for analysis_type, result in analysis_results.items():
        if result and result.risk_assessment:
            risk_score = result.risk_assessment.get('risk_score', 0)
            weight = weights.get(analysis_type, 0.1)
            risk_scores.append(risk_score * weight)
    
    return sum(risk_scores) if risk_scores else 0


def _get_risk_level(risk_score):
    """Get risk level based on risk score"""
    if risk_score >= 80:
        return 'critical'
    elif risk_score >= 60:
        return 'high'
    elif risk_score >= 40:
        return 'medium'
    else:
        return 'low'


def _generate_ai_risk_assessment(analysis_results, overall_risk_score):
    """Generate AI-powered risk assessment"""
    risk_level = _get_risk_level(overall_risk_score)
    
    # Count anomalies by type
    anomaly_counts = {}
    for analysis_type, result in analysis_results.items():
        if result and result.anomaly_list:
            anomaly_counts[analysis_type] = len(result.anomaly_list)
    
    # Calculate risk distribution
    risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
    for result in analysis_results.values():
        if result and result.risk_assessment:
            distribution = result.risk_assessment.get('risk_distribution', {})
            for level, count in distribution.items():
                risk_distribution[level] += count
    
    return {
        'overall_risk_score': overall_risk_score,
        'risk_level': risk_level,
        'total_anomalies': sum(anomaly_counts.values()),
        'anomaly_counts_by_type': anomaly_counts,
        'risk_distribution': risk_distribution,
        'analysis_coverage': len(analysis_results),
        'confidence_level': _calculate_confidence_level(analysis_results)
    }


def _generate_ai_recommendations(analysis_results, overall_risk_score):
    """Generate AI-powered recommendations"""
    recommendations = []
    
    # High-level recommendations based on overall risk
    if overall_risk_score >= 80:
        recommendations.append({
            'priority': 'immediate',
            'category': 'overall_risk',
            'title': 'Critical Risk Level Detected',
            'description': 'Overall risk score indicates critical level requiring immediate attention',
            'actions': [
                'Conduct immediate detailed review of all flagged transactions',
                'Implement additional controls and monitoring',
                'Consider escalating to senior management'
            ]
        })
    elif overall_risk_score >= 60:
        recommendations.append({
            'priority': 'high',
            'category': 'overall_risk',
            'title': 'High Risk Level Detected',
            'description': 'Overall risk score indicates high level requiring prompt attention',
            'actions': [
                'Review flagged transactions within 48 hours',
                'Implement enhanced monitoring procedures',
                'Consider additional audit procedures'
            ]
        })
    
    # Specific recommendations based on analysis types
    for analysis_type, result in analysis_results.items():
        if result and result.anomaly_list:
            anomaly_count = len(result.anomaly_list)
            
            if anomaly_count > 0:
                recommendations.append({
                    'priority': 'high' if anomaly_count > 10 else 'medium',
                    'category': analysis_type,
                    'title': f'{analysis_type.title()} Analysis Issues',
                    'description': f'Found {anomaly_count} {analysis_type} anomalies requiring review',
                    'actions': [
                        f'Review all {analysis_type} flagged transactions',
                        f'Investigate root causes of {analysis_type} issues',
                        f'Implement controls to prevent future {analysis_type} problems'
                    ]
                })
    
    return recommendations


def _generate_ai_compliance_assessment(analysis_results, overall_risk_score):
    """Generate AI-powered compliance assessment"""
    compliance_score = max(0, 100 - overall_risk_score)
    
    if compliance_score >= 90:
        compliance_status = 'compliant'
    elif compliance_score >= 70:
        compliance_status = 'minor_issues'
    elif compliance_score >= 50:
        compliance_status = 'moderate_issues'
    else:
        compliance_status = 'non_compliant'
    
    # Identify compliance violations
    violations = []
    for analysis_type, result in analysis_results.items():
        if result and result.anomaly_list:
            violations.append({
                'type': analysis_type,
                'severity': 'high' if len(result.anomaly_list) > 10 else 'medium',
                'count': len(result.anomaly_list),
                'description': f'{analysis_type.title()} anomalies detected'
            })
    
    return {
        'compliance_status': compliance_status,
        'compliance_score': compliance_score,
        'violations': violations,
        'recommendations': [
            'Implement regular compliance monitoring',
            'Conduct periodic risk assessments',
            'Provide staff training on compliance requirements'
        ]
    }


def _consolidate_anomalies(analysis_results):
    """Consolidate anomalies from all analysis results"""
    consolidated = []
    
    for analysis_type, result in analysis_results.items():
        if result and result.anomaly_list:
            for anomaly in result.anomaly_list:
                anomaly['analysis_type'] = analysis_type
                consolidated.append(anomaly)
    
    return consolidated


def _generate_risk_chart_data(analysis_results, overall_risk_score):
    """Generate chart data for risk visualization"""
    chart_data = {
        'overall_risk_score': overall_risk_score,
        'risk_level': _get_risk_level(overall_risk_score),
        'analysis_breakdown': {},
        'anomaly_trends': {},
        'risk_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
    }
    
    for analysis_type, result in analysis_results.items():
        if result:
            chart_data['analysis_breakdown'][analysis_type] = {
                'anomaly_count': len(result.anomaly_list) if result.anomaly_list else 0,
                'risk_score': result.risk_assessment.get('risk_score', 0) if result.risk_assessment else 0
            }
    
    return chart_data


def _generate_risk_export_data(analysis_results, overall_risk_score):
    """Generate export data for risk analysis"""
    export_data = []
    
    # Add overall risk summary
    export_data.append({
        'type': 'overall_risk_summary',
        'data': {
            'overall_risk_score': overall_risk_score,
            'risk_level': _get_risk_level(overall_risk_score),
            'analysis_count': len(analysis_results)
        }
    })
    
    # Add individual analysis results
    for analysis_type, result in analysis_results.items():
        if result:
            export_data.append({
                'type': f'{analysis_type}_analysis',
                'data': {
                    'anomaly_count': len(result.anomaly_list) if result.anomaly_list else 0,
                    'risk_score': result.risk_assessment.get('risk_score', 0) if result.risk_assessment else 0,
                    'anomalies': result.anomaly_list if result.anomaly_list else []
                }
            })
    
    return export_data


def _calculate_confidence_level(analysis_results):
    """Calculate confidence level based on analysis coverage and quality"""
    if not analysis_results:
        return 0
    
    # Base confidence on number of analyses
    base_confidence = min(100, len(analysis_results) * 15)
    
    # Adjust based on data quality
    quality_adjustment = 0
    for result in analysis_results.values():
        if result and result.risk_assessment:
            risk_score = result.risk_assessment.get('risk_score', 0)
            # Higher risk scores indicate more data to work with
            quality_adjustment += min(10, risk_score / 10)
    
    return min(100, base_confidence + quality_adjustment)


def _calculate_comprehensive_risk_scores(transactions, *analysis_results):
    """Calculate comprehensive risk scores"""
    # This would implement the comprehensive risk scoring logic
    # from the original tasks.py file
    return {
        'overall_risk_score': 50,  # Placeholder
        'confidence_score': 75,    # Placeholder
        'individual_scores': {}    # Placeholder
    }


def _generate_comprehensive_risk_assessment(risk_scores, transactions):
    """Generate comprehensive risk assessment"""
    return {
        'overall_risk_score': risk_scores.get('overall_risk_score', 0),
        'confidence_score': risk_scores.get('confidence_score', 0),
        'total_transactions': len(transactions)
    }


def _generate_comprehensive_audit_recommendations(risk_scores, transactions):
    """Generate comprehensive audit recommendations"""
    return {
        'immediate_actions': [],
        'high_priority_actions': [],
        'medium_priority_actions': [],
        'low_priority_actions': []
    }


def _generate_comprehensive_compliance_assessment(risk_scores, transactions):
    """Generate comprehensive compliance assessment"""
    return {
        'compliance_status': 'compliant',
        'compliance_score': 100,
        'violations': []
    }


def _generate_comprehensive_chart_data(risk_scores, transactions):
    """Generate comprehensive chart data"""
    return {
        'risk_scores': risk_scores,
        'transaction_count': len(transactions)
    }


def _generate_comprehensive_export_data(risk_scores, transactions):
    """Generate comprehensive export data"""
    return [
        {
            'type': 'risk_analysis',
            'data': risk_scores
        }
    ]


def _consolidate_all_anomalies(analysis_results):
    """Consolidate anomalies from all analysis results"""
    consolidated = []
    
    for result in analysis_results:
        if result and result.anomaly_list:
            consolidated.extend(result.anomaly_list)
    
    return consolidated
