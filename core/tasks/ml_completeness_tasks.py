#!/usr/bin/env python
"""
ML-based Completeness Test Tasks
Integrates scikit-learn recommendations into the completeness workflow
"""

import logging
from celery import shared_task
from django.utils import timezone
from django.db import transaction

from core.models import (
    DataFile, Engagement, CompletenessTestResult, 
    CompletenessAIPrediction, AICompletenessModel
)
from core.ml_completeness_recommendations import analyze_completeness_with_ml

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_ml_completeness_analysis(self, data_file_id):
    """
    Run ML-based completeness analysis with scikit-learn recommendations
    """
    try:
        logger.info(f"🤖 Starting ML completeness analysis for data file: {data_file_id}")
        
        # Get data file
        try:
            data_file = DataFile.objects.get(id=data_file_id)
        except DataFile.DoesNotExist:
            logger.error(f"Data file {data_file_id} not found")
            return {'success': False, 'error': 'Data file not found'}
        
        # Get engagement
        engagement = data_file.engagement
        if not engagement:
            logger.error(f"No engagement found for data file {data_file_id}")
            return {'success': False, 'error': 'No engagement found'}
        
        # Get GL postings
        from core.models import SAPGLPosting
        gl_postings = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not gl_postings.exists():
            logger.warning(f"No GL postings found for data file {data_file_id}")
            return {'success': False, 'error': 'No GL postings found'}
        
        # Get Trial Balance records
        from core.models import TrialBalance
        trial_balance_records = TrialBalance.objects.filter(engagement=engagement)
        
        # Get Chart of Accounts records
        from core.models import GLAccount
        coa_records = GLAccount.objects.filter(engagement=engagement)
        
        # Get historical data for ML analysis
        historical_data = get_historical_completeness_data(engagement)
        
        logger.info(f"📊 Data loaded: {gl_postings.count()} GL, {trial_balance_records.count()} TB, {coa_records.count()} COA")
        
        # Run ML analysis
        ml_results = analyze_completeness_with_ml(
            gl_postings, 
            trial_balance_records, 
            coa_records, 
            historical_data
        )
        
        # Save ML analysis results
        with transaction.atomic():
            # Create or update AI model
            ai_model, created = AICompletenessModel.objects.get_or_create(
                model_name='completeness_ml_analyzer',
                model_version='1.0.0',
                client_name=engagement.engagement_name,
                defaults={
                    'model_type': 'COMPLETENESS_ANALYZER',
                    'status': 'DEPLOYED',
                    'validation_accuracy': ml_results['prediction']['confidence'],
                    'training_completed_at': timezone.now(),
                    'model_file_path': 'ml_completeness_recommendations.py',
                    'feature_importance': ml_results['features'],
                    'model_parameters': {
                        'algorithm': 'scikit-learn',
                        'features_count': len(ml_results['features']),
                        'analysis_type': 'completeness_recommendations'
                    }
                }
            )
            
            # Create AI prediction record
            ai_prediction = CompletenessAIPrediction.objects.create(
                data_file=data_file,
                ai_model=ai_model,
                prediction_confidence=ml_results['prediction']['confidence'],
                predicted_completeness_score=ml_results['prediction']['predicted_score'],
                predicted_status=ml_results['analysis']['overall_status'],
                predicted_step_results=ml_results['analysis'],
                predicted_processing_time=0,  # Not applicable for ML analysis
                input_features=ml_results['features']
            )
            
            # Update or create CompletenessTestResult with ML insights
            test_result, created = CompletenessTestResult.objects.get_or_create(
                engagement=engagement,
                data_version=getattr(data_file, 'version', '1.0'),
                defaults={
                    'overall_status': ml_results['analysis']['overall_status'],
                    'completeness_score': ml_results['prediction']['predicted_score'],
                    'test_timestamp': timezone.now(),
                    'legacy_engagement_id': engagement.engagement_id,
                    'ml_analysis_results': ml_results,
                    'ai_prediction_id': ai_prediction.id
                }
            )
            
            if not created:
                # Update existing result with ML insights
                test_result.ml_analysis_results = ml_results
                test_result.ai_prediction_id = ai_prediction.id
                test_result.save()
        
        logger.info(f"✅ ML completeness analysis completed successfully")
        logger.info(f"📊 Predicted Score: {ml_results['prediction']['predicted_score']:.2f}%")
        logger.info(f"🎯 Overall Status: {ml_results['analysis']['overall_status']}")
        logger.info(f"🔧 Issues Found: {ml_results['analysis']['total_issues']}")
        logger.info(f"📋 Fixes Generated: {len(ml_results['fixes'])}")
        
        return {
            'success': True,
            'ml_results': ml_results,
            'ai_prediction_id': ai_prediction.id,
            'test_result_id': test_result.id,
            'predicted_score': ml_results['prediction']['predicted_score'],
            'overall_status': ml_results['analysis']['overall_status'],
            'issues_count': ml_results['analysis']['total_issues'],
            'fixes_count': len(ml_results['fixes'])
        }
        
    except Exception as e:
        logger.error(f"❌ ML completeness analysis failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'success': False,
            'error': str(e),
            'analysis_duration': (timezone.now() - timezone.now()).total_seconds()
        }


@shared_task(bind=True, max_retries=1, default_retry_delay=30, time_limit=120, soft_time_limit=90)
def generate_ml_recommendations(self, engagement_id):
    """
    Generate ML-based recommendations for completeness improvements
    """
    try:
        print("🎯 GENERATING ML RECOMMENDATIONS")
        print("=" * 40)
        print(f"📊 Engagement ID: {engagement_id}")
        logger.info(f"🎯 Generating ML recommendations for engagement: {engagement_id}")
        
        # Get engagement
        try:
            engagement = Engagement.objects.get(id=engagement_id)
            print(f"📊 Engagement: {engagement.engagement_name}")
        except Engagement.DoesNotExist:
            print(f"❌ Engagement {engagement_id} not found")
            logger.error(f"Engagement {engagement_id} not found")
            return {'success': False, 'error': 'Engagement not found'}
        
        # Get latest completeness test result
        latest_result = CompletenessTestResult.objects.filter(
            engagement=engagement
        ).order_by('-test_timestamp').first()
        
        if not latest_result:
            logger.warning(f"No completeness test results found for engagement {engagement_id}")
            return {'success': False, 'error': 'No completeness test results found'}
        
        # Generate ML recommendations based on completeness test results
        print("🔍 Analyzing completeness test results...")
        
        # Extract key metrics from completeness test
        completeness_score = latest_result.completeness_score
        overall_status = latest_result.overall_status
        step1_completeness = latest_result.step1_file_completeness
        step2_reconciliation = latest_result.step2_gl_tb_reconciliation
        
        # Generate recommendations based on completeness results
        recommendations = []
        fixes_count = 0
        
        # Recommendation 1: Score-based improvements
        if completeness_score < 95:
            recommendations.append({
                'type': 'score_improvement',
                'priority': 'HIGH',
                'title': 'Improve Completeness Score',
                'description': f'Current score is {completeness_score:.1f}%. Target: 95%+',
                'action': 'Review and fix data quality issues',
                'impact': 'High - Will improve overall data reliability'
            })
            fixes_count += 1
        
        # Recommendation 2: Step 1 improvements
        if step1_completeness and step1_completeness.get('status') != 'PASS':
            recommendations.append({
                'type': 'file_completeness',
                'priority': 'HIGH',
                'title': 'Fix File Completeness Issues',
                'description': 'GL file completeness validation failed',
                'action': 'Verify GL file data integrity and completeness',
                'impact': 'Critical - Affects data accuracy'
            })
            fixes_count += 1
        
        # Recommendation 3: Step 2 improvements
        if step2_reconciliation and step2_reconciliation.get('status') != 'PASS':
            recommendations.append({
                'type': 'reconciliation',
                'priority': 'HIGH',
                'title': 'Fix GL-TB Reconciliation',
                'description': 'GL and Trial Balance reconciliation failed',
                'action': 'Review and correct GL-TB balance discrepancies',
                'impact': 'Critical - Affects financial accuracy'
            })
            fixes_count += 1
        
        # Recommendation 4: General improvements
        if completeness_score >= 95:
            recommendations.append({
                'type': 'maintenance',
                'priority': 'MEDIUM',
                'title': 'Maintain High Quality',
                'description': f'Excellent score of {completeness_score:.1f}%',
                'action': 'Continue current data quality practices',
                'impact': 'Positive - Maintains high standards'
            })
            fixes_count += 1
        
        # Create comprehensive recommendations
        ml_recommendations = {
            'engagement_id': engagement_id,
            'engagement_name': engagement.engagement_name,
            'test_timestamp': latest_result.test_timestamp,
            'completeness_score': completeness_score,
            'overall_status': overall_status,
            'recommendations': recommendations,
            'fixes_count': fixes_count,
            'analysis_summary': {
                'total_issues': len([r for r in recommendations if r['priority'] == 'HIGH']),
                'high_priority_fixes': len([r for r in recommendations if r['priority'] == 'HIGH']),
                'medium_priority_fixes': len([r for r in recommendations if r['priority'] == 'MEDIUM']),
                'overall_health': 'GOOD' if completeness_score >= 95 else 'NEEDS_IMPROVEMENT'
            }
        }
        
        print(f"✅ ML recommendations generated for engagement: {engagement.engagement_name}")
        print(f"📊 Score: {completeness_score}%")
        print(f"🔧 Fixes: {fixes_count}")
        print(f"⚠️ High Priority Issues: {len([r for r in recommendations if r['priority'] == 'HIGH'])}")
        
        logger.info(f"✅ ML recommendations generated for engagement: {engagement.engagement_name}")
        logger.info(f"📊 Score: {completeness_score}%")
        logger.info(f"🔧 Fixes: {fixes_count}")
        logger.info(f"⚠️ High Priority Issues: {len([r for r in recommendations if r['priority'] == 'HIGH'])}")
        
        return {
            'success': True,
            'recommendations': ml_recommendations,
            'engagement_name': engagement.engagement_name,
            'completeness_score': latest_result.completeness_score,
            'fixes_count': fixes_count,
            'issues_count': len([r for r in recommendations if r['priority'] == 'HIGH'])
        }
        
    except Exception as e:
        logger.error(f"❌ ML recommendations generation failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def get_historical_completeness_data(engagement):
    """
    Get historical completeness data for ML analysis
    """
    try:
        # Get historical test results for this engagement
        historical_results = CompletenessTestResult.objects.filter(
            engagement=engagement
        ).order_by('-test_timestamp')[:10]  # Last 10 results
        
        historical_data = []
        for result in historical_results:
            if hasattr(result, 'ml_analysis_results') and result.ml_analysis_results:
                features = result.ml_analysis_results.get('features', {})
                if features:
                    historical_data.append(features)
        
        logger.info(f"📚 Retrieved {len(historical_data)} historical data points for ML analysis")
        return historical_data
        
    except Exception as e:
        logger.warning(f"Could not retrieve historical data: {e}")
        return []


@shared_task(bind=True, max_retries=1, default_retry_delay=30, time_limit=60, soft_time_limit=45)
def validate_ml_recommendations(self, engagement_id):
    """
    Validate ML recommendations against actual completeness results
    """
    try:
        logger.info(f"🔍 Validating ML recommendations for engagement: {engagement_id}")
        
        # Get engagement
        engagement = Engagement.objects.get(id=engagement_id)
        
        # Get latest AI predictions
        latest_predictions = CompletenessAIPrediction.objects.filter(
            data_file__engagement=engagement
        ).order_by('-prediction_timestamp')[:5]
        
        validation_results = []
        
        for prediction in latest_predictions:
            # Get actual completeness result
            actual_result = CompletenessTestResult.objects.filter(
                engagement=engagement,
                test_timestamp__gte=prediction.prediction_timestamp
            ).order_by('test_timestamp').first()
            
            if actual_result:
                # Calculate accuracy
                score_accuracy = 100 - abs(
                    prediction.predicted_completeness_score - actual_result.completeness_score
                )
                status_correct = prediction.predicted_status == actual_result.overall_status
                
                # Update prediction with actual results
                prediction.actual_completeness_score = actual_result.completeness_score
                prediction.actual_status = actual_result.overall_status
                prediction.score_accuracy = score_accuracy
                prediction.status_correct = status_correct
                prediction.save()
                
                validation_results.append({
                    'prediction_id': prediction.id,
                    'predicted_score': prediction.predicted_completeness_score,
                    'actual_score': actual_result.completeness_score,
                    'score_accuracy': score_accuracy,
                    'status_correct': status_correct,
                    'overall_accurate': status_correct and score_accuracy >= 80
                })
        
        # Calculate overall validation metrics
        if validation_results:
            avg_score_accuracy = sum(r['score_accuracy'] for r in validation_results) / len(validation_results)
            status_accuracy = sum(r['status_correct'] for r in validation_results) / len(validation_results)
            overall_accuracy = sum(r['overall_accurate'] for r in validation_results) / len(validation_results)
            
            logger.info(f"📊 ML Validation Results:")
            logger.info(f"   Score Accuracy: {avg_score_accuracy:.2f}%")
            logger.info(f"   Status Accuracy: {status_accuracy:.2f}%")
            logger.info(f"   Overall Accuracy: {overall_accuracy:.2f}%")
            
            return {
                'success': True,
                'validation_results': validation_results,
                'avg_score_accuracy': avg_score_accuracy,
                'status_accuracy': status_accuracy,
                'overall_accuracy': overall_accuracy,
                'total_predictions': len(validation_results)
            }
        else:
            logger.warning("No validation results found")
            return {
                'success': False,
                'error': 'No validation results found'
            }
            
    except Exception as e:
        logger.error(f"❌ ML validation failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }
