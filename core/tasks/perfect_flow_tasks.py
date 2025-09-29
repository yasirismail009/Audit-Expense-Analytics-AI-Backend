#!/usr/bin/env python
"""
Perfect Flow Tasks Module
Handles the sequential execution of the perfect ML flow
"""

import logging
from celery import shared_task
from django.utils import timezone

from .completeness_tasks import run_gl_completeness_analysis
from .ml_training_tasks import train_comprehensive_ai_models
from .ml_completeness_tasks import generate_ml_recommendations

logger = logging.getLogger(__name__)




@shared_task(bind=True, max_retries=1, default_retry_delay=30, time_limit=1800, soft_time_limit=1500)
def trigger_perfect_ml_flow(self, data_file_id, engagement_id, client_name):
    """
    Execute the perfect ML flow directly without calling other Celery tasks
    """
    try:
        print("🎯 TRIGGERING PERFECT ML FLOW")
        print("=" * 40)
        print(f"📊 Engagement ID: {engagement_id}")
        print(f"📁 Data File ID: {data_file_id}")
        print(f"👤 Client: {client_name}")
        logger.info(f"🎯 Triggering Perfect ML Flow for engagement: {engagement_id}")
        
        # Execute the perfect flow directly (not as a separate task)
        print("🚀 Executing Perfect ML Flow directly...")
        
        # Task functions are already imported at the top of the file
        
        # Step 1: Run Completeness Test
        print("🚀 STEP 1: Running Completeness Test...")
        print("📊 Completeness test starting...")
        logger.info("1️⃣ Running Completeness Test...")
        # Call the Celery task function's run method directly
        completeness_result = run_gl_completeness_analysis.run(data_file_id)
        
        if not completeness_result.get('success', False):
            print(f"❌ Completeness test failed: {completeness_result.get('error', 'Unknown error')}")
            logger.error(f"❌ Completeness test failed: {completeness_result.get('error', 'Unknown error')}")
            return {
                'success': False,
                'error': 'Completeness test failed',
                'completeness_result': completeness_result
            }
        
        print(f"✅ Completeness test completed: {completeness_result.get('completeness_score', 'N/A')}%")
        logger.info(f"✅ Completeness test completed: {completeness_result.get('completeness_score', 'N/A')}%")
        
        # Step 2: Run Model Training
        print("🧠 STEP 2: Running Model Training...")
        print("📈 Training Trend Analysis Model (scikit-learn Isolation Forest)")
        print("🔍 Training Unusual Transaction Detection (scikit-learn Random Forest)")
        print("🎯 Training Completeness Prediction Model")
        logger.info("2️⃣ Running Model Training...")
        training_result = train_comprehensive_ai_models.run(engagement_id=engagement_id, client_name=client_name)
        
        if not training_result.get('status') == 'success':
            print(f"❌ Model training failed: {training_result.get('message', 'Unknown error')}")
            logger.error(f"❌ Model training failed: {training_result.get('message', 'Unknown error')}")
            return {
                'success': False,
                'error': 'Model training failed',
                'completeness_result': completeness_result,
                'training_result': training_result
            }
        
        print(f"✅ Model training completed: {training_result.get('models_trained', 0)} models trained")
        logger.info(f"✅ Model training completed: {training_result.get('models_trained', 0)} models trained")
        
        # Step 3: Generate ML Recommendations
        print("🎯 STEP 3: Generating ML Recommendations...")
        print("🔧 Analyzing data patterns and generating fixes")
        print("📊 Creating actionable recommendations")
        logger.info("3️⃣ Generating ML Recommendations...")
        recommendations_result = generate_ml_recommendations.run(engagement_id=engagement_id)
        
        if not recommendations_result.get('success', False):
            print(f"❌ ML recommendations failed: {recommendations_result.get('error', 'Unknown error')}")
            logger.error(f"❌ ML recommendations failed: {recommendations_result.get('error', 'Unknown error')}")
            return {
                'success': False,
                'error': 'ML recommendations failed',
                'completeness_result': completeness_result,
                'training_result': training_result,
                'recommendations_result': recommendations_result
            }
        
        print(f"✅ ML recommendations completed: {recommendations_result.get('fixes_count', 0)} fixes generated")
        logger.info(f"✅ ML recommendations completed: {recommendations_result.get('fixes_count', 0)} fixes generated")
        
        # Perfect flow completed successfully
        print("🎉 PERFECT ML FLOW COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ All steps completed:")
        print("   1️⃣ Completeness Test - PASSED")
        print("   2️⃣ Model Training - COMPLETED")
        print("   3️⃣ ML Recommendations - GENERATED")
        print("=" * 60)
        logger.info("🎉 Perfect ML Flow completed successfully!")
        
        return {
            'success': True,
            'message': 'Perfect ML flow completed successfully',
            'data_file_id': data_file_id,
            'engagement_id': engagement_id,
            'client_name': client_name,
            'completeness_result': completeness_result,
            'training_result': training_result,
            'recommendations_result': recommendations_result,
            'flow_completed_at': timezone.now()
        }
        
    except Exception as e:
        print(f"❌ Perfect ML flow failed: {e}")
        logger.error(f"❌ Perfect ML flow failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'success': False,
            'error': str(e),
            'data_file_id': data_file_id,
            'engagement_id': engagement_id,
            'client_name': client_name
        }
