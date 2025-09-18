#!/usr/bin/env python3
"""
Test script for running anomaly detection training on ENG-008
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import Engagement
from core.gl_prediction_tasks import train_gl_anomaly_detection_model
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_anomaly_detection_eng008():
    """
    Test anomaly detection training for ENG-008
    """
    try:
        logger.info("🔍 Starting anomaly detection test for ENG-008")
        
        # Find ENG-008 engagement
        engagement = Engagement.objects.filter(engagement_id='ENG-008').first()
        
        if not engagement:
            logger.error("❌ ENG-008 engagement not found")
            return False
        
        logger.info(f"✅ Found engagement: {engagement.engagement_id}")
        logger.info(f"📊 Engagement ID: {engagement.id}")
        logger.info(f"👤 Client: {engagement.client.client_name if engagement.client else 'Unknown'}")
        
        # Check if engagement has GL data
        from core.models import SAPGLPosting
        gl_count = SAPGLPosting.objects.filter(data_file__engagement=engagement).count()
        logger.info(f"📈 GL transactions available: {gl_count:,}")
        
        if gl_count < 10:
            logger.warning(f"⚠️ Insufficient GL data: {gl_count} transactions (minimum 10 required)")
            return False
        
        # Run anomaly detection training
        logger.info("🚀 Starting anomaly detection training...")
        result = train_gl_anomaly_detection_model(
            engagement_id=str(engagement.id),
            client_name=engagement.client.client_name if engagement.client else 'Unknown'
        )
        
        if result.get('success'):
            logger.info("✅ Anomaly detection training completed successfully!")
            logger.info(f"📊 Training Results:")
            logger.info(f"  - Model ID: {result.get('model_id')}")
            logger.info(f"  - Training Duration: {result.get('training_duration', 0):.2f} seconds")
            logger.info(f"  - Training Data Size: {result.get('training_data_size', 0):,} transactions")
            logger.info(f"  - Normal Transactions: {result.get('normal_count', 0):,}")
            logger.info(f"  - Anomalous Transactions: {result.get('anomaly_count', 0):,}")
            logger.info(f"  - Feature Count: {result.get('feature_count', 0)}")
            logger.info(f"  - Engagement: {result.get('engagement_name', 'Unknown')}")
            return True
        else:
            logger.error(f"❌ Anomaly detection training failed: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return False

def main():
    """
    Main function to run the test
    """
    logger.info("=" * 60)
    logger.info("🧪 ANOMALY DETECTION TEST FOR ENG-008")
    logger.info("=" * 60)
    
    success = test_anomaly_detection_eng008()
    
    logger.info("=" * 60)
    if success:
        logger.info("🎉 TEST COMPLETED SUCCESSFULLY!")
    else:
        logger.info("💥 TEST FAILED!")
    logger.info("=" * 60)
    
    return success

if __name__ == "__main__":
    main()
