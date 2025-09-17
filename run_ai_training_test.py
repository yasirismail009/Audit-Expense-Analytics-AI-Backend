#!/usr/bin/env python
"""
Quick AI training test script for ENG-008
"""
import os
import sys
import django

# Add the project directory to Python path
sys.path.insert(0, '/app')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.gl_prediction_tasks import train_gl_anomaly_detection_model, train_gl_volume_prediction_model
from core.models import Engagement

def main():
    print('🚀 Running AI training directly on ENG-008...')
    
    # Find ENG-008
    eng008 = Engagement.objects.filter(engagement_id__icontains='ENG-008').first()
    if not eng008:
        print('❌ ENG-008 not found!')
        return
    
    print(f'📊 Training on: {eng008.engagement_name}')
    print(f'🎯 Client: {eng008.client.client_name}')
    print(f'🆔 ID: {eng008.id}')
    
    results = {}
    
    # 1. Train Anomaly Detection
    print('\n🔍 1. Training Anomaly Detection Model...')
    try:
        anomaly_result = train_gl_anomaly_detection_model(str(eng008.id), eng008.client.client_name)
        results['anomaly'] = anomaly_result.get('success', False)
        if results['anomaly']:
            print('✅ Anomaly Detection: SUCCESS')
            print(f"   Features: {anomaly_result.get('feature_count', 'N/A')}")
            print(f"   Training samples: {anomaly_result.get('training_samples', 'N/A')}")
        else:
            print('❌ Anomaly Detection: FAILED')
            print(f"   Error: {anomaly_result.get('error', 'Unknown')}")
    except Exception as e:
        print(f'❌ Anomaly Detection: FAILED - {e}')
        results['anomaly'] = False
    
    # 2. Train Volume Prediction
    print('\n📈 2. Training Volume Prediction Model...')
    try:
        volume_result = train_gl_volume_prediction_model(str(eng008.id), eng008.client.client_name)
        results['volume'] = volume_result.get('success', False)
        if results['volume']:
            print('✅ Volume Prediction: SUCCESS')
            print(f"   Features: {volume_result.get('feature_count', 'N/A')}")
            print(f"   Training samples: {volume_result.get('training_samples', 'N/A')}")
        else:
            print('❌ Volume Prediction: FAILED')
            print(f"   Error: {volume_result.get('error', 'Unknown')}")
    except Exception as e:
        print(f'❌ Volume Prediction: FAILED - {e}')
        results['volume'] = False
    
    # Summary
    models_trained = sum(results.values())
    print(f'\n🎉 Training Complete!')
    print(f'✅ Models Trained: {models_trained}/2')
    print(f'🔍 Anomaly Detection: {"✅" if results.get("anomaly") else "❌"}')
    print(f'📈 Volume Prediction: {"✅" if results.get("volume") else "❌"}')
    
    if models_trained > 0:
        print('\n🚀 Your AI models are now ready for predictions!')
    else:
        print('\n⚠️ No models were trained successfully. Check logs for details.')

if __name__ == '__main__':
    main()
