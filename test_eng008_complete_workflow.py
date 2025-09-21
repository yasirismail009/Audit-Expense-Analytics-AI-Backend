#!/usr/bin/env python
"""
Test script for complete ENG-008 workflow: Completeness → AI Prediction → ML Training
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.tasks import run_eng008_complete_workflow

def test_complete_workflow():
    """Test the complete ENG-008 workflow"""
    print("🎯 Testing Complete ENG-008 Workflow")
    print("=" * 60)
    print("Workflow: Completeness Test → AI Prediction → ML Training")
    print("=" * 60)
    
    # Trigger the complete workflow
    result = run_eng008_complete_workflow.delay()
    
    print(f"✅ Complete workflow triggered!")
    print(f"📋 Task ID: {result.id}")
    print(f"🔗 Task Status: {result.status}")
    
    # Wait a bit and check result
    import time
    print("\n⏳ Waiting for workflow to complete...")
    time.sleep(5)
    
    try:
        task_result = result.get(timeout=10)
        print(f"\n📊 Workflow Result:")
        print(f"   Status: {task_result.get('status', 'unknown')}")
        print(f"   Message: {task_result.get('message', 'No message')}")
        
        if 'completeness_result' in task_result:
            comp_result = task_result['completeness_result']
            print(f"   ✅ Completeness: {comp_result.get('success', False)}")
        
        if 'ai_prediction_result' in task_result:
            ai_result = task_result['ai_prediction_result']
            print(f"   🤖 AI Prediction: {ai_result.get('status', 'unknown')}")
        
        if 'ml_training_result' in task_result:
            ml_result = task_result['ml_training_result']
            print(f"   🧠 ML Training: {ml_result.get('status', 'unknown')}")
            
    except Exception as e:
        print(f"⏳ Workflow still running or error: {e}")

if __name__ == '__main__':
    test_complete_workflow()
