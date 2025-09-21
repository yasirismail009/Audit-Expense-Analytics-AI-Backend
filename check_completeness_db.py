#!/usr/bin/env python
"""
Check completeness test data in database for ENG-008
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    CompletenessTestResult, 
    CompletenessAIPrediction,
    AICompletenessModel,
    DataFile, 
    Engagement,
    SAPGLPosting,
    TrialBalance
)

def check_completeness_db():
    """Check what completeness data exists in the database"""
    try:
        print("🔍 Checking Completeness Test Data in Database")
        print("=" * 60)
        
        # Get ENG-008 engagement
        try:
            engagement = Engagement.objects.get(engagement_id='ENG-008')
            print(f"✅ Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
            print(f"   Client: {engagement.client.client_name}")
            print(f"   Fiscal Year: {engagement.fiscal_year}")
        except Engagement.DoesNotExist:
            print("❌ ENG-008 engagement not found")
            return
        
        # Check data files
        data_files = DataFile.objects.filter(engagement=engagement)
        print(f"\n📁 Data Files ({data_files.count()}):")
        for df in data_files:
            gl_count = SAPGLPosting.objects.filter(data_file=df).count()
            tb_count = TrialBalance.objects.filter(data_file=df).count()
            print(f"   📄 {df.file_name}")
            print(f"      Type: {df.file_type}")
            print(f"      Status: {df.status}")
            print(f"      Records: {df.total_records}")
            print(f"      GL Postings: {gl_count}")
            print(f"      TB Records: {tb_count}")
            print(f"      Created: {df.created_at}")
            print()
        
        # Check completeness test results
        completeness_results = CompletenessTestResult.objects.filter(engagement=engagement)
        print(f"🧪 Completeness Test Results ({completeness_results.count()}):")
        
        if completeness_results.exists():
            for i, result in enumerate(completeness_results.order_by('-test_timestamp'), 1):
                print(f"   Test #{i}:")
                print(f"      ID: {result.id}")
                print(f"      Timestamp: {result.test_timestamp}")
                print(f"      Status: {result.overall_status}")
                print(f"      Score: {result.completeness_score}%")
                print(f"      Tests Passed: {result.tests_passed}/{result.total_tests}")
                print(f"      Critical Issues: {result.critical_issues_count}")
                print(f"      GL Records: {result.total_gl_records}")
                print(f"      TB Records: {result.total_tb_records}")
                print(f"      Duration: {result.processing_duration}s")
                print(f"      GL File: {result.gl_file.file_name if result.gl_file else 'None'}")
                print(f"      TB File: {result.tb_file.file_name if result.tb_file else 'None'}")
                print()
        else:
            print("   ℹ️  No completeness test results found")
        
        # Check AI predictions
        ai_predictions = CompletenessAIPrediction.objects.filter(data_file__engagement=engagement)
        print(f"🤖 AI Predictions ({ai_predictions.count()}):")
        
        if ai_predictions.exists():
            for i, prediction in enumerate(ai_predictions.order_by('-prediction_timestamp'), 1):
                print(f"   Prediction #{i}:")
                print(f"      ID: {prediction.id}")
                print(f"      Timestamp: {prediction.prediction_timestamp}")
                print(f"      Confidence: {prediction.prediction_confidence}%")
                print(f"      Predicted Score: {prediction.predicted_completeness_score}%")
                print(f"      Predicted Status: {prediction.predicted_status}")
                print(f"      Data File: {prediction.data_file.file_name}")
                print()
        else:
            print("   ℹ️  No AI predictions found")
        
        # Check AI models
        ai_models = AICompletenessModel.objects.all()
        print(f"🧠 AI Models ({ai_models.count()}):")
        
        if ai_models.exists():
            for i, model in enumerate(ai_models.order_by('-created_at'), 1):
                print(f"   Model #{i}:")
                print(f"      Name: {model.model_name}")
                print(f"      Type: {model.model_type}")
                print(f"      Version: {model.model_version}")
                print(f"      Status: {model.status}")
                print(f"      Client: {model.client_name}")
                print(f"      Accuracy: {model.prediction_accuracy}%")
                print(f"      Created: {model.created_at}")
                print()
        else:
            print("   ℹ️  No AI models found")
        
        # Summary
        print("📊 Summary:")
        print(f"   Data Files: {data_files.count()}")
        print(f"   Completeness Tests: {completeness_results.count()}")
        print(f"   AI Predictions: {ai_predictions.count()}")
        print(f"   AI Models: {ai_models.count()}")
        
        return {
            'engagement': engagement,
            'data_files': data_files,
            'completeness_results': completeness_results,
            'ai_predictions': ai_predictions,
            'ai_models': ai_models
        }
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == '__main__':
    check_completeness_db()
