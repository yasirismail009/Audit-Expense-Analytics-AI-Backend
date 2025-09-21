#!/usr/bin/env python
"""
Comprehensive data cleaning script for ENG-008
Cleans completeness results, AI predictions, and other related data
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

def clean_eng008_data():
    """Clean all ENG-008 related data"""
    try:
        print("🧹 Cleaning ENG-008 data from database...")
        
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
        
        # Get data files for this engagement
        data_files = DataFile.objects.filter(engagement=engagement)
        print(f"📁 Found {data_files.count()} data files for ENG-008")
        
        # Clean completeness test results
        completeness_results = CompletenessTestResult.objects.filter(engagement=engagement)
        comp_count = completeness_results.count()
        if comp_count > 0:
            completeness_results.delete()
            print(f"🗑️  Deleted {comp_count} completeness test results")
        else:
            print("ℹ️  No completeness test results found")
        
        # Clean AI predictions
        ai_predictions = CompletenessAIPrediction.objects.filter(data_file__engagement=engagement)
        ai_count = ai_predictions.count()
        if ai_count > 0:
            ai_predictions.delete()
            print(f"🗑️  Deleted {ai_count} AI predictions")
        else:
            print("ℹ️  No AI predictions found")
        
        # Clean AI models (optional - be careful with this)
        # Note: AICompletenessModel doesn't have engagement field, so we'll skip this
        ai_models = AICompletenessModel.objects.all()
        model_count = ai_models.count()
        if model_count > 0:
            print(f"ℹ️  Found {model_count} AI models (keeping all - no engagement filter)")
        else:
            print("ℹ️  No AI models found")
        
        # Show data file info
        for data_file in data_files:
            gl_count = SAPGLPosting.objects.filter(data_file=data_file).count()
            tb_count = TrialBalance.objects.filter(data_file=data_file).count()
            print(f"   📄 {data_file.file_name}: {gl_count} GL records, {tb_count} TB records")
        
        return True
        
    except Engagement.DoesNotExist:
        print("❌ ENG-008 engagement not found")
        return False
    except Exception as e:
        print(f"❌ Error cleaning ENG-008 data: {e}")
        return False

def main():
    """Main function to clean ENG-008 data"""
    print("🎯 Clean ENG-008 Data")
    print("=" * 50)
    
    if clean_eng008_data():
        print("\n" + "=" * 50)
        print("✅ ENG-008 data cleaned successfully!")
        print("🚀 You can now run a fresh completeness test")
    else:
        print("\n" + "=" * 50)
        print("❌ Failed to clean ENG-008 data")

if __name__ == '__main__':
    main()
