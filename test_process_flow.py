#!/usr/bin/env python
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, CompletenessTestResult
from core.tasks import run_gl_completeness_analysis, train_comprehensive_ai_models
import logging

logger = logging.getLogger(__name__)

print("=== PROCESS FLOW ANALYSIS ===")

# 1. Check GL files
print("\n1. GL Files Status:")
gl_files = DataFile.objects.filter(file_type='GL').order_by('-uploaded_at')
for f in gl_files:
    print(f"  - {f.file_name}: {f.status} (Processed: {f.processed_at})")

# 2. Check Completeness Test Results
print("\n2. Completeness Test Results:")
completeness_results = CompletenessTestResult.objects.all().order_by('-test_timestamp')
print(f"  Total results: {completeness_results.count()}")
for r in completeness_results[:3]:
    print(f"  - Engagement: {r.engagement.engagement_id}, Status: {r.overall_status}, Score: {r.completeness_score}")

# 3. Test Task Import
print("\n3. Task Import Test:")
try:
    from core.tasks import run_gl_completeness_analysis, train_comprehensive_ai_models
    print("  ✅ Tasks imported successfully")
    print(f"  - run_gl_completeness_analysis: {run_gl_completeness_analysis}")
    print(f"  - train_comprehensive_ai_models: {train_comprehensive_ai_models}")
except Exception as e:
    print(f"  ❌ Task import failed: {e}")

# 4. Test AI_TRAINING_AVAILABLE
print("\n4. AI Training Availability:")
try:
    from core.tasks import AI_TRAINING_AVAILABLE
    print(f"  AI_TRAINING_AVAILABLE: {AI_TRAINING_AVAILABLE}")
except Exception as e:
    print(f"  ❌ Could not check AI_TRAINING_AVAILABLE: {e}")

# 5. Test Task Execution (if GL file exists)
print("\n5. Task Execution Test:")
if gl_files.exists():
    gl_file = gl_files.first()
    print(f"  Testing with GL file: {gl_file.file_name}")
    try:
        # Test completeness task
        task = run_gl_completeness_analysis.delay(str(gl_file.id))
        print(f"  ✅ Completeness task queued: {task.id}")
        print(f"  Task state: {task.state}")
    except Exception as e:
        print(f"  ❌ Completeness task failed: {e}")
else:
    print("  No GL files found for testing")

print("\n=== ANALYSIS COMPLETE ===")
