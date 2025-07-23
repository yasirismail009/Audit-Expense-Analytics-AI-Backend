#!/usr/bin/env python
import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import AnalyticsProcessingResult, DuplicateAnalysisResult, AnalyticsResult

print("=== ANALYTICS DATA CHECK ===")
print()

print("1. AnalyticsProcessingResult (NEW MODEL):")
print(f"   Count: {AnalyticsProcessingResult.objects.count()}")
for result in AnalyticsProcessingResult.objects.all():
    print(f"   - ID: {result.id}")
    print(f"     Type: {result.analytics_type}")
    print(f"     Status: {result.processing_status}")
    print(f"     Transactions: {result.total_transactions}")
    print(f"     Amount: {result.total_amount}")
    print(f"     Chart Data Keys: {list(result.chart_data.keys()) if result.chart_data else 'None'}")
    print(f"     Trial Balance Keys: {list(result.trial_balance_data.keys()) if result.trial_balance_data else 'None'}")
    print()

print("2. DuplicateAnalysisResult (OLD MODEL):")
print(f"   Count: {DuplicateAnalysisResult.objects.count()}")
for result in DuplicateAnalysisResult.objects.all():
    print(f"   - ID: {result.id}")
    print(f"     Type: {result.analysis_type}")
    print(f"     Status: {result.status}")
    print(f"     Duplicates: {len(result.duplicate_list)}")
    print()

print("3. AnalyticsResult (OLD MODEL):")
print(f"   Count: {AnalyticsResult.objects.count()}")
for result in AnalyticsResult.objects.all():
    print(f"   - ID: {result.id}")
    print(f"     Type: {result.analysis_type}")
    print(f"     Status: {result.status}")
    print()

print("=== SUMMARY ===")
print(f"AnalyticsProcessingResult: {AnalyticsProcessingResult.objects.count()} records")
print(f"DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()} records")
print(f"AnalyticsResult: {AnalyticsResult.objects.count()} records") 