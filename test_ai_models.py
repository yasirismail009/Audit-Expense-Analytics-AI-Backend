#!/usr/bin/env python
"""
Test script to check AI model imports
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

try:
    from core.models import AIRiskAssessment, RiskPattern, AnomalyCluster, AIRiskRecommendation, RiskTrend, ModelPerformance
    print("✅ All AI models imported successfully!")
    print(f"AIRiskAssessment: {AIRiskAssessment}")
    print(f"RiskPattern: {RiskPattern}")
    print(f"AnomalyCluster: {AnomalyCluster}")
    print(f"AIRiskRecommendation: {AIRiskRecommendation}")
    print(f"RiskTrend: {RiskTrend}")
    print(f"ModelPerformance: {ModelPerformance}")
except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Other error: {e}")
