#!/usr/bin/env python
"""
Minimal test to check AI model imports
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

try:
    # Try importing the base class first
    from core.models import BaseAnalysisResult
    print("✅ BaseAnalysisResult imported successfully")
    
    # Try importing each AI model individually
    try:
        from core.models import AIRiskAssessment
        print("✅ AIRiskAssessment imported successfully")
    except ImportError as e:
        print(f"❌ AIRiskAssessment import failed: {e}")
    
    try:
        from core.models import RiskPattern
        print("✅ RiskPattern imported successfully")
    except ImportError as e:
        print(f"❌ RiskPattern import failed: {e}")
    
    try:
        from core.models import AnomalyCluster
        print("✅ AnomalyCluster imported successfully")
    except ImportError as e:
        print(f"❌ AnomalyCluster import failed: {e}")
    
    try:
        from core.models import AIRiskRecommendation
        print("✅ AIRiskRecommendation imported successfully")
    except ImportError as e:
        print(f"❌ AIRiskRecommendation import failed: {e}")
    
    try:
        from core.models import RiskTrend
        print("✅ RiskTrend imported successfully")
    except ImportError as e:
        print(f"❌ RiskTrend import failed: {e}")
    
    try:
        from core.models import ModelPerformance
        print("✅ ModelPerformance imported successfully")
    except ImportError as e:
        print(f"❌ ModelPerformance import failed: {e}")
        
except Exception as e:
    print(f"❌ General error: {e}")
