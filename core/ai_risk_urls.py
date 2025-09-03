"""
AI Risk Recommendation URL Configuration
URL patterns for AI-powered risk assessment and recommendations
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .ai_risk_views import (
    AIRiskAssessmentViewSet, RiskPatternViewSet, AnomalyClusterViewSet,
    AIRiskRecommendationViewSet, RiskTrendViewSet, ModelPerformanceViewSet,
    AIRiskRecommendationAPIView
)

# Create router for ViewSets
router = DefaultRouter()
router.register(r'ai-risk-assessments', AIRiskAssessmentViewSet, basename='ai-risk-assessment')
router.register(r'risk-patterns', RiskPatternViewSet, basename='risk-pattern')
router.register(r'anomaly-clusters', AnomalyClusterViewSet, basename='anomaly-cluster')
router.register(r'ai-risk-recommendations', AIRiskRecommendationViewSet, basename='ai-risk-recommendation')
router.register(r'risk-trends', RiskTrendViewSet, basename='risk-trend')
router.register(r'model-performances', ModelPerformanceViewSet, basename='model-performance')

# URL patterns
urlpatterns = [
    # Include router URLs
    path('', include(router.urls)),
    
    # API endpoints for generating recommendations
    path('generate-recommendations/', AIRiskRecommendationAPIView.as_view(), name='generate-ai-recommendations'),
]
