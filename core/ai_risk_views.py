"""
AI Risk Recommendation Views
API endpoints for AI-powered risk assessment and recommendations
"""

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
import logging

from .models import (
    AIRiskAssessment, RiskPattern, AnomalyCluster, AIRiskRecommendation,
    RiskTrend, ModelPerformance, DataFile
)
from .serializers import (
    AIRiskAssessmentSerializer, RiskPatternSerializer, AnomalyClusterSerializer,
    AIRiskRecommendationSerializer, RiskTrendSerializer, ModelPerformanceSerializer
)

logger = logging.getLogger(__name__)


class AIRiskAssessmentViewSet(viewsets.ModelViewSet):
    """
    ViewSet for AI Risk Assessment results
    Provides CRUD operations for AI risk assessments
    """
    queryset = AIRiskAssessment.objects.all()
    serializer_class = AIRiskAssessmentSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file if provided"""
        queryset = AIRiskAssessment.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        return queryset.order_by('-analysis_date')
    
    @action(detail=True, methods=['get'])
    def recommendations(self, request, pk=None):
        """Get AI recommendations for a specific assessment"""
        assessment = self.get_object()
        recommendations = assessment.ai_recommendations.get('immediate_actions', [])
        return Response({
            'assessment_id': str(assessment.id),
            'recommendations': recommendations,
            'investigation_priorities': assessment.investigation_priorities,
            'audit_procedures': assessment.audit_procedures,
            'risk_mitigation': assessment.risk_mitigation
        })
    
    @action(detail=True, methods=['get'])
    def risk_summary(self, request, pk=None):
        """Get risk summary for a specific assessment"""
        assessment = self.get_object()
        return Response(assessment.get_risk_summary())


class RiskPatternViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Risk Pattern results
    Provides CRUD operations for risk patterns
    """
    queryset = RiskPattern.objects.all()
    serializer_class = RiskPatternSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file and pattern type if provided"""
        queryset = RiskPattern.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        pattern_type = self.request.query_params.get('pattern_type', None)
        
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        if pattern_type:
            queryset = queryset.filter(pattern_type=pattern_type)
        
        return queryset.order_by('-pattern_risk_score')


class AnomalyClusterViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Anomaly Cluster results
    Provides CRUD operations for anomaly clusters
    """
    queryset = AnomalyCluster.objects.all()
    serializer_class = AnomalyClusterSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file and cluster type if provided"""
        queryset = AnomalyCluster.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        cluster_type = self.request.query_params.get('cluster_type', None)
        
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        if cluster_type:
            queryset = queryset.filter(cluster_type=cluster_type)
        
        return queryset.order_by('-cluster_risk_score')


class AIRiskRecommendationViewSet(viewsets.ModelViewSet):
    """
    ViewSet for AI Risk Recommendations
    Provides CRUD operations for AI-generated recommendations
    """
    queryset = AIRiskRecommendation.objects.all()
    serializer_class = AIRiskRecommendationSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file, category, and priority if provided"""
        queryset = AIRiskRecommendation.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        category = self.request.query_params.get('category', None)
        priority = self.request.query_params.get('priority', None)
        
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        if category:
            queryset = queryset.filter(category=category)
        if priority:
            queryset = queryset.filter(priority=priority)
        
        return queryset.order_by('-priority', '-urgency', '-created_at')
    
    @action(detail=True, methods=['patch'])
    def update_status(self, request, pk=None):
        """Update recommendation status"""
        recommendation = self.get_object()
        new_status = request.data.get('status')
        assigned_to = request.data.get('assigned_to')
        due_date = request.data.get('due_date')
        
        if new_status:
            recommendation.status = new_status
        if assigned_to:
            recommendation.assigned_to = assigned_to
        if due_date:
            recommendation.due_date = due_date
        
        recommendation.save()
        return Response(self.get_serializer(recommendation).data)


class RiskTrendViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Risk Trend results
    Provides CRUD operations for risk trends
    """
    queryset = RiskTrend.objects.all()
    serializer_class = RiskTrendSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file and trend type if provided"""
        queryset = RiskTrend.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        trend_type = self.request.query_params.get('trend_type', None)
        
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        if trend_type:
            queryset = queryset.filter(trend_type=trend_type)
        
        return queryset.order_by('-end_date', '-trend_significance')


class ModelPerformanceViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Model Performance results
    Provides CRUD operations for ML model performance tracking
    """
    queryset = ModelPerformance.objects.all()
    serializer_class = ModelPerformanceSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter by data file and model type if provided"""
        queryset = ModelPerformance.objects.all()
        data_file_id = self.request.query_params.get('data_file_id', None)
        model_type = self.request.query_params.get('model_type', None)
        
        if data_file_id:
            queryset = queryset.filter(data_file_id=data_file_id)
        if model_type:
            queryset = queryset.filter(model_type=model_type)
        
        return queryset.order_by('-training_timestamp', '-accuracy')


class AIRiskRecommendationAPIView(APIView):
    """
    API View for generating AI risk recommendations
    """
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        """Generate AI risk recommendations for a data file"""
        data_file_id = request.data.get('data_file_id')
        
        if not data_file_id:
            return Response(
                {'error': 'data_file_id is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            data_file = DataFile.objects.get(id=data_file_id)
            
            # Import AI risk recommendation engine
            from .ai_risk_recommendations import AIRiskRecommendationAPI
            
            # Generate recommendations
            ai_results = AIRiskRecommendationAPI.generate_risk_recommendations(str(data_file_id))
            
            if 'error' in ai_results:
                return Response(
                    {'error': ai_results['error']}, 
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            return Response({
                'message': 'AI risk recommendations generated successfully',
                'data_file_id': data_file_id,
                'results': ai_results
            })
            
        except DataFile.DoesNotExist:
            return Response(
                {'error': 'Data file not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error generating AI risk recommendations: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
