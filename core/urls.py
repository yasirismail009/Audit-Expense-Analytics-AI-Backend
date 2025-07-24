from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create router and register viewsets
router = DefaultRouter()
router.register(r'sap-gl-postings', views.SAPGLPostingViewSet, basename='sap-gl-posting')
router.register(r'data-files', views.DataFileViewSet, basename='data-file')
router.register(r'analysis-sessions', views.AnalysisSessionViewSet, basename='analysis-session')
router.register(r'transaction-analysis', views.TransactionAnalysisViewSet, basename='transaction-analysis')
router.register(r'gl-accounts', views.GLAccountViewSet, basename='gl-account')
router.register(r'processing-jobs', views.FileProcessingJobViewSet, basename='processing-job')
router.register(r'ml-model-training', views.MLModelTrainingViewSet, basename='ml-model-training')
router.register(r'celery-debug', views.CeleryDebugViewSet, basename='celery-debug')

urlpatterns = [
    # Include router URLs
    path('', include(router.urls)),
    
    # Custom endpoints
    path('targeted-anomaly-upload/', views.TargetedAnomalyUploadView.as_view(), name='targeted-anomaly-upload'),
    path('processing-jobs/<uuid:pk>/status/', views.FileProcessingJobViewSet.as_view({'get': 'status'}), name='processing-job-status'),
    path('all-files/', views.AllFilesListView.as_view(), name='all-files'),
    path('files/', views.FileListView.as_view(), name='files'),
    path('analysis/', views.AnalysisAPIView.as_view(), name='analysis'),
    path('analysis/file/<str:file_id>/', views.FileAnalysisView.as_view(), name='file-analysis'),
    path('comprehensive-analytics/file/<str:file_id>/', views.ComprehensiveFileAnalyticsView.as_view(), name='comprehensive-file-analytics'),
    path('comprehensive-duplicate-analysis/file/<str:file_id>/', views.ComprehensiveDuplicateAnalysisView.as_view(), name='comprehensive-duplicate-analysis'),
  
    # path('analytics/file/<str:file_id>/', views.FileAnalyticsView.as_view(), name='file-analytics'),

    # ML Model Training specific endpoints
    path('ml-model-training/train/', views.MLModelTrainingViewSet.as_view({'post': 'train_models'}), name='ml-train-models'),
    path('ml-model-training/retrain/', views.MLModelTrainingViewSet.as_view({'post': 'retrain_models'}), name='ml-retrain-models'),
    path('ml-model-training/model-info/', views.MLModelTrainingViewSet.as_view({'get': 'model_info'}), name='ml-model-info'),
    path('ml-model-training/predict/', views.MLModelTrainingViewSet.as_view({'post': 'predict_anomalies'}), name='ml-predict-anomalies'),
    path('ml-model-training/<uuid:pk>/status/', views.MLModelTrainingViewSet.as_view({'get': 'status'}), name='ml-training-status'),
    
    # Database-stored analytics and ML processing results
    path('processing-results/', views.ProcessingResultsAPIView.as_view(), name='processing-results'),
    path('analytics-results/', views.AnalyticsResultsAPIView.as_view(), name='analytics-results'),
    path('ml-processing-results/', views.MLProcessingResultsAPIView.as_view(), name='ml-processing-results'),
    path('processing-progress/', views.ProcessingProgressAPIView.as_view(), name='processing-progress'),
    
    # Database-stored comprehensive analytics (same pattern as existing endpoints)
    path('db-comprehensive-analytics/file/<str:file_id>/', views.DatabaseStoredComprehensiveAnalyticsView.as_view(), name='db-comprehensive-analytics'),
    path('db-comprehensive-backdated-analysis/file/<str:file_id>/', views.DatabaseStoredBackdatedAnalysisView.as_view(), name='db-comprehensive-backdated-analysis'),
    path('db-comprehensive-duplicate-analysis/file/<str:file_id>/', views.DatabaseStoredDuplicateAnalysisView.as_view(), name='db-comprehensive-duplicate-analysis'),
    path('analytics-db-check/file/<str:file_id>/', views.AnalyticsDatabaseCheckView.as_view(), name='analytics-db-check'),
    
    # Backdated analysis is now included in main processing flow
    # Use DatabaseStoredComprehensiveAnalyticsView to get backdated analysis results
] 