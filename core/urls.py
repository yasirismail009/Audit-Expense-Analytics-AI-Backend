from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create router and register viewsets
router = DefaultRouter()
router.register(r'postings', views.SAPGLPostingViewSet, basename='posting')
router.register(r'files', views.DataFileViewSet, basename='file')
router.register(r'sessions', views.AnalysisSessionViewSet, basename='session')
router.register(r'analyses', views.TransactionAnalysisViewSet, basename='analysis')
router.register(r'gl-accounts', views.GLAccountViewSet, basename='gl-account')
router.register(r'duplicate-anomalies', views.DuplicateAnomalyViewSet, basename='duplicate-anomaly')

urlpatterns = [
    # API endpoints
    path('', include(router.urls)),
    
    # Dashboard
    path('dashboard/', views.DashboardView.as_view(), name='dashboard'),
    
    # File upload endpoint
    path('files/upload/', views.DataFileViewSet.as_view({'post': 'upload'}), name='file-upload'),
    # path('files/<uuid:pk>/analysis/', views.FileAnalysisView.as_view(), name='file-analysis'),
    
    # Analysis endpoints
    path('sessions/<uuid:pk>/run/', views.AnalysisSessionViewSet.as_view({'post': 'run_analysis'}), name='run-analysis'),
    path('sessions/<uuid:pk>/summary/', views.AnalysisSessionViewSet.as_view({'get': 'summary'}), name='analysis-summary'),
    
    # Statistics endpoints
    path('postings/statistics/', views.SAPGLPostingViewSet.as_view({'get': 'statistics'}), name='posting-statistics'),
    path('postings/top-users/', views.SAPGLPostingViewSet.as_view({'get': 'top_users'}), name='top-users'),
    path('postings/top-accounts/', views.SAPGLPostingViewSet.as_view({'get': 'top_accounts'}), name='top-accounts'),
    
    # GL Account endpoints
    path('gl-accounts/analysis/', views.GLAccountViewSet.as_view({'get': 'analysis'}), name='gl-account-analysis'),
    path('gl-accounts/trial-balance/', views.GLAccountViewSet.as_view({'get': 'trial_balance'}), name='trial-balance'),
    path('gl-accounts/charts/', views.GLAccountViewSet.as_view({'get': 'charts'}), name='gl-account-charts'),
    path('gl-accounts/upload-master-data/', views.GLAccountViewSet.as_view({'post': 'upload_master_data'}), name='gl-account-upload'),
    
    # Duplicate Anomaly endpoints (consolidated)
    path('duplicate-anomalies/analyze/', views.DuplicateAnomalyViewSet.as_view({'post': 'analyze'}), name='duplicate-analyze'),
    
    # New file management endpoints
    path('file-list/', views.FileListView.as_view(), name='file-list'),
    path('file-upload-analysis/', views.FileUploadAnalysisView.as_view(), name='file-upload-analysis'),
    path('file-summary/<uuid:id>/', views.FileSummaryView.as_view(), name='file-summary'),
] 