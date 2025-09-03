"""
Core URLs - No authentication required
All endpoints in this module are accessible without authentication.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create router and register upload and processing functionality
router = DefaultRouter()
router.register(r'data-files', views.DataFileViewSet, basename='data-file')
router.register(r'processing-jobs', views.FileProcessingJobViewSet, basename='processing-job')
router.register(r'ml-model-training', views.MLModelTrainingViewSet, basename='ml-model-training')
router.register(r'celery-debug', views.CeleryDebugViewSet, basename='celery-debug')

urlpatterns = [
    # Include router URLs (upload and processing functionality)
    path('', include(router.urls)),
    
    # Upload and processing endpoints
    path('targeted-anomaly-upload/', views.TargetedAnomalyUploadView.as_view(), name='targeted-anomaly-upload'),
    # File listing endpoint
    path('files-listing/', views.FileListingAPIView.as_view(), name='files-listing'),
    # File analysis statistics endpoint
    path('file-analysis-statistics/<uuid:file_id>/', views.FileAnalysisStatisticsView.as_view(), name='file-analysis-statistics'),
    # SAPGLPosting listing endpoint
    path('sapgl-postings/<uuid:file_id>/', views.SAPGLPostingListView.as_view(), name='sapgl-postings-list'),
    # Filter dropdown data endpointktL
    path('filter-dropdowns/<uuid:file_id>/', views.FilterDropdownDataView.as_view(), name='filter-dropdowns'),
    # Duplicate analysis endpoint
    path('duplicate-analysis/<uuid:file_id>/', views.DuplicateAnalysisView.as_view(), name='duplicate-analysis'),
    # Duplicate entries listing endpoint
    path('duplicate-list/<uuid:file_id>/', views.DuplicateListView.as_view(), name='duplicate-list'),

    # User analysis endpoint
    path('user-analysis/<uuid:file_id>/', views.UserAnalysisView.as_view(), name='user-analysis'),
    # User entries listing endpoint
    path('user-list/<uuid:file_id>/', views.UserListView.as_view(), name='user-list'),
    # Unusual days analysis endpoint
    path('unusual-days-analysis/<uuid:file_id>/', views.UnusualDaysAnalysisView.as_view(), name='unusual-days-analysis'),
    # Unusual days entries listing endpoint
    path('unusual-days-list/<uuid:file_id>/', views.UnusualDaysListView.as_view(), name='unusual-days-list'),
    # Closing entries analysis endpoint
    path('closing-entries-analysis/<uuid:file_id>/', views.ClosingEntriesAnalysisView.as_view(), name='closing-entries-analysis'),
    # Closing entries listing endpoint
    path('closing-entries-list/<uuid:file_id>/', views.ClosingEntriesListView.as_view(), name='closing-entries-list'),
    # Backdated analysis endpoint
    path('backdated-analysis/<uuid:file_id>/', views.BackdatedAnalysisView.as_view(), name='backdated-analysis'),
    # Backdated entries listing endpoint
    path('backdated-entries-list/<uuid:file_id>/', views.BackdatedEntriesListView.as_view(), name='backdated-entries-list'),
    # Holiday analysis endpoint
    path('holiday-analysis/<uuid:file_id>/', views.HolidayAnalysisView.as_view(), name='holiday-analysis'),
    # Holiday entries listing endpoint
    path('holiday-entries-list/<uuid:file_id>/', views.HolidayListView.as_view(), name='holiday-entries-list'),
    # Manual entry analysis endpoint (Management Override Risk)
    path('manual-entry-analysis/<uuid:file_id>/', views.ManualEntryAnalysisView.as_view(), name='manual-entry-analysis'),
    # File GL accounts endpoint
    path('file-gl-accounts/<uuid:file_id>/', views.FileGLAccountsView.as_view(), name='file-gl-accounts'),
    # Excel export endpoint
    path('excel-export/<uuid:file_id>/', views.ExcelExportView.as_view(), name='excel-export'),
    # Analysis export endpoint
    path('analysis-export/<uuid:file_id>/<str:analysis_type>/', views.AnalysisExportView.as_view(), name='analysis-export'),
    
    # AI Risk Recommendation URLs
    path('ai-risk/', include('core.ai_risk_urls')),
] 