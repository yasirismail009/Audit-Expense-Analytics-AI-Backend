"""
Core URLs - No authentication required
All endpoints in this module are accessible without authentication.
"""
from django.urls import path
from . import views
from . import listing_views

urlpatterns = [
    # File upload
    path('upload/', views.FileUploadView.as_view(), name='file-upload'),
    
    # Listing APIs
    path('gl-postings/', listing_views.SAPGLPostingListView.as_view(), name='gl-postings-list'),
    path('gl-postings/export/', listing_views.GLPostingExportView.as_view(), name='gl-postings-export'),
    path('trial-balance/', listing_views.TrialBalanceListView.as_view(), name='trial-balance-list'),
    path('chart-of-accounts/', listing_views.ChartOfAccountListView.as_view(), name='chart-of-accounts-list'),
    path('data-files/', listing_views.DataFileListView.as_view(), name='data-files-list'),
    
    # Completeness Test APIs
    path('completeness-test/', views.list_completeness_tests, name='completeness-test-list'),
    path('completeness-test/engagement/<str:engagement_id>/', views.get_completeness_test_by_engagement, name='completeness-test-by-engagement'),
    path('completeness-test/<uuid:test_id>/', views.get_completeness_test_by_id, name='completeness-test-by-id'),
] 