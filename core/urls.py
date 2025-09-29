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
    
    # Core Entity APIs
    path('clients/', listing_views.ClientListView.as_view(), name='clients-list'),
    path('engagements/', listing_views.EngagementListView.as_view(), name='engagements-list'),
    
    # Data File APIs
    path('data-files/', listing_views.DataFileListView.as_view(), name='data-files-list'),
    
    # Listing APIs
    path('gl-postings/', listing_views.SAPGLPostingListView.as_view(), name='gl-postings-list'),
    path('gl-postings/export/', listing_views.GLPostingExportView.as_view(), name='gl-postings-export'),
    path('trial-balance/', listing_views.TrialBalanceListView.as_view(), name='trial-balance-list'),
    path('chart-of-accounts/', listing_views.ChartOfAccountListView.as_view(), name='chart-of-accounts-list'),
    
    # Completeness Test APIs
    path('completeness-test/', views.list_completeness_tests, name='completeness-test-list'),
    path('completeness-test/engagement/<str:engagement_id>/', views.get_completeness_test_by_engagement, name='completeness-test-by-engagement'),
    path('completeness-test/<uuid:test_id>/', views.get_completeness_test_by_id, name='completeness-test-by-id'),
    path('completeness-test/trigger/<str:gl_file_id>/', views.trigger_completeness_analysis, name='trigger-completeness-analysis'),
    
    # Account Verifications APIs
    path('account-verifications/engagement/<str:engagement_id>/', views.get_account_verifications_by_engagement, name='account-verifications-by-engagement'),
    
    # Document Verifications APIs
    path('document-verifications/engagement/<str:engagement_id>/', views.get_document_verifications_by_engagement, name='document-verifications-by-engagement'),
    
    # Profit Center Data APIs
    path('profit-center-data/account/<str:account_id>/', views.get_profit_center_data_by_account, name='profit-center-data-by-account'),
    
    # Version Management APIs
    path('engagement/<str:engagement_id>/versions/', views.get_engagement_versions, name='engagement-versions'),
    path('engagement/<str:engagement_id>/version/<str:version>/files/', views.get_files_by_version, name='files-by-version'),
] 