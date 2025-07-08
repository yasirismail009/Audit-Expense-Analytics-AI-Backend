from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    path('', views.home, name='home'),
    path('test-db/', views.test_db_connection, name='test_db_connection'),
    path('expenses/upload/', views.ExpenseUploadView.as_view(), name='expense_upload'),
    path('expenses/', views.ExpenseListView.as_view(), name='expense_list'),  # Now returns sheets
    path('expenses/<int:expense_id>/analysis/', views.ExpenseAnalysisView.as_view(), name='expense_analysis'),
    path('expenses/<int:expense_id>/debug/', views.DebugExpenseView.as_view(), name='expense_debug'),
    path('sheets/<int:sheet_id>/', views.ExpenseSheetView.as_view(), name='expense_sheet_detail'),
    path('sheets/<int:sheet_id>/analyze/', views.SheetAnalysisView.as_view(), name='sheet_analysis'),
    path('analysis/train/', views.ModelTrainingView.as_view(), name='model_training'),
    path('analysis/bulk/', views.BulkAnalysisView.as_view(), name='bulk_analysis'),
    path('analysis/session/<str:session_id>/', views.AnalysisSessionView.as_view(), name='analysis_session'),
] 