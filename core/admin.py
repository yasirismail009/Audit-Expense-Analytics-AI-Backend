from django.contrib import admin
from .models import (
    SAPGLPosting, DataFile, AnalysisSession, TransactionAnalysis, 
    SystemMetrics, GLAccount, FileProcessingJob, MLModelTraining
)

@admin.register(GLAccount)
class GLAccountAdmin(admin.ModelAdmin):
    list_display = ['account_id', 'account_name', 'account_type', 'account_category', 'is_active']
    list_filter = ['account_type', 'account_category', 'is_active']
    search_fields = ['account_id', 'account_name']
    ordering = ['account_id']

@admin.register(SAPGLPosting)
class SAPGLPostingAdmin(admin.ModelAdmin):
    list_display = ['document_number', 'posting_date', 'gl_account', 'amount_local_currency', 'user_name', 'fiscal_year']
    list_filter = ['posting_date', 'transaction_type', 'fiscal_year', 'document_type']
    search_fields = ['document_number', 'gl_account', 'user_name']
    date_hierarchy = 'posting_date'
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(DataFile)
class DataFileAdmin(admin.ModelAdmin):
    list_display = ['file_name', 'client_name', 'fiscal_year', 'status', 'uploaded_at']
    list_filter = ['status', 'fiscal_year', 'uploaded_at']
    search_fields = ['file_name', 'client_name', 'engagement_id']
    readonly_fields = ['id', 'uploaded_at', 'processed_at']

@admin.register(AnalysisSession)
class AnalysisSessionAdmin(admin.ModelAdmin):
    list_display = ['session_name', 'status', 'total_transactions', 'flagged_transactions', 'created_at']
    list_filter = ['status', 'created_at']
    search_fields = ['session_name', 'description']
    readonly_fields = ['id', 'created_at', 'started_at', 'completed_at']

@admin.register(TransactionAnalysis)
class TransactionAnalysisAdmin(admin.ModelAdmin):
    list_display = ['transaction', 'risk_score', 'risk_level', 'created_at']
    list_filter = ['risk_level', 'created_at']
    search_fields = ['transaction__document_number']
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(SystemMetrics)
class SystemMetricsAdmin(admin.ModelAdmin):
    list_display = ['metric_date', 'total_transactions', 'total_amount', 'active_users']
    list_filter = ['metric_date']
    date_hierarchy = 'metric_date'
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(FileProcessingJob)
class FileProcessingJobAdmin(admin.ModelAdmin):
    list_display = ['data_file', 'status', 'run_anomalies', 'created_at']
    list_filter = ['status', 'run_anomalies', 'created_at']
    search_fields = ['data_file__file_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']

@admin.register(MLModelTraining)
class MLModelTrainingAdmin(admin.ModelAdmin):
    list_display = ['session_name', 'model_type', 'status', 'training_data_size', 'created_at']
    list_filter = ['model_type', 'status', 'created_at']
    search_fields = ['session_name', 'description']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']
