from django.contrib import admin
from .models import (
    SAPGLPosting, DataFile, FileProcessingJob, MLModelTraining, FileProcessingTask, CompletenessJob,
    TrialBalance, ChartOfAccount
)

@admin.register(SAPGLPosting)
class SAPGLPostingAdmin(admin.ModelAdmin):
    list_display = ['document_number', 'posting_date', 'gl_account', 'amount_local_currency', 'user_name', 'fiscal_year']
    list_filter = ['posting_date', 'fiscal_year', 'document_type', 'local_currency']
    search_fields = ['document_number', 'gl_account', 'user_name']
    date_hierarchy = 'posting_date'
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(DataFile)
class DataFileAdmin(admin.ModelAdmin):
    list_display = ['file_name', 'client_name', 'fiscal_year', 'status', 'uploaded_at']
    list_filter = ['status', 'fiscal_year', 'uploaded_at']
    search_fields = ['file_name', 'client_name', 'engagement_id']
    readonly_fields = ['id', 'uploaded_at', 'processed_at']

@admin.register(FileProcessingJob)
class FileProcessingJobAdmin(admin.ModelAdmin):
    list_display = ['data_file', 'status', 'run_anomalies', 'created_at']
    list_filter = ['status', 'run_anomalies', 'created_at']
    search_fields = ['data_file__file_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']

@admin.register(MLModelTraining)
class MLModelTrainingAdmin(admin.ModelAdmin):
    list_display = ['model_name', 'model_type', 'model_version', 'training_data_size', 'created_at']
    list_filter = ['model_type', 'model_version', 'created_at']
    search_fields = ['model_name', 'model_type']
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(FileProcessingTask)
class FileProcessingTaskAdmin(admin.ModelAdmin):
    list_display = ['task_type', 'data_file', 'status', 'created_at']
    list_filter = ['task_type', 'status', 'created_at']
    search_fields = ['data_file__file_name', 'task_type']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']

@admin.register(CompletenessJob)
class CompletenessJobAdmin(admin.ModelAdmin):
    list_display = ['data_file', 'status', 'created_at']
    list_filter = ['status', 'created_at']
    search_fields = ['data_file__file_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']

@admin.register(TrialBalance)
class TrialBalanceAdmin(admin.ModelAdmin):
    list_display = ['gl_account', 'short_text', 'company_code', 'opening_balance', 'closing_balance', 'currency']
    list_filter = ['company_code', 'currency', 'data_file']
    search_fields = ['gl_account', 'short_text', 'company_code']
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(ChartOfAccount)
class ChartOfAccountAdmin(admin.ModelAdmin):
    list_display = ['account', 'type', 'sub_type', 'sub_sub_type', 'company', 'fiscal_year']
    list_filter = ['type', 'sub_type', 'sub_sub_type', 'company', 'fiscal_year', 'data_file']
    search_fields = ['account', 'type', 'sub_type', 'sub_sub_type', 'gl_account_long_text']
    readonly_fields = ['id', 'created_at', 'updated_at']
