from django.contrib import admin
from .models import (
    Client, Engagement, DataFile, GLAccount, SAPGLPosting, TrialBalance, ChartOfAccount,
    FileProcessingJob, CompletenessTestResult, MLModelTraining, ProfitCenter, ProfitCenterAccount
)

@admin.register(SAPGLPosting)
class SAPGLPostingAdmin(admin.ModelAdmin):
    list_display = ['document_number', 'posting_date', 'gl_account', 'amount_local_currency', 'user_name', 'fiscal_year']
    list_filter = ['posting_date', 'fiscal_year', 'document_type', 'local_currency']
    search_fields = ['document_number', 'gl_account', 'user_name']
    date_hierarchy = 'posting_date'
    readonly_fields = ['id', 'created_at', 'updated_at']

# Core audit structure admin
@admin.register(Client)
class ClientAdmin(admin.ModelAdmin):
    list_display = ['client_code', 'client_name', 'company_name', 'industry', 'is_active', 'created_at']
    list_filter = ['is_active', 'industry', 'country', 'created_at']
    search_fields = ['client_code', 'client_name', 'company_name']
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(Engagement)
class EngagementAdmin(admin.ModelAdmin):
    list_display = ['engagement_id', 'engagement_name', 'client', 'fiscal_year', 'status', 'created_at']
    list_filter = ['status', 'fiscal_year', 'engagement_type', 'created_at']
    search_fields = ['engagement_id', 'engagement_name', 'client__client_name']
    readonly_fields = ['id', 'created_at', 'updated_at']

@admin.register(DataFile)
class DataFileAdmin(admin.ModelAdmin):
    list_display = ['file_name', 'get_client_name', 'get_fiscal_year', 'file_type', 'status', 'is_validated', 'uploaded_at']
    list_filter = ['status', 'file_type', 'is_validated', 'uploaded_at']
    search_fields = ['file_name', 'engagement__client__client_name', 'engagement__engagement_id']
    readonly_fields = ['id', 'uploaded_at', 'processed_at', 'legacy_engagement_id', 'legacy_client_name', 'legacy_company_name', 'legacy_fiscal_year']
    
    def get_client_name(self, obj):
        return obj.engagement.client.client_name if obj.engagement else obj.legacy_client_name
    get_client_name.short_description = 'Client Name'
    
    def get_fiscal_year(self, obj):
        return obj.engagement.fiscal_year if obj.engagement else obj.legacy_fiscal_year
    get_fiscal_year.short_description = 'Fiscal Year'

class ProfitCenterAccountInline(admin.TabularInline):
    """Inline admin for profit center-account relationships"""
    model = ProfitCenterAccount
    extra = 1
    fields = ['gl_account', 'is_primary', 'allocation_percentage', 'company_code', 'business_area']


@admin.register(ProfitCenter)
class ProfitCenterAdmin(admin.ModelAdmin):
    list_display = ['profit_center_code', 'profit_center_name', 'profit_center_type', 'cost_center', 'revenue_center', 'profit_center_status', 'get_account_count', 'created_at']
    list_filter = ['profit_center_status', 'profit_center_type', 'cost_center', 'revenue_center', 'company_code', 'created_at']
    search_fields = ['profit_center_code', 'profit_center_name', 'responsible_person', 'department']
    readonly_fields = ['id', 'created_at', 'updated_at', 'cost_center_code', 'cost_center_name']
    inlines = [ProfitCenterAccountInline]
    fieldsets = (
        ('Basic Information (Profit Center)', {
            'fields': ('profit_center_code', 'profit_center_name', 'profit_center_short_text', 'profit_center_description')
        }),
        ('Cost Center Aliases (Same Data)', {
            'fields': ('cost_center_code', 'cost_center_name'),
            'description': 'Cost Center and Profit Center are the same data with different names'
        }),
        ('Classification', {
            'fields': ('profit_center_type', 'profit_center_group', 'cost_center', 'revenue_center')
        }),
        ('Properties', {
            'fields': ('profit_center_currency', 'profit_center_status', 'company_code', 'business_area', 'segment')
        }),
        ('Hierarchy', {
            'fields': ('parent_profit_center', 'profit_center_level')
        }),
        ('Management', {
            'fields': ('responsible_person', 'department', 'notes')
        }),
        ('Audit Trail', {
            'fields': ('created_from_file', 'last_updated_from_file', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_account_count(self, obj):
        """Get the number of accounts associated with this profit center"""
        return obj.get_account_count()
    get_account_count.short_description = 'Account Count'


@admin.register(ProfitCenterAccount)
class ProfitCenterAccountAdmin(admin.ModelAdmin):
    list_display = ['profit_center', 'gl_account', 'is_primary', 'allocation_percentage', 'company_code', 'created_at']
    list_filter = ['is_primary', 'company_code', 'business_area', 'created_at']
    search_fields = ['profit_center__profit_center_code', 'profit_center__profit_center_name', 'gl_account__account_code', 'gl_account__account_name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    fieldsets = (
        ('Relationship', {
            'fields': ('profit_center', 'gl_account', 'is_primary', 'allocation_percentage')
        }),
        ('Context', {
            'fields': ('company_code', 'business_area', 'notes')
        }),
        ('Audit Trail', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(GLAccount)
class GLAccountAdmin(admin.ModelAdmin):
    list_display = ['account_code', 'account_name', 'get_engagement', 'cost_code', 'get_profit_center', 'account_type', 'is_active', 'created_at']
    list_filter = ['is_active', 'account_type', 'sub_type', 'sub_sub_type', 'created_at']
    search_fields = ['account_code', 'account_name', 'cost_code', 'engagement__engagement_id', 'engagement__client__client_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'legacy_engagement_id', 'legacy_client_name']
    fieldsets = (
        ('Basic Information', {
            'fields': ('engagement', 'account_code', 'account_name', 'account_description')
        }),
        ('Account Classification', {
            'fields': ('account_type', 'sub_type', 'sub_sub_type', 'financial_statement_category', 'balance_sheet_category', 'income_statement_category')
        }),
        ('Cost Center & Profit Center', {
            'fields': ('cost_code', 'profit_center_ref'),
            'description': 'Cost code maps to Profit Center from GL listing'
        }),
        ('Trial Balance Data', {
            'fields': ('opening_balance', 'closing_balance', 'tb_debit', 'tb_credit')
        }),
        ('Properties', {
            'fields': ('is_active', 'currency', 'company_code')
        }),
        ('Legacy Fields', {
            'fields': ('legacy_engagement_id', 'legacy_client_name'),
            'classes': ('collapse',)
        }),
    )
    
    def get_engagement(self, obj):
        return obj.engagement.engagement_id if obj.engagement else obj.legacy_engagement_id
    get_engagement.short_description = 'Engagement'
    
    def get_profit_center(self, obj):
        profit_center = obj.get_profit_center()
        return profit_center.profit_center_code if profit_center else 'Not linked'
    get_profit_center.short_description = 'Profit Center'

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

@admin.register(FileProcessingJob)
class FileProcessingJobAdmin(admin.ModelAdmin):
    list_display = ['data_file', 'job_type', 'status', 'created_at']
    list_filter = ['status', 'job_type', 'run_anomalies', 'created_at']
    search_fields = ['data_file__file_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']

@admin.register(CompletenessTestResult)
class CompletenessTestResultAdmin(admin.ModelAdmin):
    list_display = ['get_engagement', 'overall_status', 'completeness_score', 'tests_passed', 'total_tests', 'test_timestamp']
    list_filter = ['overall_status', 'test_timestamp']
    search_fields = ['engagement__engagement_id', 'engagement__client__client_name']
    readonly_fields = ['id', 'test_timestamp', 'processing_duration', 'legacy_engagement_id', 'created_at', 'updated_at']
    
    def get_engagement(self, obj):
        return obj.engagement.engagement_id if obj.engagement else obj.legacy_engagement_id
    get_engagement.short_description = 'Engagement'

@admin.register(MLModelTraining)
class MLModelTrainingAdmin(admin.ModelAdmin):
    list_display = ['model_name', 'model_type', 'model_version', 'training_data_size', 'created_at']
    list_filter = ['model_type', 'model_version', 'created_at']
    search_fields = ['model_name', 'model_type']
    readonly_fields = ['id', 'created_at', 'updated_at']
