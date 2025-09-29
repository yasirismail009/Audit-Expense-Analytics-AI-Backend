from rest_framework import serializers
from .models import (
    # Core audit structure models
    Client, Engagement, DataFile, GLAccount,
    # Transaction models
    SAPGLPosting, TrialBalance, ChartOfAccount,
    # Processing and analysis models
    FileProcessingJob, CompletenessTestResult,
    # Analysis result models  
    OverallAnalysisResult, RiskScoringDocument, 
    DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult, 
    ClosingEntriesAnalysisResult, UnusualDaysAnalysisResult, HolidayAnalysisResult, 
    GeneralAnalysisResult, MLModelTraining,
    # Utility classes
    AmountParser
)
from decimal import Decimal
import uuid

# ============================================================================
# CORE AUDIT STRUCTURE SERIALIZERS
# ============================================================================

class ClientSerializer(serializers.ModelSerializer):
    """Serializer for Client model"""
    
    # Computed fields
    active_engagements_count = serializers.SerializerMethodField()
    total_engagements_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Client
        fields = [
            'id', 'client_code', 'client_name', 'company_name', 'industry', 
            'country', 'currency', 'primary_contact_name', 'primary_contact_email', 
            'primary_contact_phone', 'tax_id', 'registration_number', 
            'fiscal_year_end', 'audit_firm', 'is_active', 'notes',
            'active_engagements_count', 'total_engagements_count',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_active_engagements_count(self, obj):
        """Get count of active engagements"""
        return obj.get_active_engagements().count()
    
    def get_total_engagements_count(self, obj):
        """Get total count of all engagements"""
        return obj.engagements.count()


class ClientListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing clients"""
    
    active_engagements_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Client
        fields = [
            'id', 'client_code', 'client_name', 'company_name', 'industry',
            'is_active', 'active_engagements_count', 'created_at'
        ]
    
    def get_active_engagements_count(self, obj):
        return obj.get_active_engagements().count()


class EngagementSerializer(serializers.ModelSerializer):
    """Serializer for Engagement model"""
    
    # Related fields
    client_name = serializers.CharField(source='client.client_name', read_only=True)
    client_code = serializers.CharField(source='client.client_code', read_only=True)
    
    # Computed fields
    completeness_status = serializers.SerializerMethodField()
    uploaded_files_info = serializers.SerializerMethodField()
    file_completeness = serializers.SerializerMethodField()
    
    class Meta:
        model = Engagement
        fields = [
            'id', 'engagement_id', 'engagement_name', 'client', 'client_name', 'client_code',
            'fiscal_year', 'fiscal_year_start', 'fiscal_year_end', 
            'audit_start_date', 'audit_end_date', 'engagement_type', 'audit_opinion',
            'engagement_partner', 'engagement_manager', 'audit_team_members',
            'status', 'required_files', 'completeness_threshold',
            'description', 'notes', 'completeness_status', 'uploaded_files_info',
            'file_completeness', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_completeness_status(self, obj):
        """Get engagement completeness status"""
        return obj.get_completeness_status()
    
    def get_uploaded_files_info(self, obj):
        """Get information about uploaded files"""
        files = obj.data_files.all()
        return {
            'total_files': files.count(),
            'file_types': list(files.values_list('file_type', flat=True)),
            'validated_files': files.filter(is_validated=True).count(),
        }
    
    def get_file_completeness(self, obj):
        """Check if engagement has all required files"""
        return obj.is_complete_file_set()


class EngagementListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing engagements"""
    
    client_name = serializers.CharField(source='client.client_name', read_only=True)
    completeness_status = serializers.SerializerMethodField()
    files_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Engagement
        fields = [
            'id', 'engagement_id', 'engagement_name', 'client_name',
            'fiscal_year', 'status', 'completeness_status', 'files_count', 'created_at'
        ]
    
    def get_completeness_status(self, obj):
        return obj.get_completeness_status()
    
    def get_files_count(self, obj):
        return obj.data_files.count()


class GLAccountSerializer(serializers.ModelSerializer):
    """Serializer for GL Account unified view"""
    
    # Related fields
    engagement_name = serializers.CharField(source='engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='engagement.client.client_name', read_only=True)
    
    # Computed fields
    unified_view = serializers.SerializerMethodField()
    has_trial_balance_data = serializers.SerializerMethodField()
    has_chart_of_accounts_data = serializers.SerializerMethodField()
    has_gl_transactions = serializers.SerializerMethodField()
    
    class Meta:
        model = GLAccount
        fields = [
            'id', 'account_code', 'account_name', 'account_description',
            'engagement', 'engagement_name', 'client_name',
            'company_code', 'profit_center', 'cost_center', 'is_active', 'currency',
            # COA hierarchy
            'account_type', 'sub_type', 'sub_sub_type',
            # Trial Balance data
            'opening_balance', 'closing_balance', 'tb_debit', 'tb_credit',
            # Financial statement classification
            'financial_statement_category', 'balance_sheet_category', 'income_statement_category',
            # Computed fields
            'unified_view', 'has_trial_balance_data', 'has_chart_of_accounts_data', 'has_gl_transactions',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'engagement_id', 'client_name', 'created_at', 'updated_at']
    
    def get_unified_view(self, obj):
        """Get unified account view combining all data"""
        return obj.get_unified_view()
    
    def get_has_trial_balance_data(self, obj):
        """Check if account has trial balance data"""
        return bool(obj.opening_balance or obj.closing_balance)
    
    def get_has_chart_of_accounts_data(self, obj):
        """Check if account has chart of accounts data"""
        return bool(obj.account_type and obj.sub_type and obj.sub_sub_type)
    
    def get_has_gl_transactions(self, obj):
        """Check if account has GL transactions"""
        return obj.gl_postings.exists()


# ============================================================================
# TRANSACTION DATA SERIALIZERS
# ============================================================================

class SAPGLPostingSerializer(serializers.ModelSerializer):
    """Serializer for SAP GL Posting data with amount parsing support"""
    
    # Related fields
    engagement_name = serializers.CharField(source='data_file.engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='data_file.engagement.client.client_name', read_only=True)
    file_type = serializers.CharField(source='data_file.file_type', read_only=True)
    
    # GL Account details
    gl_account_name = serializers.CharField(source='gl_account_ref.account_name', read_only=True)
    account_type = serializers.CharField(source='gl_account_ref.account_type', read_only=True)
    
    # Computed fields
    transaction_type = serializers.ReadOnlyField()
    debit_amount = serializers.ReadOnlyField()
    credit_amount = serializers.ReadOnlyField()
    amount_display = serializers.SerializerMethodField()
    parsing_info = serializers.SerializerMethodField()
    
    class Meta:
        model = SAPGLPosting
        fields = [
            'id', 'data_file', 'engagement_name', 'client_name', 'file_type',
            'document_number', 'document_type', 'amount_local_currency', 'local_currency',
            'gl_account', 'gl_account_ref', 'gl_account_name', 'account_type',
            'gl_account_type', 'gl_account_sub_type', 'gl_account_sub_sub_type', 'gl_account_long_text',
            'financial_statement', 'ref_to_fs',
            'profit_center', 'user_name', 'posting_date', 'document_date', 'entry_date',
            'fiscal_year', 'posting_period', 'text', 'segment', 'clearing_document',
            'offsetting_account', 'invoice_reference', 'sales_document', 'assignment', 'year_month',
            'transaction_type', 'debit_amount', 'credit_amount', 'amount_display', 'parsing_info',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_amount_display(self, obj):
        """Get formatted amount display"""
        return obj.get_amount_display()
    
    def get_parsing_info(self, obj):
        """Get amount parsing information"""
        return obj.get_parsing_info()

class SAPGLPostingListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing SAP GL Postings"""
    
    engagement_name = serializers.CharField(source='data_file.engagement.engagement_name', read_only=True)
    transaction_type = serializers.ReadOnlyField()
    amount_display = serializers.SerializerMethodField()
    
    class Meta:
        model = SAPGLPosting
        fields = [
            'id', 'document_number', 'document_type', 'posting_date', 
            'amount_local_currency', 'local_currency', 'transaction_type', 'amount_display',
            'gl_account', 'profit_center', 'user_name', 'engagement_name',
            'fiscal_year', 'posting_period', 'created_at'
        ]
    
    def get_amount_display(self, obj):
        return obj.get_amount_display()


class TrialBalanceSerializer(serializers.ModelSerializer):
    """Serializer for Trial Balance data with amount parsing support"""
    
    # Related fields
    engagement_name = serializers.CharField(source='data_file.engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='data_file.engagement.client.client_name', read_only=True)
    gl_account_name = serializers.CharField(source='gl_account_ref.account_name', read_only=True)
    
    # Computed fields
    balance_movement = serializers.SerializerMethodField()
    amounts_display = serializers.SerializerMethodField()
    parsing_validation = serializers.SerializerMethodField()
    
    class Meta:
        model = TrialBalance
        fields = [
            'id', 'data_file', 'engagement_name', 'client_name',
            'company_code', 'gl_account', 'gl_account_ref', 'gl_account_name',
            'short_text', 'currency', 'opening_balance', 'closing_balance',
            'debit', 'credit', 'balance_movement', 'amounts_display', 'parsing_validation',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_balance_movement(self, obj):
        """Get balance movement"""
        return obj.get_balance_movement()
    
    def get_amounts_display(self, obj):
        """Get formatted amounts display"""
        return obj.get_amounts_display()
    
    def get_parsing_validation(self, obj):
        """Get parsing validation results"""
        return obj.get_parsing_validation()


class ChartOfAccountSerializer(serializers.ModelSerializer):
    """Serializer for Chart of Accounts data with hierarchy support"""
    
    # Related fields
    engagement_name = serializers.CharField(source='data_file.engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='data_file.engagement.client.client_name', read_only=True)
    gl_account_name = serializers.CharField(source='gl_account_ref.account_name', read_only=True)
    
    # Computed fields
    hierarchy_path = serializers.SerializerMethodField()
    hierarchy_dict = serializers.SerializerMethodField()
    amounts_display = serializers.SerializerMethodField()
    hierarchy_validation = serializers.SerializerMethodField()
    
    class Meta:
        model = ChartOfAccount
        fields = [
            'id', 'data_file', 'engagement_name', 'client_name',
            'account', 'type', 'sub_type', 'sub_sub_type', 'hierarchy_path', 'hierarchy_dict',
            'gl_account', 'gl_account_ref', 'gl_account_name', 'company', 'branch',
            'cost_center', 'code', 'gl_account_long_text', 'ref_to_fs', 'financial_statement',
            'ref_to_note', 'fiscal_year', 'q1_amount', 'adj_reclas', 'fin_q1_amount',
            'amounts_display', 'hierarchy_validation', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_hierarchy_path(self, obj):
        """Get hierarchy path"""
        return obj.get_hierarchy_path()
    
    def get_hierarchy_dict(self, obj):
        """Get hierarchy as dictionary"""
        return obj.get_hierarchy_dict()
    
    def get_amounts_display(self, obj):
        """Get formatted amounts display"""
        return obj.get_amounts_display()
    
    def get_hierarchy_validation(self, obj):
        """Get hierarchy validation"""
        return obj.get_hierarchy_validation()

# ============================================================================
# FILE MANAGEMENT SERIALIZERS
# ============================================================================

class DataFileSerializer(serializers.ModelSerializer):
    """Serializer for uploaded data files with engagement support"""
    
    # Related fields
    engagement_name = serializers.CharField(source='engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='engagement.client.client_name', read_only=True)
    
    # Computed fields
    file_type_display = serializers.CharField(source='get_file_type_display', read_only=True)
    validation_status = serializers.SerializerMethodField()
    
    class Meta:
        model = DataFile
        fields = [
            'id', 'file_name', 'file_size', 'file_hash', 'engagement', 'engagement_name',
            'file_type', 'file_type_display', 'is_validated', 'validation_errors',
            'client_name', 'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date',
            'version', 'version_notes',  # Version control fields
            'local_file_path',  # Local file storage
            'total_records', 'processed_records', 'failed_records', 'status',
            'uploaded_at', 'processed_at', 'error_message', 'min_date', 
            'max_date', 'min_amount', 'max_amount', 'validation_status',
            'created_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'uploaded_at', 'processed_at', 'engagement_id', 'client_name', 
            'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date',
            'created_at', 'updated_at'
        ]
    
    def get_validation_status(self, obj):
        """Get file validation status"""
        return {
            'is_validated': obj.is_validated,
            'error_count': len(obj.validation_errors) if obj.validation_errors else 0,
            'has_errors': bool(obj.validation_errors),
        }

class DataFileUploadSerializer(serializers.Serializer):
    """Serializer for file upload requests with engagement support"""
    
    file = serializers.FileField(help_text='CSV or Excel file containing audit data (TB, GL, COA)')
    description = serializers.CharField(max_length=500, required=False, help_text='Optional description of the file')
    
    # Engagement reference (primary method)
    engagement = serializers.UUIDField(required=False, help_text='Engagement UUID (preferred method)')
    
    # File type specification
    file_type = serializers.ChoiceField(
        choices=[('TB', 'Trial Balance'), ('GL', 'General Ledger'), ('COA', 'Chart of Accounts'), ('OTHER', 'Other')],
        help_text='Type of audit file being uploaded'
    )
    
    # Legacy fields for backward compatibility (will be used to create/find engagement)
    engagement_id = serializers.CharField(max_length=100, required=False, help_text='Engagement ID (legacy)')
    client_name = serializers.CharField(max_length=255, required=False, help_text='Client name (legacy)')
    company_name = serializers.CharField(max_length=255, required=False, help_text='Company name (legacy)')
    fiscal_year = serializers.IntegerField(required=False, help_text='Fiscal year (legacy)')
    audit_start_date = serializers.DateField(required=False, help_text='Audit start date (legacy)')
    audit_end_date = serializers.DateField(required=False, help_text='Audit end date (legacy)')
    
    def validate(self, data):
        """Validate that either engagement UUID or legacy fields are provided"""
        engagement = data.get('engagement')
        engagement_id = data.get('engagement_id')
        
        if not engagement and not engagement_id:
            raise serializers.ValidationError(
                'Either engagement UUID or engagement_id must be provided'
            )
        
        # If using legacy fields, ensure all required fields are present
        if not engagement and engagement_id:
            required_legacy_fields = ['client_name', 'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date']
            missing_fields = [field for field in required_legacy_fields if not data.get(field)]
            
            if missing_fields:
                raise serializers.ValidationError(
                    f'When using legacy mode, these fields are required: {", ".join(missing_fields)}'
                )
        
        return data




class AnalysisRequestSerializer(serializers.Serializer):
    """Serializer for analysis requests"""
    
    session_name = serializers.CharField(max_length=255, help_text='Name for the analysis session')
    description = serializers.CharField(max_length=1000, required=False, help_text='Optional description')
    
    # Date range filters
    date_from = serializers.DateField(required=False, help_text='Start date for analysis')
    date_to = serializers.DateField(required=False, help_text='End date for analysis')
    
    # Amount filters
    min_amount = serializers.DecimalField(
        max_digits=20, decimal_places=2, required=False, 
        help_text='Minimum amount to include'
    )
    max_amount = serializers.DecimalField(
        max_digits=20, decimal_places=2, required=False, 
        help_text='Maximum amount to include'
    )
    
    # Specific filters
    document_types = serializers.ListField(
        child=serializers.CharField(max_length=10), 
        required=False, 
        help_text='Filter by document types'
    )
    gl_accounts = serializers.ListField(
        child=serializers.CharField(max_length=20), 
        required=False, 
        help_text='Filter by G/L accounts'
    )
    profit_centers = serializers.ListField(
        child=serializers.CharField(max_length=20), 
        required=False, 
        help_text='Filter by profit centers'
    )
    users = serializers.ListField(
        child=serializers.CharField(max_length=50), 
        required=False, 
        help_text='Filter by users'
    )
    
    # Analysis options
    include_high_value_only = serializers.BooleanField(
        default=False, 
        help_text='Only analyze high-value transactions (> 1M SAR)'
    )
    include_cleared_only = serializers.BooleanField(
        default=False, 
        help_text='Only analyze cleared transactions'
    )
    include_arabic_text_only = serializers.BooleanField(
        default=False, 
        help_text='Only analyze transactions with Arabic text'
    )

class AnalysisSummarySerializer(serializers.Serializer):
    """Serializer for comprehensive analysis summary results with specific anomaly tests"""
    
    session_id = serializers.UUIDField()
    session_name = serializers.CharField()
    status = serializers.CharField()
    
    # Summary statistics
    total_transactions = serializers.IntegerField()
    total_amount = serializers.DecimalField(max_digits=20, decimal_places=2)
    flagged_transactions = serializers.IntegerField()
    high_value_transactions = serializers.IntegerField()
    flag_rate = serializers.FloatField()
    
    # Risk distribution
    risk_distribution = serializers.ListField()
    
    # Anomaly summary
    anomaly_summary = serializers.DictField()
    
    # Detailed anomaly data
    duplicate_entries = serializers.ListField()
    user_anomalies = serializers.ListField()
    backdated_entries = serializers.ListField()
    closing_entries = serializers.ListField()
    unusual_days = serializers.ListField()
    holiday_entries = serializers.ListField()
    
    # Charts data
    charts_data = serializers.DictField()
    
    # Timing information
    created_at = serializers.DateTimeField()
    started_at = serializers.DateTimeField(allow_null=True)
    completed_at = serializers.DateTimeField(allow_null=True)
    duration = serializers.FloatField(allow_null=True)

class DataUploadResponseSerializer(serializers.Serializer):
    """Serializer for data upload responses"""
    
    id = serializers.UUIDField()
    file_name = serializers.CharField()
    engagement_id = serializers.CharField()
    client_name = serializers.CharField()
    company_name = serializers.CharField()
    fiscal_year = serializers.IntegerField()
    audit_start_date = serializers.DateField()
    audit_end_date = serializers.DateField()
    status = serializers.CharField()
    total_records = serializers.IntegerField()
    processed_records = serializers.IntegerField()
    failed_records = serializers.IntegerField()
    error_message = serializers.CharField(allow_null=True)
    
    # Data range information
    min_date = serializers.DateField(allow_null=True)
    max_date = serializers.DateField(allow_null=True)
    min_amount = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    max_amount = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)

class DashboardStatsSerializer(serializers.Serializer):
    """Serializer for dashboard statistics"""
    
    # Overall statistics
    total_transactions = serializers.IntegerField()
    total_amount = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_files = serializers.IntegerField()
    total_sessions = serializers.IntegerField()
    
    # Recent activity
    recent_transactions = serializers.IntegerField()
    recent_files = serializers.IntegerField()
    recent_sessions = serializers.IntegerField()
    
    # Risk statistics
    flagged_transactions = serializers.IntegerField()
    high_risk_transactions = serializers.IntegerField()
    critical_risk_transactions = serializers.IntegerField()
    
    # Top statistics
    top_users = serializers.ListField()
    top_profit_centers = serializers.ListField()
    top_document_types = serializers.ListField()
    
    # Date range
    date_from = serializers.DateField()
    date_to = serializers.DateField()


class TrialBalanceSerializer(serializers.Serializer):
    """Serializer for Trial Balance data"""
    
    account_id = serializers.CharField()
    account_name = serializers.CharField()
    account_type = serializers.CharField()
    account_category = serializers.CharField()
    normal_balance = serializers.CharField()
    
    # Opening balances
    opening_debit = serializers.DecimalField(max_digits=20, decimal_places=2)
    opening_credit = serializers.DecimalField(max_digits=20, decimal_places=2)
    
    # Current period movements
    period_debit = serializers.DecimalField(max_digits=20, decimal_places=2)
    period_credit = serializers.DecimalField(max_digits=20, decimal_places=2)
    
    # Closing balances
    closing_debit = serializers.DecimalField(max_digits=20, decimal_places=2)
    closing_credit = serializers.DecimalField(max_digits=20, decimal_places=2)
    
    # Net balance
    net_balance = serializers.DecimalField(max_digits=20, decimal_places=2)
    
    # Transaction counts
    transaction_count = serializers.IntegerField()


class DuplicateAnomalySerializer(serializers.Serializer):
    """Serializer for duplicate anomaly data"""
    
    # Basic duplicate information
    type = serializers.CharField(help_text='Type of duplicate (Type 1-6)')
    criteria = serializers.CharField(help_text='Criteria used for duplicate detection')
    gl_account = serializers.CharField(help_text='GL Account involved in duplicate')
    amount = serializers.DecimalField(max_digits=20, decimal_places=2, help_text='Amount involved in duplicate')
    count = serializers.IntegerField(help_text='Number of duplicate transactions')
    risk_score = serializers.FloatField(help_text='Risk score for this duplicate group')
    
    # Additional fields based on duplicate type
    user_name = serializers.CharField(allow_null=True, help_text='User name (for Type 3)')
    posting_date = serializers.DateField(allow_null=True, help_text='Posting date (for Type 4)')
    document_date = serializers.DateField(allow_null=True, help_text='Document date (for Type 5)')
    source = serializers.CharField(allow_null=True, help_text='Source/Document type (for Type 2, 6)')
    
    # Transaction details
    transactions = serializers.ListField(help_text='List of duplicate transactions')

class DuplicateAnomalyListSerializer(serializers.Serializer):
    """Serializer for duplicate anomaly list with summary"""
    
    # Summary statistics
    total_duplicates = serializers.IntegerField(help_text='Total number of duplicate groups')
    total_transactions_involved = serializers.IntegerField(help_text='Total transactions involved in duplicates')
    total_amount_involved = serializers.DecimalField(max_digits=20, decimal_places=2, help_text='Total amount involved in duplicates')
    
    # Breakdown by type
    type_breakdown = serializers.DictField(help_text='Breakdown of duplicates by type')
    
    # List of duplicates
    duplicates = DuplicateAnomalySerializer(many=True, help_text='List of duplicate anomalies')

class DuplicateChartDataSerializer(serializers.Serializer):
    """Serializer for duplicate charts data"""
    
    # Breakdown of duplicate flags chart
    duplicate_flags_breakdown = serializers.DictField(help_text='Breakdown of duplicate flags by type')
    
    # Debit/Credit amounts and journal line count per duplicate and month
    monthly_duplicate_data = serializers.ListField(help_text='Monthly duplicate data with amounts and counts')
    
    # Breakdown of duplicates per impacted user
    user_breakdown = serializers.ListField(help_text='Breakdown of duplicates by user')
    
    # Duplicate type breakdown (Type 3, 4, 5, 6)
    duplicate_type_breakdown = serializers.DictField(help_text='Detailed breakdown by duplicate type')
    
    # Breakdown of duplicates per impacted FS line
    fs_line_breakdown = serializers.ListField(help_text='Breakdown of duplicates by financial statement line')

class DuplicateAnalysisRequestSerializer(serializers.Serializer):
    """Serializer for duplicate analysis requests"""
    
    # Date range filters
    date_from = serializers.DateField(required=False, help_text='Start date for analysis')
    date_to = serializers.DateField(required=False, help_text='End date for analysis')
    
    # Amount filters
    min_amount = serializers.DecimalField(
        max_digits=20, decimal_places=2, required=False, 
        help_text='Minimum amount to include'
    )
    max_amount = serializers.DecimalField(
        max_digits=20, decimal_places=2, required=False, 
        help_text='Maximum amount to include'
    )
    
    # Specific filters
    gl_accounts = serializers.ListField(
        child=serializers.CharField(max_length=20), 
        required=False, 
        help_text='Filter by G/L accounts'
    )
    users = serializers.ListField(
        child=serializers.CharField(max_length=50), 
        required=False, 
        help_text='Filter by users'
    )
    document_types = serializers.ListField(
        child=serializers.CharField(max_length=10), 
        required=False, 
        help_text='Filter by document types'
    )
    
    # Duplicate detection options
    duplicate_threshold = serializers.IntegerField(
        default=2, 
        min_value=2, 
        max_value=10,
        help_text='Minimum count for duplicate detection (2-10)'
    )
    include_all_types = serializers.BooleanField(
        default=True, 
        help_text='Include all duplicate types (1-6)'
    )
    duplicate_types = serializers.ListField(
        child=serializers.IntegerField(min_value=1, max_value=6),
        required=False,
        help_text='Specific duplicate types to include (1-6)'
    )

class DuplicateTrainingDataSerializer(serializers.Serializer):
    """Serializer for duplicate training data"""
    
    # Training data for machine learning
    training_features = serializers.ListField(help_text='Features for training')
    training_labels = serializers.ListField(help_text='Labels for training')
    
    # Training metadata
    total_samples = serializers.IntegerField(help_text='Total number of training samples')
    duplicate_samples = serializers.IntegerField(help_text='Number of duplicate samples')
    non_duplicate_samples = serializers.IntegerField(help_text='Number of non-duplicate samples')
    
    # Feature importance
    feature_importance = serializers.DictField(help_text='Feature importance scores')
    
    # Model performance metrics
    model_metrics = serializers.DictField(help_text='Model performance metrics')

class DuplicateAnomalyComprehensiveSerializer(serializers.Serializer):
    """Comprehensive serializer for duplicate anomaly API response"""
    
    # Sheet identification
    sheet_id = serializers.CharField(help_text='Sheet ID for the analysis')
    
    # Summary statistics
    total_duplicates = serializers.IntegerField(help_text='Total number of duplicate groups')
    total_transactions_involved = serializers.IntegerField(help_text='Total transactions involved in duplicates')
    total_amount_involved = serializers.DecimalField(max_digits=20, decimal_places=2, help_text='Total amount involved in duplicates')
    
    # Type breakdown
    type_breakdown = serializers.DictField(help_text='Breakdown of duplicates by type')
    
    # List of duplicates
    duplicates = DuplicateAnomalySerializer(many=True, help_text='List of duplicate anomalies')
    
    # Charts data
    charts_data = DuplicateChartDataSerializer(help_text='Charts and visualization data')
    
    # Training data
    training_data = DuplicateTrainingDataSerializer(help_text='Machine learning training data')
    
    # Optional message for POST requests
    message = serializers.CharField(required=False, help_text='Analysis completion message')

class TargetedAnomalyUploadSerializer(serializers.Serializer):
    """Serializer for file upload with targeted anomaly detection"""
    
    # File upload
    file = serializers.FileField(help_text='CSV file containing SAP GL posting data')
    
    # Standard file metadata
    engagement_id = serializers.CharField(max_length=100, help_text='Engagement ID for the audit')
    client_name = serializers.CharField(max_length=255, help_text='Client name')
    company_name = serializers.CharField(max_length=255, help_text='Company name')
    fiscal_year = serializers.IntegerField(help_text='Fiscal year for the audit')
    audit_start_date = serializers.DateField(help_text='Audit start date')
    audit_end_date = serializers.DateField(help_text='Audit end date')
    description = serializers.CharField(max_length=500, required=False, help_text='Optional description of the file')
    
    # Anomaly detection configuration
    run_anomalies = serializers.BooleanField(default=False, help_text='Whether to run anomaly detection')
    anomalies = serializers.ListField(
        child=serializers.ChoiceField(choices=[
            ('duplicate', 'Duplicate Detection'),
            ('backdated', 'Backdated Entries'),
            ('closing', 'Closing Entries'),
            ('unusual_days', 'Unusual Days'),
            ('holiday', 'Holiday Entries'),
            ('user_anomalies', 'User Anomalies'),
        ]),
        required=False,
        default=list,
        help_text='List of specific anomaly types to run'
    )

class FileProcessingJobSerializer(serializers.ModelSerializer):
    """Serializer for FileProcessingJob model"""
    
    data_file = DataFileSerializer(read_only=True)
    processing_summary = serializers.SerializerMethodField()
    
    class Meta:
        model = FileProcessingJob
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']
    
    def get_processing_summary(self, obj):
        """Get processing summary"""
        return obj.get_processing_summary()

class FileProcessingJobListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing FileProcessingJob"""
    
    file_name = serializers.CharField(source='data_file.file_name', read_only=True)
    file_size = serializers.IntegerField(source='data_file.file_size', read_only=True)
    client_name = serializers.CharField(source='data_file.client_name', read_only=True)
    engagement_id = serializers.CharField(source='data_file.engagement_id', read_only=True)
    
    class Meta:
        model = FileProcessingJob
        fields = [
            'id', 'file_name', 'file_size', 'client_name', 'engagement_id',
            'run_anomalies', 'requested_anomalies', 'status', 'processing_duration',
            'created_at', 'started_at', 'completed_at'
        ]

class TargetedAnomalyResponseSerializer(serializers.Serializer):
    """Serializer for targeted anomaly detection response"""
    
    # Job information
    job_id = serializers.UUIDField(help_text='Processing job ID')
    status = serializers.CharField(help_text='Current processing status')
    
    # File information
    file_info = DataFileSerializer(help_text='Uploaded file information')
    
    # Processing results
    analytics_results = serializers.DictField(help_text='Default analytics results (TB, TE, GL summaries)')
    anomaly_results = serializers.DictField(help_text='Results from requested anomaly tests')
    
    # Processing metadata
    processing_duration = serializers.FloatField(allow_null=True, help_text='Processing duration in seconds')
    is_duplicate_content = serializers.BooleanField(help_text='Whether this is duplicate content')
    existing_job_id = serializers.UUIDField(allow_null=True, help_text='Reference to existing job if duplicate')
    
    # Timestamps
    created_at = serializers.DateTimeField(help_text='Job creation timestamp')
    started_at = serializers.DateTimeField(allow_null=True, help_text='Processing start timestamp')
    completed_at = serializers.DateTimeField(allow_null=True, help_text='Processing completion timestamp')
    
    # Optional message
    message = serializers.CharField(required=False, help_text='Processing completion message')

class MLModelTrainingSerializer(serializers.ModelSerializer):
    """Serializer for ML Model Training"""
    
    training_summary = serializers.SerializerMethodField()
    
    class Meta:
        model = MLModelTraining
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at', 'started_at', 'completed_at']
    
    def get_training_summary(self, obj):
        """Get training summary"""
        return obj.get_training_summary()

class MLModelTrainingListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing ML Model Training sessions"""
    
    class Meta:
        model = MLModelTraining
        fields = [
            'id', 'session_name', 'model_type', 'status', 'training_data_size',
            'feature_count', 'performance_metrics', 'training_duration',
            'model_version', 'is_latest_model', 'created_at', 'started_at', 'completed_at'
        ]

class MLModelTrainingRequestSerializer(serializers.Serializer):
    """Serializer for ML model training requests"""
    
    session_name = serializers.CharField(max_length=255, help_text='Training session name')
    description = serializers.CharField(max_length=1000, required=False, help_text='Training session description')
    
    # Model configuration
    model_type = serializers.ChoiceField(choices=[
        ('isolation_forest', 'Isolation Forest'),
        ('random_forest', 'Random Forest'),
        ('dbscan', 'DBSCAN'),
        ('ensemble', 'Ensemble'),
        ('all', 'All Models'),
    ], help_text='Type of ML model to train')
    
    # Training data filters
    date_from = serializers.DateField(required=False, help_text='Start date for training data')
    date_to = serializers.DateField(required=False, help_text='End date for training data')
    min_transactions = serializers.IntegerField(default=100, help_text='Minimum number of transactions required')
    
    # Model parameters
    training_parameters = serializers.DictField(required=False, default=dict, help_text='Model-specific training parameters')

class MLModelInfoSerializer(serializers.Serializer):
    """Serializer for ML model information"""
    
    is_trained = serializers.BooleanField(help_text='Whether models are trained')
    models_dir = serializers.CharField(help_text='Directory where models are stored')
    feature_count = serializers.IntegerField(help_text='Number of features used')
    models_available = serializers.ListField(help_text='List of available models')
    label_encoders = serializers.ListField(help_text='List of label encoders')
    
    # Performance information
    latest_performance = serializers.DictField(required=False, help_text='Latest model performance metrics')
    training_history = serializers.ListField(required=False, help_text='Training history')

class MLPredictionSerializer(serializers.Serializer):
    """Serializer for ML prediction results"""
    
    transaction_id = serializers.CharField(help_text='Transaction ID')
    anomaly_score = serializers.FloatField(help_text='Anomaly score (0-1)')
    confidence = serializers.FloatField(help_text='Prediction confidence (0-100)')
    model = serializers.CharField(help_text='Model that made the prediction')
    ensemble_prediction = serializers.BooleanField(required=False, help_text='Whether this is an ensemble prediction')
    models_agreed = serializers.ListField(required=False, help_text='Models that agreed on this prediction')
    model_count = serializers.IntegerField(required=False, help_text='Number of models that predicted this as anomaly')

class MLAnomalyResultsSerializer(serializers.Serializer):
    """Serializer for ML anomaly detection results"""
    
    # Individual model predictions
    isolation_forest = MLPredictionSerializer(many=True, required=False)
    random_forest = MLPredictionSerializer(many=True, required=False)
    dbscan = MLPredictionSerializer(many=True, required=False)
    
    # Ensemble predictions
    ensemble_predictions = MLPredictionSerializer(many=True, required=False)
    
    # Summary statistics
    total_anomalies = serializers.IntegerField(help_text='Total anomalies detected')
    model_performance = serializers.DictField(help_text='Performance metrics for each model')
    processing_time = serializers.FloatField(help_text='Processing time in seconds') 

class ClosingEntriesListSerializer(serializers.Serializer):
    """Serializer for closing entries listing with pagination"""
    
    # Basic transaction information from actual data structure
    transaction_id = serializers.CharField(help_text='Transaction ID')
    document_number = serializers.CharField(help_text='SAP Document Number')
    posting_date = serializers.CharField(help_text='Posting Date')
    account = serializers.CharField(help_text='G/L Account Number')
    amount = serializers.FloatField(help_text='Amount in Local Currency')
    user = serializers.CharField(help_text='User Name')
    
    # Closing entry specific information
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.FloatField(help_text='Risk score (0-100)')
    days_from_month_end = serializers.IntegerField(help_text='Days from month end')
    
    # Additional computed fields
    is_high_value = serializers.SerializerMethodField(help_text='Whether this is a high value transaction')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    month_end_indicator = serializers.SerializerMethodField(help_text='Whether this is a month-end transaction')
    amount_category = serializers.SerializerMethodField(help_text='Amount category (High/Medium/Low)')
    
    def get_is_high_value(self, obj):
        """Check if transaction is high value (> 10M)"""
        return float(obj.get('amount', 0)) > 10000000
    
    def get_amount_formatted(self, obj):
        """Format amount with currency"""
        amount = float(obj.get('amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_risk_color(self, obj):
        """Get risk color for UI display"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_colors = {
            'LOW': '#28a745',      # Green
            'MEDIUM': '#ffc107',   # Yellow
            'HIGH': '#fd7e14',     # Orange
            'CRITICAL': '#dc3545'  # Red
        }
        return risk_colors.get(risk_level, '#6c757d')  # Default gray
    
    def get_month_end_indicator(self, obj):
        """Check if this is a month-end transaction (0-1 days from month end)"""
        days_from_month_end = obj.get('days_from_month_end', 0)
        return days_from_month_end <= 1
    
    def get_amount_category(self, obj):
        """Get amount category"""
        amount = float(obj.get('amount', 0))
        if amount > 10000000:  # > 10M
            return 'HIGH'
        elif amount > 1000000:  # 1M-10M
            return 'MEDIUM'
        else:  # < 1M
            return 'LOW' 

class BackdatedEntriesListSerializer(serializers.Serializer):
    """Serializer for backdated entries listing with pagination"""
    
    # Basic transaction information from actual data structure
    transaction_id = serializers.CharField(help_text='Transaction ID')
    document_number = serializers.CharField(help_text='SAP Document Number')
    posting_date = serializers.CharField(help_text='Posting Date')
    document_date = serializers.CharField(help_text='Document Date')
    account = serializers.CharField(help_text='G/L Account Number')
    amount = serializers.FloatField(help_text='Amount in Local Currency')
    user = serializers.CharField(help_text='User Name')
    
    # Backdated specific information
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.FloatField(help_text='Risk score (0-100)')
    days_difference = serializers.IntegerField(help_text='Days difference between posting and document date')
    
    # Additional computed fields
    is_high_value = serializers.SerializerMethodField(help_text='Whether this is a high value transaction')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    backdated_severity = serializers.SerializerMethodField(help_text='Backdated severity level')
    amount_category = serializers.SerializerMethodField(help_text='Amount category (High/Medium/Low)')
    
    def get_is_high_value(self, obj):
        """Check if transaction is high value (> 10M)"""
        return float(obj.get('amount', 0)) > 10000000
    
    def get_amount_formatted(self, obj):
        """Format amount with currency"""
        amount = float(obj.get('amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_risk_color(self, obj):
        """Get risk color for UI display"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_colors = {
            'LOW': '#28a745',      # Green
            'MEDIUM': '#ffc107',   # Yellow
            'HIGH': '#fd7e14',     # Orange
            'CRITICAL': '#dc3545'  # Red
        }
        return risk_colors.get(risk_level, '#6c757d')  # Default gray
    
    def get_backdated_severity(self, obj):
        """Get backdated severity level based on days difference"""
        days_difference = obj.get('days_difference', 0)
        if days_difference <= 7:
            return 'MINOR'
        elif days_difference <= 30:
            return 'MODERATE'
        elif days_difference <= 90:
            return 'SIGNIFICANT'
        else:
            return 'CRITICAL'
    
    def get_amount_category(self, obj):
        """Get amount category"""
        amount = float(obj.get('amount', 0))
        if amount > 10000000:  # > 10M
            return 'HIGH'
        elif amount > 1000000:  # 1M-10M
            return 'MEDIUM'
        else:  # < 1M
            return 'LOW' 

class UnusualDaysListSerializer(serializers.Serializer):
    """Serializer for unusual days entries listing with pagination"""
    transaction_id = serializers.CharField(help_text='Transaction ID')
    document_number = serializers.CharField(help_text='SAP Document Number')
    posting_date = serializers.CharField(help_text='Posting Date')
    account = serializers.CharField(help_text='G/L Account Number')
    amount = serializers.FloatField(help_text='Amount in Local Currency')
    user = serializers.CharField(help_text='User Name')
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.FloatField(help_text='Risk score (0-100)')
    day_of_week = serializers.CharField(help_text='Day of the week')
    is_high_value = serializers.SerializerMethodField(help_text='Whether this is a high value transaction')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    day_type = serializers.SerializerMethodField(help_text='Type of day (Weekend, Holiday, etc.)')
    amount_category = serializers.SerializerMethodField(help_text='Amount category (High/Medium/Low)')

    def get_is_high_value(self, obj):
        return float(obj.get('amount', 0)) > 10000000

    def get_amount_formatted(self, obj):
        return f"{float(obj.get('amount', 0)):,.2f} SAR"

    def get_risk_color(self, obj):
        risk_colors = {
            'LOW': '#28a745', 
            'MEDIUM': '#ffc107', 
            'HIGH': '#fd7e14', 
            'CRITICAL': '#dc3545'
        }
        return risk_colors.get(obj.get('risk_level', 'LOW'), '#6c757d') 

    def get_day_type(self, obj):
        day_of_week = obj.get('day_of_week', '').lower()
        if day_of_week in ['friday', 'saturday']:
            return 'WEEKEND'
        return 'WEEKDAY'

    def get_amount_category(self, obj):
        """Get amount category"""
        amount = float(obj.get('amount', 0))
        if amount > 10000000:  # > 10M
            return 'HIGH'
        elif amount > 1000000:  # 1M-10M
            return 'MEDIUM'
        else:  # < 1M
            return 'LOW' 


class DuplicateListSerializer(serializers.Serializer):
    """Serializer for duplicate entries listing with pagination"""
    
    # Basic transaction information
    transaction_id = serializers.CharField(help_text='Transaction ID')
    document_number = serializers.CharField(help_text='SAP Document Number')
    posting_date = serializers.CharField(help_text='Posting Date')
    account = serializers.CharField(help_text='G/L Account Number')
    amount = serializers.FloatField(help_text='Amount in Local Currency')
    user = serializers.CharField(help_text='User Name')
    
    # Duplicate specific information
    duplicate_type = serializers.CharField(help_text='Type of duplicate (Type 1-6)')
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.FloatField(help_text='Risk score (0-100)')
    similarity_score = serializers.FloatField(help_text='Similarity score (0-1)')
    matching_fields = serializers.ListField(help_text='Fields that match between duplicates')
    
    # Additional computed fields
    is_high_value = serializers.SerializerMethodField(help_text='Whether this is a high value transaction')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    duplicate_severity = serializers.SerializerMethodField(help_text='Duplicate severity level')
    amount_category = serializers.SerializerMethodField(help_text='Amount category (High/Medium/Low)')
    duplicate_group_id = serializers.CharField(help_text='Unique identifier for the duplicate group')
    
    def get_is_high_value(self, obj):
        """Check if transaction is high value (> 10M)"""
        return float(obj.get('amount', 0)) > 10000000
    
    def get_amount_formatted(self, obj):
        """Format amount with currency"""
        amount = float(obj.get('amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_risk_color(self, obj):
        """Get risk color for UI display"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_colors = {
            'LOW': '#28a745',      # Green
            'MEDIUM': '#ffc107',   # Yellow
            'HIGH': '#fd7e14',     # Orange
            'CRITICAL': '#dc3545'  # Red
        }
        return risk_colors.get(risk_level, '#6c757d')  # Default gray
    
    def get_duplicate_severity(self, obj):
        """Get duplicate severity level based on type and risk score"""
        duplicate_type = obj.get('duplicate_type', '')
        risk_score = float(obj.get('risk_score', 0))
        
        if 'Type 6' in duplicate_type or risk_score > 80:
            return 'CRITICAL'
        elif 'Type 5' in duplicate_type or risk_score > 60:
            return 'HIGH'
        elif 'Type 4' in duplicate_type or risk_score > 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_amount_category(self, obj):
        """Get amount category"""
        amount = float(obj.get('amount', 0))
        if amount > 10000000:  # > 10M
            return 'HIGH'
        elif amount > 1000000:  # 1M-10M
            return 'MEDIUM'
        else:  # < 1M
            return 'LOW'


class UserListSerializer(serializers.Serializer):
    """Serializer for user entries listing with pagination"""
    
    # Basic user information
    user = serializers.CharField(help_text='User Name')
    transaction_count = serializers.IntegerField(help_text='Number of transactions')
    total_amount = serializers.FloatField(help_text='Total amount of transactions')
    avg_amount = serializers.FloatField(help_text='Average transaction amount')
    
    # User specific information
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.FloatField(help_text='Risk score (0-100)')
    anomaly_count = serializers.IntegerField(help_text='Number of anomalies detected')
    accounts = serializers.ListField(help_text='List of accounts used by user')
    
    # Additional computed fields
    is_high_activity = serializers.SerializerMethodField(help_text='Whether this is a high activity user')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted total amount with currency')
    avg_amount_formatted = serializers.SerializerMethodField(help_text='Formatted average amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    user_severity = serializers.SerializerMethodField(help_text='User severity level')
    activity_category = serializers.SerializerMethodField(help_text='Activity category (Low/Medium/High)')
    accounts_count = serializers.SerializerMethodField(help_text='Number of unique accounts')
    
    def get_is_high_activity(self, obj):
        """Check if user has high activity (> 50 transactions)"""
        return int(obj.get('transaction_count', 0)) > 50
    
    def get_amount_formatted(self, obj):
        """Format total amount with currency"""
        amount = float(obj.get('total_amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_avg_amount_formatted(self, obj):
        """Format average amount with currency"""
        amount = float(obj.get('avg_amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_risk_color(self, obj):
        """Get risk color for UI display"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_colors = {
            'LOW': '#28a745',      # Green
            'MEDIUM': '#ffc107',   # Yellow
            'HIGH': '#fd7e14',     # Orange
            'CRITICAL': '#dc3545'  # Red
        }
        return risk_colors.get(risk_level, '#6c757d')  # Default gray
    
    def get_user_severity(self, obj):
        """Get user severity level based on risk score and anomalies"""
        risk_score = float(obj.get('risk_score', 0))
        anomaly_count = int(obj.get('anomaly_count', 0))
        
        if risk_score > 80 or anomaly_count > 10:
            return 'CRITICAL'
        elif risk_score > 60 or anomaly_count > 5:
            return 'HIGH'
        elif risk_score > 40 or anomaly_count > 2:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_activity_category(self, obj):
        """Get activity category based on transaction count"""
        transaction_count = int(obj.get('transaction_count', 0))
        if transaction_count > 100:
            return 'HIGH'
        elif transaction_count > 20:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_accounts_count(self, obj):
        """Get number of unique accounts"""
        accounts = obj.get('accounts', [])
        return len(accounts) if isinstance(accounts, list) else 0


class HolidayListSerializer(serializers.Serializer):
    """Serializer for holiday entries listing with pagination"""
    
    # Basic transaction information from actual data structure
    transaction_id = serializers.CharField(help_text='Transaction ID')
    document_number = serializers.CharField(help_text='SAP Document Number')
    posting_date = serializers.CharField(help_text='Posting Date')
    account = serializers.CharField(help_text='G/L Account Number')
    amount = serializers.FloatField(help_text='Amount in Local Currency')
    user = serializers.CharField(help_text='User Name')
    
    # Holiday specific information
    holiday_name = serializers.CharField(help_text='Name of the holiday', allow_null=True)
    risk_level = serializers.CharField(help_text='Risk level (LOW, MEDIUM, HIGH, CRITICAL)')
    risk_score = serializers.SerializerMethodField(help_text='Risk score (0-100)')
    
    def get_risk_score(self, obj):
        """Calculate risk score based on risk level"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_scores = {
            'LOW': 25.0,
            'MEDIUM': 50.0,
            'HIGH': 75.0,
            'CRITICAL': 100.0
        }
        return risk_scores.get(risk_level, 0.0)
    
    # Additional computed fields
    is_high_value = serializers.SerializerMethodField(help_text='Whether this is a high value transaction')
    amount_formatted = serializers.SerializerMethodField(help_text='Formatted amount with currency')
    risk_color = serializers.SerializerMethodField(help_text='Risk color for UI display')
    holiday_severity = serializers.SerializerMethodField(help_text='Holiday severity level')
    amount_category = serializers.SerializerMethodField(help_text='Amount category (High/Medium/Low)')
    
    def get_is_high_value(self, obj):
        """Check if transaction is high value (> 10M)"""
        return float(obj.get('amount', 0)) > 10000000
    
    def get_amount_formatted(self, obj):
        """Format amount with currency"""
        amount = float(obj.get('amount', 0))
        return f"{amount:,.2f} SAR"
    
    def get_risk_color(self, obj):
        """Get risk color for UI display"""
        risk_level = obj.get('risk_level', 'LOW')
        risk_colors = {
            'LOW': '#28a745',      # Green
            'MEDIUM': '#ffc107',   # Yellow
            'HIGH': '#fd7e14',     # Orange
            'CRITICAL': '#dc3545'  # Red
        }
        return risk_colors.get(risk_level, '#6c757d')  # Default gray
    
    def get_holiday_severity(self, obj):
        """Get holiday severity level based on risk score"""
        risk_score = float(obj.get('risk_score', 0))
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MODERATE'
        else:
            return 'LOW'
    
    def get_amount_category(self, obj):
        """Get amount category"""
        amount = float(obj.get('amount', 0))
        if amount > 10000000:  # > 10M
            return 'HIGH'
        elif amount > 1000000:  # 1M-10M
            return 'MEDIUM'
        else:  # < 1M
            return 'LOW' 


# ============================================================================
# COMPLETENESS TEST RESULT SERIALIZER
# ============================================================================

# ============================================================================
# ENHANCED COMPLETENESS TEST SERIALIZERS
# ============================================================================

class CompletenessTestResultSerializer(serializers.ModelSerializer):
    """Serializer for Engagement-level Completeness Test Results"""
    
    # Related fields
    engagement_name = serializers.CharField(source='engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='engagement.client.client_name', read_only=True)
    
    # File information
    gl_file_name = serializers.CharField(source='gl_file.file_name', read_only=True)
    tb_file_name = serializers.CharField(source='tb_file.file_name', read_only=True)
    coa_file_name = serializers.CharField(source='coa_file.file_name', read_only=True)
    
    # Computed fields
    status_display = serializers.CharField(source='get_overall_status_display', read_only=True)
    test_duration = serializers.SerializerMethodField()
    test_summary = serializers.SerializerMethodField()
    engagement_completeness_status = serializers.SerializerMethodField()
    failed_tests = serializers.SerializerMethodField()
    
    class Meta:
        model = CompletenessTestResult
        fields = [
            'id', 'engagement', 'engagement_name', 'client_name', 'test_timestamp', 'test_version',
            'data_version', 'version_notes',  # Version control fields
            'gl_file', 'tb_file', 'coa_file', 'gl_file_name', 'tb_file_name', 'coa_file_name',
            'overall_status', 'status_display', 'overall_explanation', 'completeness_score',
            # Enhanced test steps
            'step1_file_completeness', 'step2_gl_tb_reconciliation',
            # Document verification fields
            'total_documents', 'balanced_documents', 'unbalanced_documents', 'document_balance_rate',
            'total_document_variance', 'average_document_variance', 'transactions_with_documents',
            'transactions_without_documents', 'no_document_transaction_count',
            # Enhanced statistics
            'total_gl_records', 'total_tb_records', 'total_coa_records', 'total_accounts_unified',
            'tests_passed', 'total_tests', 'critical_issues_count', 'comprehensive_statistics',
            'processing_duration', 'test_duration', 'test_summary', 'engagement_completeness_status', 'failed_tests',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'test_timestamp', 'processing_duration', 'engagement_id', 'created_at', 'updated_at']
    
    def get_test_duration(self, obj):
        """Get formatted test duration"""
        if obj.processing_duration:
            return f"{obj.processing_duration:.2f} seconds"
        return None
    
    def get_test_summary(self, obj):
        """Get comprehensive test summary"""
        return obj.get_summary()
    
    def get_engagement_completeness_status(self, obj):
        """Get engagement-level completeness status"""
        return obj.get_engagement_completeness_status()
    
    def get_failed_tests(self, obj):
        """Get list of failed tests"""
        return obj.get_failed_tests()


class CompletenessTestSummarySerializer(serializers.ModelSerializer):
    """Simplified serializer for completeness test summary"""
    
    engagement_name = serializers.CharField(source='engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='engagement.client.client_name', read_only=True)
    status_display = serializers.CharField(source='get_overall_status_display', read_only=True)
    
    class Meta:
        model = CompletenessTestResult
        fields = [
            'id', 'engagement', 'engagement_name', 'client_name', 'test_timestamp',
            'overall_status', 'status_display', 'completeness_score',
            'tests_passed', 'total_tests', 'critical_issues_count',
            'total_gl_records', 'total_tb_records', 'total_coa_records', 'total_accounts_unified'
        ]


# ============================================================================
# AMOUNT PARSING UTILITY SERIALIZERS
# ============================================================================

class AmountParsingTestSerializer(serializers.Serializer):
    """Serializer for testing amount parsing functionality"""
    
    amount_string = serializers.CharField(help_text='Amount string to parse')
    default_zero = serializers.BooleanField(default=False, help_text='Return zero for invalid amounts')
    
    def validate_and_parse(self):
        """Validate and parse the amount"""
        amount_str = self.validated_data['amount_string']
        default_zero = self.validated_data['default_zero']
        
        # Parse the amount
        parsed_amount = AmountParser.parse_amount(amount_str, default_zero)
        debit, credit = AmountParser.parse_debit_credit_amounts(amount_str)
        is_valid, error = AmountParser.validate_amount_format(amount_str)
        
        return {
            'input': amount_str,
            'parsed_amount': str(parsed_amount) if parsed_amount is not None else None,
            'debit_amount': str(debit) if debit is not None else None,
            'credit_amount': str(credit) if credit is not None else None,
            'is_valid': is_valid,
            'error_message': error,
            'transaction_type': 'DEBIT' if parsed_amount and parsed_amount > 0 else 'CREDIT' if parsed_amount and parsed_amount < 0 else 'UNKNOWN',
            'formatted_display': AmountParser.format_amount(parsed_amount) if parsed_amount is not None else None
        }


class EngagementCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating new engagements"""
    
    class Meta:
        model = Engagement
        fields = [
            'engagement_id', 'engagement_name', 'client', 'fiscal_year',
            'fiscal_year_start', 'fiscal_year_end', 'audit_start_date', 'audit_end_date',
            'engagement_type', 'engagement_partner', 'engagement_manager', 
            'audit_team_members', 'required_files', 'completeness_threshold',
            'description', 'notes'
        ]


class ClientCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating new clients"""
    
    class Meta:
        model = Client
        fields = [
            'client_code', 'client_name', 'company_name', 'industry', 'country', 'currency',
            'primary_contact_name', 'primary_contact_email', 'primary_contact_phone',
            'tax_id', 'registration_number', 'fiscal_year_end', 'audit_firm', 'notes'
        ]


# ============================================================================
# UNIFIED ACCOUNT VIEW SERIALIZERS
# ============================================================================

class UnifiedAccountViewSerializer(serializers.Serializer):
    """Serializer for unified account view across TB, GL, and COA"""
    
    account_code = serializers.CharField()
    account_name = serializers.CharField()
    engagement = serializers.CharField()
    client = serializers.CharField()
    
    # Trial Balance data
    opening_balance = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    closing_balance = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    tb_debit = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    tb_credit = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    
    # Chart of Accounts hierarchy
    account_type = serializers.CharField(allow_null=True)
    sub_type = serializers.CharField(allow_null=True)
    sub_sub_type = serializers.CharField(allow_null=True)
    
    # GL Posting statistics
    gl_transaction_count = serializers.IntegerField()
    gl_total_debits = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    gl_total_credits = serializers.DecimalField(max_digits=20, decimal_places=2, allow_null=True)
    
    # Financial statement classification
    financial_statement_category = serializers.CharField(allow_null=True)
    balance_sheet_category = serializers.CharField(allow_null=True)
    income_statement_category = serializers.CharField(allow_null=True)


# ============================================================================
# AI RISK ASSESSMENT SERIALIZERS - REMOVED (models deleted)
# ============================================================================