from rest_framework import serializers
from .models import SAPGLPosting, DataFile, AnalysisSession, TransactionAnalysis, SystemMetrics, GLAccount, FileProcessingJob, MLModelTraining
from decimal import Decimal
import uuid

class GLAccountSerializer(serializers.ModelSerializer):
    """Serializer for GL Account data"""
    
    current_balance = serializers.ReadOnlyField()
    total_debits = serializers.SerializerMethodField()
    total_credits = serializers.SerializerMethodField()
    transaction_count = serializers.SerializerMethodField()
    
    class Meta:
        model = GLAccount
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def get_total_debits(self, obj):
        """Calculate total debits for this account"""
        from django.db.models import Sum
        return obj.postings.filter(transaction_type='DEBIT').aggregate(
            total=Sum('amount_local_currency')
        )['total'] or Decimal('0.00')
    
    def get_total_credits(self, obj):
        """Calculate total credits for this account"""
        from django.db.models import Sum
        return obj.postings.filter(transaction_type='CREDIT').aggregate(
            total=Sum('amount_local_currency')
        )['total'] or Decimal('0.00')
    
    def get_transaction_count(self, obj):
        """Get total transaction count for this account"""
        return obj.postings.count()

class GLAccountListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing GL Accounts"""
    
    current_balance = serializers.ReadOnlyField()
    transaction_count = serializers.SerializerMethodField()
    
    class Meta:
        model = GLAccount
        fields = [
            'id', 'account_id', 'account_name', 'account_type', 
            'account_category', 'normal_balance', 'is_active',
            'current_balance', 'transaction_count'
        ]
    
    def get_transaction_count(self, obj):
        """Get total transaction count for this account"""
        return obj.postings.count()

class SAPGLPostingSerializer(serializers.ModelSerializer):
    """Serializer for SAP GL Posting data"""
    
    # Computed fields
    is_high_value = serializers.ReadOnlyField()
    is_cleared = serializers.ReadOnlyField()
    has_arabic_text = serializers.ReadOnlyField()
    
    # GL Account details
    gl_account_details = GLAccountSerializer(source='gl_account_ref', read_only=True)
    
    class Meta:
        model = SAPGLPosting
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

class SAPGLPostingListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing SAP GL Postings"""
    
    is_high_value = serializers.ReadOnlyField()
    is_cleared = serializers.ReadOnlyField()
    gl_account_name = serializers.CharField(source='gl_account_ref.account_name', read_only=True)
    
    class Meta:
        model = SAPGLPosting
        fields = [
            'id', 'document_number', 'document_type', 'posting_date', 
            'amount_local_currency', 'local_currency', 'transaction_type',
            'gl_account', 'gl_account_name', 'profit_center', 'user_name', 
            'fiscal_year', 'posting_period', 'is_high_value', 'is_cleared', 
            'created_at'
        ]

class DataFileSerializer(serializers.ModelSerializer):
    """Serializer for uploaded data files"""
    
    class Meta:
        model = DataFile
        fields = [
            'id', 'file_name', 'file_size', 'engagement_id', 'client_name', 
            'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date',
            'total_records', 'processed_records', 'failed_records', 'status',
            'uploaded_at', 'processed_at', 'error_message', 'min_date', 
            'max_date', 'min_amount', 'max_amount'
        ]
        read_only_fields = ['id', 'uploaded_at', 'processed_at']

class DataFileUploadSerializer(serializers.Serializer):
    """Serializer for file upload requests"""
    
    file = serializers.FileField(help_text='CSV file containing SAP GL posting data')
    description = serializers.CharField(max_length=500, required=False, help_text='Optional description of the file')
    
    # New fields for enhanced file upload flow
    engagement_id = serializers.CharField(max_length=100, help_text='Engagement ID for the audit')
    client_name = serializers.CharField(max_length=255, help_text='Client name')
    company_name = serializers.CharField(max_length=255, help_text='Company name')
    fiscal_year = serializers.IntegerField(help_text='Fiscal year for the audit')
    audit_start_date = serializers.DateField(help_text='Audit start date')
    audit_end_date = serializers.DateField(help_text='Audit end date')

class AnalysisSessionSerializer(serializers.ModelSerializer):
    """Serializer for analysis sessions"""
    
    # Computed fields
    flag_rate = serializers.SerializerMethodField()
    duration = serializers.SerializerMethodField()
    
    class Meta:
        model = AnalysisSession
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'started_at', 'completed_at']
    
    def get_flag_rate(self, obj):
        """Calculate flag rate percentage"""
        if obj.total_transactions > 0:
            return round((obj.flagged_transactions / obj.total_transactions) * 100, 2)
        return 0.0
    
    def get_duration(self, obj):
        """Calculate analysis duration in seconds"""
        if obj.started_at and obj.completed_at:
            return (obj.completed_at - obj.started_at).total_seconds()
        return None

class AnalysisSessionCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating analysis sessions"""
    
    class Meta:
        model = AnalysisSession
        fields = [
            'session_name', 'description', 'date_from', 'date_to',
            'min_amount', 'max_amount', 'document_types', 'gl_accounts',
            'profit_centers', 'users'
        ]

class TransactionAnalysisSerializer(serializers.ModelSerializer):
    """Serializer for transaction analysis results"""
    
    # Include transaction details
    transaction = SAPGLPostingListSerializer(read_only=True)
    
    class Meta:
        model = TransactionAnalysis
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

class TransactionAnalysisListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing transaction analyses"""
    
    document_number = serializers.CharField(source='transaction.document_number', read_only=True)
    amount = serializers.DecimalField(source='transaction.amount_local_currency', max_digits=20, decimal_places=2, read_only=True)
    currency = serializers.CharField(source='transaction.local_currency', read_only=True)
    user_name = serializers.CharField(source='transaction.user_name', read_only=True)
    posting_date = serializers.DateField(source='transaction.posting_date', read_only=True)
    
    class Meta:
        model = TransactionAnalysis
        fields = [
            'id', 'document_number', 'amount', 'currency', 'user_name', 
            'posting_date', 'risk_score', 'risk_level', 'amount_anomaly',
            'timing_anomaly', 'user_anomaly', 'account_anomaly', 'pattern_anomaly',
            'created_at'
        ]

class SystemMetricsSerializer(serializers.ModelSerializer):
    """Serializer for system metrics"""
    
    class Meta:
        model = SystemMetrics
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

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

class GLAccountAnalysisSerializer(serializers.Serializer):
    """Serializer for GL Account analysis results"""
    
    account_id = serializers.CharField()
    account_name = serializers.CharField()
    account_type = serializers.CharField()
    account_category = serializers.CharField()
    normal_balance = serializers.CharField()
    
    # Balance information
    current_balance = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_debits = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_credits = serializers.DecimalField(max_digits=20, decimal_places=2)
    
    # Transaction statistics
    transaction_count = serializers.IntegerField()
    debit_count = serializers.IntegerField()
    credit_count = serializers.IntegerField()
    
    # Risk analysis
    high_value_transactions = serializers.IntegerField()
    flagged_transactions = serializers.IntegerField()
    risk_score = serializers.FloatField()
    
    # Activity analysis
    first_transaction_date = serializers.DateField(allow_null=True)
    last_transaction_date = serializers.DateField(allow_null=True)
    avg_transaction_amount = serializers.DecimalField(max_digits=20, decimal_places=2)
    max_transaction_amount = serializers.DecimalField(max_digits=20, decimal_places=2)

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

class GLAccountChartSerializer(serializers.Serializer):
    """Serializer for GL Account charts data"""
    
    # Account distribution by type
    account_type_distribution = serializers.ListField()
    
    # Account distribution by category
    account_category_distribution = serializers.ListField()
    
    # Top accounts by balance
    top_accounts_by_balance = serializers.ListField()
    
    # Top accounts by transaction count
    top_accounts_by_transactions = serializers.ListField()
    
    # Debit vs Credit analysis
    debit_credit_analysis = serializers.DictField()
    
    # Monthly activity by account type
    monthly_activity = serializers.ListField()
    
    # Risk distribution by account type
    risk_distribution = serializers.ListField()

class GLAccountUploadSerializer(serializers.Serializer):
    """Serializer for GL Account master data upload"""
    
    file = serializers.FileField(help_text='CSV file containing GL Account master data')
    description = serializers.CharField(max_length=500, required=False, help_text='Optional description of the file') 

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