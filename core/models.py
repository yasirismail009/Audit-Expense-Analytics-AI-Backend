from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from decimal import Decimal
import uuid
from django.utils import timezone
from django.core.cache import cache
from django.db.models import JSONField
import json

# ============================================================================
# BASE CLASSES FOR INHERITANCE
# ============================================================================

class BaseModel(models.Model):
    """Base model with common fields for all models"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        abstract = True


class BaseAnalysisResult(BaseModel):
    """Base model for all analysis results with unified structure"""
    
    # File reference
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='%(class)s_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='2.0.0', help_text='Version of analysis algorithm')
    
    # Unified analysis results structure
    analysis_summary = models.JSONField(default=dict, help_text='Summary statistics and key metrics')
    anomaly_list = models.JSONField(default=list, help_text='Standardized list of anomalies detected')
    chart_data = models.JSONField(default=dict, help_text='Unified chart data for visualizations')
    risk_assessment = models.JSONField(default=dict, help_text='Risk assessment and scoring')
    audit_recommendations = models.JSONField(default=dict, help_text='Audit recommendations and priorities')
    compliance_assessment = models.JSONField(default=dict, help_text='Compliance risk assessment')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey('FileProcessingJob', on_delete=models.SET_NULL, null=True, blank=True, related_name='%(class)s_results', help_text='Reference to the processing job')
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    
    # Analysis status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='COMPLETED')
    error_message = models.TextField(blank=True, null=True, help_text='Error message if analysis failed')
    
    class Meta:
        abstract = True
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['analysis_type', 'status']),
            models.Index(fields=['processing_job', 'status']),
        ]
    
    def __str__(self):
        return f"{self.analysis_type} for {self.data_file.file_name} - {self.analysis_date}"
    
    def get_anomaly_count(self):
        """Get count of anomalies detected"""
        return len(self.anomaly_list) if self.anomaly_list else 0
    
    def get_total_amount(self):
        """Get total amount involved in anomalies"""
        if not self.anomaly_list:
            return Decimal('0.00')
        return sum(Decimal(str(anomaly.get('amount', 0))) for anomaly in self.anomaly_list)
    
    def get_risk_distribution(self):
        """Get risk level distribution"""
        if not self.anomaly_list:
            return {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        
        risk_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for anomaly in self.anomaly_list:
            risk_level = anomaly.get('risk_level', 'low').lower()
            if risk_level in risk_counts:
                risk_counts[risk_level] += 1
        
        return risk_counts
    
    def get_chart_data(self):
        """Get standardized chart data"""
        return self.chart_data or {}
    
    def get_analysis_summary(self):
        """Get analysis summary"""
        return self.analysis_summary or {}
    
    def get_audit_recommendations(self):
        """Get audit recommendations"""
        return self.audit_recommendations or {}
    

class BaseModelTraining(BaseModel):
    """Base model for ML model training records"""
    
    model_name = models.CharField(max_length=100, help_text='Name of the trained model')
    model_type = models.CharField(max_length=50, help_text='Type of model (classification, regression, etc.)')
    model_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of the model')
    
    # Training metadata
    training_data_size = models.IntegerField(help_text='Number of records used for training')
    training_accuracy = models.FloatField(null=True, blank=True, help_text='Training accuracy')
    validation_accuracy = models.FloatField(null=True, blank=True, help_text='Validation accuracy')
    test_accuracy = models.FloatField(null=True, blank=True, help_text='Test accuracy')
    
    # Model parameters
    model_parameters = models.JSONField(default=dict, help_text='Model hyperparameters and configuration')
    feature_importance = models.JSONField(default=list, help_text='Feature importance scores')
    
    # Training status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    error_message = models.TextField(blank=True, null=True, help_text='Error message if training failed')
    
    # Training timing
    training_started_at = models.DateTimeField(null=True, blank=True)
    training_completed_at = models.DateTimeField(null=True, blank=True)
    training_duration = models.FloatField(null=True, blank=True, help_text='Training duration in seconds')
    
    class Meta:
        abstract = True
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['model_type', 'status']),
            models.Index(fields=['training_completed_at']),
        ]
    
    def __str__(self):
        return f"{self.model_name} - {self.model_version} ({self.status})"


# ============================================================================
# CORE DATA MODELS
# ============================================================================

class DataFile(BaseModel):
    """Model to track uploaded data files"""
    
    file_name = models.CharField(max_length=255, help_text='Original file name')
    file_size = models.BigIntegerField(help_text='File size in bytes')
    file_hash = models.CharField(max_length=64, blank=True, help_text='SHA256 hash of file content for duplicate detection')
    
    # Audit context
    engagement_id = models.CharField(max_length=100, help_text='Engagement ID for the audit')
    client_name = models.CharField(max_length=255, help_text='Client name')
    company_name = models.CharField(max_length=255, help_text='Company name')
    fiscal_year = models.IntegerField(help_text='Fiscal year for the audit')
    audit_start_date = models.DateField(help_text='Audit start date')
    audit_end_date = models.DateField(help_text='Audit end date')
    
    # Processing statistics
    total_records = models.IntegerField(default=0, help_text='Total records in file')
    processed_records = models.IntegerField(default=0, help_text='Successfully processed records')
    failed_records = models.IntegerField(default=0, help_text='Failed to process records')
    
    # File processing status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('PARTIAL', 'Partially Processed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Processing metadata
    uploaded_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True, null=True)
    
    # Data range information
    min_date = models.DateField(null=True, blank=True)
    max_date = models.DateField(null=True, blank=True)
    min_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    max_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    
    class Meta:
        db_table = 'data_files'
        ordering = ['-uploaded_at']
        indexes = [
            models.Index(fields=['status', 'uploaded_at']),
            models.Index(fields=['engagement_id', 'fiscal_year']),
        ]
    
    def __str__(self):
        return f"{self.file_name} - {self.client_name} ({self.status})"

    def get_transaction_document_numbers(self):
        """Get all document numbers for transactions in this file"""
        return list(self.sapglposting_set.values_list('document_number', flat=True).distinct())


class GLAccount(BaseModel):
    """Master GL Account table - Links TB, Chart of Accounts, and GL Listing data"""
    
    # Primary account identifier
    account_code = models.CharField(max_length=10, unique=True, db_index=True, help_text='GL Account Code')
    
    # Account details
    account_name = models.CharField(max_length=200, help_text='GL Account Name')
    account_type = models.CharField(max_length=50, blank=True, help_text='Account Type')
    account_subtype = models.CharField(max_length=50, blank=True, help_text='Account Subtype')
    
    # Company and organizational info
    company_code = models.CharField(max_length=10, blank=True, help_text='Company Code')
    profit_center = models.CharField(max_length=10, blank=True, help_text='Profit Center')
    cost_center = models.CharField(max_length=10, blank=True, help_text='Cost Center')
    
    # Financial statement classification
    financial_statement_category = models.CharField(max_length=50, blank=True, help_text='Financial Statement Category')
    balance_sheet_category = models.CharField(max_length=50, blank=True, help_text='Balance Sheet Category')
    income_statement_category = models.CharField(max_length=50, blank=True, help_text='Income Statement Category')
    
    # Account properties
    is_active = models.BooleanField(default=True, help_text='Is Account Active')
    currency = models.CharField(max_length=3, default='SAR', help_text='Account Currency')
    
    # Audit context
    engagement_id = models.CharField(max_length=100, blank=True, help_text='Engagement ID where this account was first seen')
    client_name = models.CharField(max_length=255, blank=True, help_text='Client Name')
    
    class Meta:
        db_table = 'gl_accounts'
        ordering = ['account_code']
        indexes = [
            models.Index(fields=['account_code']),
            models.Index(fields=['account_name']),
            models.Index(fields=['company_code', 'account_code']),
            models.Index(fields=['engagement_id', 'account_code']),
            models.Index(fields=['financial_statement_category']),
            models.Index(fields=['is_active']),
        ]
    
    def __str__(self):
        return f"{self.account_code} - {self.account_name}"
    
    @classmethod
    def get_or_create_account(cls, account_code, account_name=None, **kwargs):
        """
        Get or create a GL Account with the given code
        Updates existing account if new information is provided
        """
        account, created = cls.objects.get_or_create(
            account_code=account_code,
            defaults={
                'account_name': account_name or f'Account {account_code}',
                **kwargs
            }
        )
        
        # Update account if new information is provided and account exists
        if not created and account_name:
            account.account_name = account_name
            for key, value in kwargs.items():
                if hasattr(account, key) and value:
                    setattr(account, key, value)
            account.save()
        
        return account


class SAPGLPostingError(BaseModel):
    """Model to capture failed SAP GL Posting transactions with error details"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, help_text='Reference to the uploaded file')
    
    # Error information
    error_type = models.CharField(max_length=100, help_text='Type of error that occurred')
    error_message = models.TextField(help_text='Detailed error message')
    error_timestamp = models.DateTimeField(auto_now_add=True, help_text='When the error occurred')
    
    # Raw data that failed to process
    raw_data = models.JSONField(help_text='Original data that failed to process')
    
    # Batch information
    batch_number = models.IntegerField(help_text='Batch number where error occurred')
    record_index = models.IntegerField(help_text='Index of record within the batch')
    
    class Meta:
        db_table = 'core_sapglposting_error'
        indexes = [
            models.Index(fields=['data_file', 'error_timestamp']),
            models.Index(fields=['error_type']),
        ]


class SAPGLPosting(BaseModel):
    """SAP GL Posting transaction data - Optimized for 400k+ records"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, help_text='Reference to the uploaded file')
    
    # Document information
    document_number = models.CharField(max_length=20, db_index=True, blank=True, default='', help_text='SAP Document Number')
    document_type = models.CharField(max_length=10, db_index=True, blank=True, default='', help_text='Document Type')
    
    # Financial data
    amount_local_currency = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        null=True,
        blank=True,
        db_index=True,
        help_text='Amount in local currency (flexible precision)'
    )
    local_currency = models.CharField(max_length=3, default='SAR', db_index=True, help_text='Local currency code')
    
    # Account information
    gl_account = models.CharField(max_length=10, db_index=True, help_text='G/L Account (legacy field)')
    gl_account_ref = models.ForeignKey(
        GLAccount, 
        on_delete=models.PROTECT, 
        null=True, 
        blank=True, 
        related_name='gl_postings',
        help_text='Reference to GL Account master record'
    )
    profit_center = models.CharField(max_length=10, blank=True, db_index=True, help_text='Profit Center')
    
    # User information
    user_name = models.CharField(max_length=50, db_index=True, help_text='User Name')
    
    # Date information
    posting_date = models.DateField(db_index=True, help_text='Posting Date')
    document_date = models.DateField(null=True, blank=True, db_index=True, help_text='Document Date')
    entry_date = models.DateField(null=True, blank=True, db_index=True, help_text='Entry Date')
    
    # Period information
    fiscal_year = models.IntegerField(db_index=True, help_text='Fiscal Year')
    posting_period = models.IntegerField(db_index=True, help_text='Posting Period')
    
    # Additional fields
    text = models.CharField(max_length=100, blank=True, help_text='Text')
    segment = models.CharField(max_length=10, blank=True, db_index=True, help_text='Segment')
    clearing_document = models.CharField(max_length=20, blank=True, help_text='Clearing Document')
    offsetting_account = models.CharField(max_length=10, blank=True, help_text='Offsetting Account')
    invoice_reference = models.CharField(max_length=20, blank=True, help_text='Invoice Reference')
    sales_document = models.CharField(max_length=20, blank=True, help_text='Sales Document')
    assignment = models.CharField(max_length=20, blank=True, help_text='Assignment')
    year_month = models.CharField(max_length=7, blank=True, help_text='Year/Month')
    
    class Meta:
        db_table = 'sap_gl_postings'
        ordering = ['-posting_date', 'document_number']
        indexes = [
            # Composite indexes for common queries
            models.Index(fields=['data_file', 'posting_date']),
            models.Index(fields=['gl_account', 'posting_date']),
            models.Index(fields=['user_name', 'posting_date']),
            models.Index(fields=['fiscal_year', 'posting_period']),
            models.Index(fields=['document_type', 'posting_date']),
            models.Index(fields=['local_currency', 'posting_date']),
            models.Index(fields=['profit_center', 'posting_date']),
            models.Index(fields=['segment', 'posting_date']),
            # Range queries for amounts
            models.Index(fields=['amount_local_currency']),
            # Date range queries
            models.Index(fields=['posting_date', 'amount_local_currency']),
            # Text search
            models.Index(fields=['text']),
        ]
    
    @property
    def transaction_type(self):
        """
        Determine transaction type based on amount
        Positive amounts are debits, negative amounts are credits
        """
        if self.amount_local_currency > 0:
            return 'DEBIT'
        else:
            return 'CREDIT'
    
    def __str__(self):
        return f"{self.document_number} - {self.gl_account} - {self.amount_local_currency}"


class TrialBalance(BaseModel):
    """Trial Balance data from TB files"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, help_text='Reference to the uploaded file')
    
    # Account information
    company_code = models.CharField(max_length=10, help_text='Company Code')
    gl_account = models.CharField(max_length=10, help_text='G/L Account (legacy field)')
    gl_account_ref = models.ForeignKey(
        GLAccount, 
        on_delete=models.PROTECT, 
        null=True, 
        blank=True, 
        related_name='trial_balances',
        help_text='Reference to GL Account master record'
    )
    short_text = models.CharField(max_length=100, help_text='Short Text')
    currency = models.CharField(max_length=3, default='SAR', help_text='Currency')
    
    # Balance information
    opening_balance = models.DecimalField(
        max_digits=20, 
        decimal_places=2, 
        help_text='Opening Balance'
    )
    debit = models.DecimalField(
        max_digits=20, 
        decimal_places=2, 
        null=True, 
        blank=True, 
        help_text='Debit Amount (optional)'
    )
    credit = models.DecimalField(
        max_digits=20, 
        decimal_places=2, 
        null=True, 
        blank=True, 
        help_text='Credit Amount (optional)'
    )
    closing_balance = models.DecimalField(
        max_digits=20, 
        decimal_places=2, 
        help_text='Closing Balance'
    )
    
    class Meta:
        db_table = 'trial_balance'
        ordering = ['gl_account']
        indexes = [
            models.Index(fields=['data_file', 'gl_account']),
            models.Index(fields=['company_code', 'gl_account']),
        ]
    
    def __str__(self):
        return f"{self.gl_account} - {self.short_text} - {self.closing_balance}"


class ChartOfAccount(BaseModel):
    """Chart of Accounts data"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, help_text='Reference to the uploaded file')
    
    # Account information (required fields)
    account = models.CharField(max_length=10, help_text='Account Code')
    type = models.CharField(max_length=50, help_text='Type (required)')
    sub_type = models.CharField(max_length=50, help_text='Sub Type (required)')
    sub_sub_type = models.CharField(max_length=50, help_text='Sub Sub Type (required)')
    
    # Additional account details
    gl_account = models.CharField(max_length=20, blank=True, help_text='G/L Account (legacy field)')
    gl_account_ref = models.ForeignKey(
        GLAccount, 
        on_delete=models.PROTECT, 
        null=True, 
        blank=True, 
        related_name='chart_of_accounts',
        help_text='Reference to GL Account master record'
    )
    company = models.CharField(max_length=10, blank=True, help_text='Company')
    branch = models.CharField(max_length=50, blank=True, help_text='Branch')
    cost_center = models.CharField(max_length=10, blank=True, help_text='Cost Center')
    code = models.CharField(max_length=10, blank=True, help_text='Code')
    gl_account_long_text = models.CharField(max_length=200, blank=True, help_text='G/L Account Long Text')
    
    # Financial statement classification
    ref_to_fs = models.CharField(max_length=10, blank=True, help_text='Reference to Financial Statement')
    financial_statement = models.CharField(max_length=50, blank=True, help_text='Financial Statement')
    ref_to_note = models.CharField(max_length=20, blank=True, help_text='Reference to Note')
    
    # Period data
    fiscal_year = models.IntegerField(null=True, blank=True, help_text='Fiscal Year')
    q1_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True, help_text='Q1 Amount')
    adj_reclas = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True, help_text='Adjustments/Reclassifications')
    fin_q1_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True, help_text='Final Q1 Amount')
    
    class Meta:
        db_table = 'chart_of_accounts'
        ordering = ['account']
        indexes = [
            models.Index(fields=['data_file', 'account']),
            models.Index(fields=['type', 'sub_type', 'sub_sub_type']),
            models.Index(fields=['account']),
        ]
    
    def __str__(self):
        return f"{self.account} - {self.type} - {self.sub_type}"


class FileProcessingJob(BaseModel):
    """Model to track file processing jobs"""
    
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='processing_jobs', help_text='Reference to the data file')
    file_hash = models.CharField(max_length=64, help_text='SHA256 hash of file content for duplicate detection')
    
    # Processing configuration
    run_anomalies = models.BooleanField(default=False, help_text='Whether to run anomaly detection')
    requested_anomalies = models.JSONField(default=list, help_text='List of specific anomalies to detect')
    
    # Job type to distinguish between GL processing and analysis
    JOB_TYPE_CHOICES = [
        ('GL_PROCESSING', 'GL Data Processing'),
        ('ANALYSIS', 'Analysis'),
        ('COMPLETENESS', 'Completeness Test'),
    ]
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default='GL_PROCESSING', help_text='Type of processing job')
    
    # Processing status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('QUEUED', 'Queued'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Processing timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    
    # Error handling
    error_message = models.TextField(blank=True, null=True, help_text='Error message if processing failed')
    retry_count = models.IntegerField(default=0, help_text='Number of retry attempts')
    
    class Meta:
        db_table = 'file_processing_jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status', 'created_at']),
            models.Index(fields=['data_file', 'status']),
            models.Index(fields=['file_hash']),
        ]
    
    def __str__(self):
        return f"Job for {self.data_file.file_name} - {self.status}"


class FileProcessingTask(BaseModel):
    """Model for tracking individual file processing tasks (GL, TB, Chart)"""
    
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='processing_tasks')
    task_type = models.CharField(max_length=20, choices=[
        ('GL_LIST', 'General Ledger List'),
        ('TB_LIST', 'Trial Balance List'),
        ('CHART_OF_ACCOUNTS', 'Chart of Accounts')
    ])
    
    # Task status and timing
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('SUCCESS', 'Success'),
        ('FAILED', 'Failed')
    ], default='PENDING')
    
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Celery task tracking
    celery_task_id = models.CharField(max_length=255, blank=True, help_text='Celery task ID for tracking')
    
    # Processing results
    extracted_data = models.JSONField(default=dict, help_text='Extracted data from the task')
    processing_metadata = models.JSONField(default=dict, help_text='Processing metadata and statistics')
    error_message = models.TextField(blank=True, help_text='Error message if task failed')
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['task_type', 'status']),
            models.Index(fields=['data_file', 'task_type']),
            models.Index(fields=['status']),
        ]
        unique_together = ['data_file', 'task_type']
    
    def __str__(self):
        return f"{self.task_type} Task {self.id} - {self.data_file.file_name} ({self.status})"


class CompletenessJob(BaseModel):
    """Model for tracking completeness jobs that run after all processing tasks complete"""
    
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='completeness_jobs')
    
    # Related processing tasks
    gl_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='gl_completeness_jobs', null=True, blank=True)
    tb_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='tb_completeness_jobs', null=True, blank=True)
    chart_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='chart_completeness_jobs', null=True, blank=True)
    
    # Job status and timing
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('CREATED', 'Created'),
        ('FAILED', 'Failed')
    ], default='PENDING')
    
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Celery task tracking
    celery_task_id = models.CharField(max_length=255, blank=True, help_text='Celery task ID for tracking')
    
    # Completeness results
    completeness_results = models.JSONField(default=dict, help_text='Completeness analysis results')
    debit_credit_summary = models.JSONField(default=dict, help_text='Debit and credit summary')
    audit_checks = models.JSONField(default=dict, help_text='Audit-style completeness checks')
    error_message = models.TextField(blank=True, help_text='Error message if job failed')
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['data_file', 'status']),
        ]
    
    def __str__(self):
        return f"Completeness Job {self.id} - {self.data_file.file_name} ({self.status})"


# ============================================================================
# ANALYSIS RESULT MODELS (Inheriting from BaseAnalysisResult)
# ============================================================================

class GeneralAnalysisResult(BaseAnalysisResult):
    """General analysis results"""
    
    class Meta:
        db_table = 'general_analysis_results'
    
    def get_duplicate_count(self):
        """Get count of duplicate transactions"""
        return len([a for a in self.anomaly_list if a.get('type') == 'duplicate'])


class DuplicateAnalysisResult(BaseAnalysisResult):
    """Duplicate analysis results"""
    
    class Meta:
        db_table = 'duplicate_analysis_results'
    
    def get_duplicate_count(self):
        """Get count of duplicate transactions"""
        return len([a for a in self.anomaly_list if a.get('type') == 'duplicate'])


class BackdatedAnalysisResult(BaseAnalysisResult):
    """Backdated analysis results"""
    
    class Meta:
        db_table = 'backdated_analysis_results'
    
    def get_backdated_count(self):
        """Get count of backdated transactions"""
        return len([a for a in self.anomaly_list if a.get('type') == 'backdated'])


class UserAnalysisResult(BaseAnalysisResult):
    """User analysis results"""
    
    class Meta:
        db_table = 'user_analysis_results'


class UnusualDaysAnalysisResult(BaseAnalysisResult):
    """Unusual days analysis results"""
    
    class Meta:
        db_table = 'unusual_days_analysis_results'


class ClosingEntriesAnalysisResult(BaseAnalysisResult):
    """Closing entries analysis results"""
    
    class Meta:
        db_table = 'closing_entries_analysis_results'


class HolidayAnalysisResult(BaseAnalysisResult):
    """Holiday analysis results"""
    
    class Meta:
        db_table = 'holiday_analysis_results'


class OverallAnalysisResult(BaseAnalysisResult):
    """Overall analysis results"""
    
    risk_score = models.FloatField(null=True, blank=True, help_text='Overall risk score')
    confidence_score = models.FloatField(null=True, blank=True, help_text='Confidence score')
    
    class Meta:
        db_table = 'overall_analysis_results'


class RiskScoringDocument(BaseAnalysisResult):
    """Risk scoring document results"""
    
    overall_risk_score = models.FloatField(null=True, blank=True, help_text='Overall risk score')
    risk_level = models.CharField(max_length=20, blank=True, help_text='Risk level (Low, Medium, High, Critical)')
    risk_summary = models.JSONField(default=dict, help_text='Risk summary and breakdown')
    recommendations = models.JSONField(default=list, help_text='Risk mitigation recommendations')
    
    class Meta:
        db_table = 'risk_scoring_documents'


class ManualEntryAnalysisResult(BaseAnalysisResult):
    """Manual entry analysis results"""
    
    class Meta:
        db_table = 'manual_entry_analysis_results'
    

# ============================================================================
# ML MODEL TRAINING MODELS (Inheriting from BaseModelTraining)
# ============================================================================

class MLModelTraining(BaseModelTraining):
    """General ML model training record"""
    
    class Meta:
        db_table = 'ml_model_training'


class RuleBasedModelTraining(BaseModelTraining):
    """Rule-based model training record"""
    
    class Meta:
        db_table = 'rule_based_model_training'


class DuplicateAnalysisModelTraining(BaseModelTraining):
    """Duplicate analysis model training record"""
    
    class Meta:
        db_table = 'duplicate_analysis_model_training'


class BackdatedAnalysisModelTraining(BaseModelTraining):
    """Backdated analysis model training record"""
    
    class Meta:
        db_table = 'backdated_analysis_model_training'


class UserAnalysisModelTraining(BaseModelTraining):
    """User analysis model training record"""
    
    class Meta:
        db_table = 'user_analysis_model_training'


class UnusualDaysAnalysisModelTraining(BaseModelTraining):
    """Unusual days analysis model training record"""
    
    class Meta:
        db_table = 'unusual_days_analysis_model_training'


class ClosingEntriesAnalysisModelTraining(BaseModelTraining):
    """Closing entries analysis model training record"""
    
    class Meta:
        db_table = 'closing_entries_analysis_model_training'


class HolidayAnalysisModelTraining(BaseModelTraining):
    """Holiday analysis model training record"""
    
    class Meta:
        db_table = 'holiday_analysis_model_training'


class OverallRiskAnalysisModelTraining(BaseModelTraining):
    """Overall risk analysis model training record"""
    
    class Meta:
        db_table = 'overall_risk_analysis_model_training'


class AICompletenessModel(BaseModel):
    """
    AI/ML Model for Completeness Test Prediction and Optimization
    
    This model stores trained ML models that can:
    1. Predict completeness issues before full processing
    2. Optimize completeness test execution
    3. Learn from historical completeness results
    4. Provide client-specific predictions
    """
    
    # Model identification
    model_name = models.CharField(max_length=100, help_text='Name of the AI model')
    model_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of the model')
    client_name = models.CharField(max_length=255, blank=True, help_text='Client-specific model (empty for general model)')
    
    # Model type and purpose
    MODEL_TYPE_CHOICES = [
        ('COMPLETENESS_PREDICTOR', 'Completeness Pass/Fail Predictor'),
        ('SCORE_ESTIMATOR', 'Completeness Score Estimator'),
        ('STEP_PREDICTOR', 'Individual Step Success Predictor'),
        ('CLIENT_PATTERN_LEARNER', 'Client-Specific Completeness Pattern Learning'),
    ]
    model_type = models.CharField(max_length=30, choices=MODEL_TYPE_CHOICES, help_text='Type of AI model')
    
    # Training data and performance
    training_data_size = models.IntegerField(help_text='Number of completeness tests used for training')
    training_accuracy = models.FloatField(null=True, blank=True, help_text='Model training accuracy (0-100%)')
    validation_accuracy = models.FloatField(null=True, blank=True, help_text='Model validation accuracy (0-100%)')
    test_accuracy = models.FloatField(null=True, blank=True, help_text='Model test accuracy (0-100%)')
    
    # Model features and configuration
    feature_set = models.JSONField(default=list, help_text='List of features used for training')
    model_parameters = models.JSONField(default=dict, help_text='Model hyperparameters and configuration')
    feature_importance = models.JSONField(default=dict, help_text='Feature importance scores')
    
    # Training status and metadata
    STATUS_CHOICES = [
        ('TRAINING', 'Training in Progress'),
        ('TRAINED', 'Training Completed'),
        ('DEPLOYED', 'Deployed for Predictions'),
        ('FAILED', 'Training Failed'),
        ('DEPRECATED', 'Model Deprecated'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='TRAINING')
    
    # Training timing
    training_started_at = models.DateTimeField(null=True, blank=True)
    training_completed_at = models.DateTimeField(null=True, blank=True)
    training_duration = models.FloatField(null=True, blank=True, help_text='Training duration in seconds')
    
    # Prediction statistics
    predictions_made = models.IntegerField(default=0, help_text='Total predictions made by this model')
    correct_predictions = models.IntegerField(default=0, help_text='Number of correct predictions')
    prediction_accuracy = models.FloatField(null=True, blank=True, help_text='Real-world prediction accuracy')
    
    # Model file storage
    model_file_path = models.CharField(max_length=500, blank=True, help_text='Path to saved model file')
    scaler_file_path = models.CharField(max_length=500, blank=True, help_text='Path to feature scaler file')
    
    # Client learning data
    client_patterns = models.JSONField(default=dict, help_text='Learned patterns specific to client')
    optimization_insights = models.JSONField(default=dict, help_text='Process optimization insights')
    
    class Meta:
        db_table = 'ai_completeness_models'
        ordering = ['-training_completed_at']
        indexes = [
            models.Index(fields=['model_type', 'status']),
            models.Index(fields=['client_name', 'model_type']),
            models.Index(fields=['status', 'prediction_accuracy']),
            models.Index(fields=['training_completed_at']),
        ]
        unique_together = ['model_name', 'model_version', 'client_name']
    
    def __str__(self):
        client_suffix = f" ({self.client_name})" if self.client_name else " (General)"
        return f"{self.model_name} v{self.model_version}{client_suffix} - {self.status}"
    
    def get_prediction_accuracy_percentage(self):
        """Get prediction accuracy as percentage"""
        if self.predictions_made > 0:
            return round((self.correct_predictions / self.predictions_made) * 100, 2)
        return 0.0
    
    def is_ready_for_prediction(self):
        """Check if model is ready for making predictions"""
        return self.status == 'DEPLOYED' and self.model_file_path and self.validation_accuracy is not None
    
    def update_prediction_stats(self, was_correct: bool):
        """Update prediction statistics"""
        self.predictions_made += 1
        if was_correct:
            self.correct_predictions += 1
        self.prediction_accuracy = self.get_prediction_accuracy_percentage()
        self.save()


class CompletenessAIPrediction(BaseModel):
    """
    Store AI predictions for completeness tests
    
    This model stores predictions made by AI models before actual processing,
    allowing us to compare predicted vs actual results for model improvement.
    """
    
    # Associated data and model
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='ai_predictions')
    ai_model = models.ForeignKey(AICompletenessModel, on_delete=models.CASCADE, related_name='predictions')
    
    # Prediction metadata
    prediction_timestamp = models.DateTimeField(auto_now_add=True)
    prediction_confidence = models.FloatField(help_text='Confidence score of the prediction (0-100%)')
    
    # Predicted results
    predicted_completeness_score = models.FloatField(help_text='Predicted completeness score (0-100%)')
    predicted_status = models.CharField(max_length=20, help_text='Predicted overall status (PASS/FAIL)')
    predicted_step_results = models.JSONField(default=dict, help_text='Predicted results for each completeness step')
    predicted_processing_time = models.FloatField(null=True, blank=True, help_text='Predicted processing time in seconds')
    
    # Input features used for prediction
    input_features = models.JSONField(default=dict, help_text='Features extracted from file for prediction')
    
    # Actual results (filled after real completeness test)
    actual_completeness_score = models.FloatField(null=True, blank=True, help_text='Actual completeness score')
    actual_status = models.CharField(max_length=20, blank=True, help_text='Actual overall status')
    actual_processing_time = models.FloatField(null=True, blank=True, help_text='Actual processing time')
    
    # Prediction accuracy assessment
    score_accuracy = models.FloatField(null=True, blank=True, help_text='Accuracy of score prediction (0-100%)')
    status_correct = models.BooleanField(null=True, blank=True, help_text='Whether status prediction was correct')
    step_accuracy = models.FloatField(null=True, blank=True, help_text='Accuracy of step predictions (0-100%)')
    
    class Meta:
        db_table = 'completeness_ai_predictions'
        ordering = ['-prediction_timestamp']
        indexes = [
            models.Index(fields=['data_file', 'prediction_timestamp']),
            models.Index(fields=['ai_model', 'status_correct']),
            models.Index(fields=['prediction_confidence', 'score_accuracy']),
        ]
    
    def __str__(self):
        return f"AI Prediction for {self.data_file.file_name} - {self.predicted_status} ({self.predicted_completeness_score:.1f}%)"
    
    def calculate_prediction_accuracy(self):
        """Calculate and update prediction accuracy after actual results are available"""
        if self.actual_completeness_score is not None:
            # Score accuracy (how close the predicted score was)
            score_diff = abs(self.predicted_completeness_score - self.actual_completeness_score)
            self.score_accuracy = max(0, 100 - score_diff)
            
            # Status accuracy
            self.status_correct = (self.predicted_status == self.actual_status)
            
            self.save()
            
            # Update the AI model's overall prediction statistics
            self.ai_model.update_prediction_stats(self.status_correct and self.score_accuracy >= 80)
            
            return {
                'score_accuracy': self.score_accuracy,
                'status_correct': self.status_correct,
                'overall_accurate': self.status_correct and self.score_accuracy >= 80
            }


class CompletenessTestResult(BaseModel):
    """
    Model to store GL-TB completeness test results
    
    Stores comprehensive completeness validation results including:
    - GL-TB reconciliation analysis
    - Debit-credit balance verification  
    - Account coverage analysis
    - Transaction gap detection
    - Overall completeness scoring
    """
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='completeness_tests', help_text='Reference to the GL data file')
    
    # Test metadata
    test_timestamp = models.DateTimeField(auto_now_add=True, help_text='When the completeness test was performed')
    engagement_id = models.CharField(max_length=100, help_text='Engagement ID for cross-referencing TB data')
    
    # Overall results
    COMPLETENESS_STATUS_CHOICES = [
        ('PASS', 'All tests passed'),
        ('FAIL', 'One or more tests failed'),
        ('TB_NOT_AVAILABLE', 'Trial Balance data not available'),
        ('ERROR', 'Error during testing'),
    ]
    overall_status = models.CharField(max_length=20, choices=COMPLETENESS_STATUS_CHOICES, help_text='Overall test result')
    overall_explanation = models.TextField(help_text='Detailed explanation of the test result')
    completeness_score = models.FloatField(help_text='Overall completeness score (0-100%)')
    
    # Test step results
    step1_gl_tb_reconciliation = models.JSONField(default=dict, help_text='GL-TB reconciliation test results')
    step2_debit_credit_balance = models.JSONField(default=dict, help_text='Debit-credit balance verification results')
    step3_account_coverage = models.JSONField(default=dict, help_text='Account coverage analysis results')
    step4_transaction_gaps = models.JSONField(default=dict, help_text='Transaction gap detection results')
    
    # Summary statistics
    total_gl_records = models.IntegerField(help_text='Total GL posting records analyzed')
    total_tb_records = models.IntegerField(null=True, blank=True, help_text='Total TB records (if available)')
    tests_passed = models.IntegerField(help_text='Number of tests passed')
    total_tests = models.IntegerField(help_text='Total number of tests performed')
    
    # Critical issues count (for completeness issues only)
    critical_issues_count = models.IntegerField(default=0, help_text='Number of critical completeness issues found')
    
    # Comprehensive statistics
    comprehensive_statistics = models.JSONField(default=dict, help_text='Comprehensive GL statistics including document stats, subtype analysis, user analysis, and monthly trends')
    
    # Processing metadata
    processing_duration = models.FloatField(help_text='Test processing duration in seconds')
    
    class Meta:
        db_table = 'completeness_test_results'
        ordering = ['-test_timestamp']
        indexes = [
            models.Index(fields=['data_file', 'test_timestamp']),
            models.Index(fields=['engagement_id', 'overall_status']),
            models.Index(fields=['overall_status', 'completeness_score']),
            models.Index(fields=['test_timestamp']),
        ]
    
    def __str__(self):
        return f"Completeness Test - {self.data_file.file_name} - {self.overall_status} ({self.completeness_score:.1f}%)"
    
    def get_summary(self):
        """Get a summary of the completeness test results"""
        return {
            'status': self.overall_status,
            'score': self.completeness_score,
            'tests_passed': f"{self.tests_passed}/{self.total_tests}",
            'critical_issues': self.critical_issues_count,
            'explanation': self.overall_explanation
        }
    
    def has_critical_issues(self):
        """Check if the test found critical issues"""
        return self.critical_issues_count > 0 or self.overall_status == 'FAIL'
    
    def get_failed_tests(self):
        """Get list of failed test steps"""
        failed_tests = []
        
        if not self.step1_gl_tb_reconciliation.get('passed', True):
            failed_tests.append('GL-TB Reconciliation')
        if not self.step2_debit_credit_balance.get('passed', True):
            failed_tests.append('Debit-Credit Balance')
        if not self.step3_account_coverage.get('passed', True):
            failed_tests.append('Account Coverage')
        if not self.step4_transaction_gaps.get('passed', True):
            failed_tests.append('Transaction Gaps')
            
        return failed_tests
