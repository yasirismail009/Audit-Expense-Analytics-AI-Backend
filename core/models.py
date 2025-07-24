from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from decimal import Decimal
import uuid
from django.utils import timezone

class GLAccount(models.Model):
    """Model to track GL Account details and categorization"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    account_id = models.CharField(max_length=20, unique=True, db_index=True, help_text='GL Account ID')
    account_name = models.CharField(max_length=255, help_text='GL Account Name')
    account_type = models.CharField(max_length=50, help_text='Account Type (Asset, Liability, Equity, Revenue, Expense)')
    account_category = models.CharField(max_length=100, help_text='Account Category (e.g., Cash, Accounts Receivable, etc.)')
    account_subcategory = models.CharField(max_length=100, blank=True, null=True, help_text='Account Subcategory')
    normal_balance = models.CharField(max_length=10, choices=[('DEBIT', 'Debit'), ('CREDIT', 'Credit')], help_text='Normal balance side')
    is_active = models.BooleanField(default=True, help_text='Whether the account is active')
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'gl_accounts'
        ordering = ['account_id']
    
    def __str__(self):
        return f"{self.account_id} - {self.account_name}"
    
    @property
    def current_balance(self):
        """Calculate current balance for this account"""
        from django.db.models import Sum
        postings = SAPGLPosting.objects.filter(gl_account=self.account_id)
        debit_total = postings.filter(transaction_type='DEBIT').aggregate(total=Sum('amount_local_currency'))['total'] or Decimal('0.00')
        credit_total = postings.filter(transaction_type='CREDIT').aggregate(total=Sum('amount_local_currency'))['total'] or Decimal('0.00')
        
        if self.normal_balance == 'DEBIT':
            return debit_total - credit_total
        else:
            return credit_total - debit_total

class SAPGLPosting(models.Model):
    """Main model for SAP General Ledger posting data"""
    
    # Unique identifier
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Document information (REQUIRED)
    document_number = models.CharField(max_length=20, db_index=True, help_text='SAP Document Number')
    posting_date = models.DateField(help_text='Posting Date')
    gl_account = models.CharField(max_length=20, db_index=True, help_text='G/L Account Number')
    gl_account_ref = models.ForeignKey(GLAccount, on_delete=models.SET_NULL, null=True, blank=True, related_name='postings', help_text='Reference to GL Account details')
    
    # Amount and transaction type
    amount_local_currency = models.DecimalField(
        max_digits=20, 
        decimal_places=2, 
        help_text='Amount in Local Currency'
    )
    transaction_type = models.CharField(
        max_length=10, 
        choices=[('DEBIT', 'Debit'), ('CREDIT', 'Credit')], 
        default='DEBIT',
        help_text='Transaction type (Debit or Credit)'
    )
    local_currency = models.CharField(max_length=10, default='SAR', help_text='Local Currency Code')
    text = models.TextField(blank=True, null=True, help_text='Transaction Text')
    document_date = models.DateField(null=True, blank=True, help_text='Original Document Date')
    offsetting_account = models.CharField(max_length=20, blank=True, null=True, help_text='Offsetting Account')
    user_name = models.CharField(max_length=50, db_index=True, help_text='User Name')
    entry_date = models.DateField(null=True, blank=True, help_text='Entry Date')
    
    # Optional fields
    document_type = models.CharField(max_length=10, db_index=True, blank=True, null=True, help_text='Document Type (DZ, SA, TR, AB, etc.)')
    profit_center = models.CharField(max_length=20, db_index=True, blank=True, null=True, help_text='Profit Center Code')
    cost_center = models.CharField(max_length=20, blank=True, null=True, help_text='Cost Center Code')
    clearing_document = models.CharField(max_length=20, blank=True, null=True, help_text='Clearing Document Number')
    
    # Organizational information
    segment = models.CharField(max_length=20, blank=True, null=True, help_text='Segment Code')
    wbs_element = models.CharField(max_length=20, blank=True, null=True, help_text='WBS Element')
    plant = models.CharField(max_length=20, blank=True, null=True, help_text='Plant Code')
    material = models.CharField(max_length=20, blank=True, null=True, help_text='Material Number')
    
    # Reference information
    invoice_reference = models.CharField(max_length=20, blank=True, null=True, help_text='Invoice Reference')
    billing_document = models.CharField(max_length=20, blank=True, null=True, help_text='Billing Document')
    sales_document = models.CharField(max_length=20, blank=True, null=True, help_text='Sales Document')
    purchasing_document = models.CharField(max_length=20, blank=True, null=True, help_text='Purchasing Document')
    order_number = models.CharField(max_length=20, blank=True, null=True, help_text='Order Number')
    asset_number = models.CharField(max_length=20, blank=True, null=True, help_text='Asset Number')
    network = models.CharField(max_length=20, blank=True, null=True, help_text='Network Number')
    
    # Additional fields
    assignment = models.CharField(max_length=20, blank=True, null=True, help_text='Assignment Field')
    tax_code = models.CharField(max_length=10, blank=True, null=True, help_text='Tax Code')
    account_assignment = models.CharField(max_length=20, blank=True, null=True, help_text='Account Assignment')
    
    # Period information
    fiscal_year = models.IntegerField(help_text='Fiscal Year')
    posting_period = models.IntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(16)], 
        help_text='Posting Period (1-16)'
    )
    year_month = models.CharField(max_length=10, blank=True, null=True, help_text='Year/Month (YYYY/MM)')
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'sap_gl_postings'
        indexes = [
            models.Index(fields=['document_number', 'fiscal_year']),
            models.Index(fields=['gl_account', 'posting_date']),
            models.Index(fields=['user_name', 'posting_date']),
            models.Index(fields=['profit_center', 'fiscal_year']),
            models.Index(fields=['amount_local_currency', 'posting_date']),
            models.Index(fields=['transaction_type', 'gl_account']),
        ]
        ordering = ['-posting_date', '-created_at']
    
    def __str__(self):
        return f"{self.document_number} - {self.amount_local_currency} {self.local_currency} ({self.transaction_type})"
    
    @property
    def is_high_value(self):
        """Check if this is a high-value transaction (> 5M SAR)"""
        return self.amount_local_currency > 5000000
    
    @property
    def is_cleared(self):
        """Check if transaction is cleared"""
        return bool(self.clearing_document)
    
    @property
    def has_arabic_text(self):
        """Check if text contains Arabic characters"""
        if not self.text:
            return False
        arabic_range = range(0x0600, 0x06FF)
        return any(ord(char) in arabic_range for char in str(self.text))
    
    def save(self, *args, **kwargs):
        """Override save to automatically link to GL Account"""
        if self.gl_account and not self.gl_account_ref:
            try:
                self.gl_account_ref = GLAccount.objects.get(account_id=self.gl_account)
            except GLAccount.DoesNotExist:
                pass
        super().save(*args, **kwargs)

class DataFile(models.Model):
    """Model to track uploaded data files"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    file_name = models.CharField(max_length=255, help_text='Original file name')
    file_size = models.BigIntegerField(help_text='File size in bytes')
    
    # New fields for enhanced file upload flow
    engagement_id = models.CharField(max_length=100, help_text='Engagement ID for the audit')
    client_name = models.CharField(max_length=255, help_text='Client name')
    company_name = models.CharField(max_length=255, help_text='Company name')
    fiscal_year = models.IntegerField(help_text='Fiscal year for the audit')
    audit_start_date = models.DateField(help_text='Audit start date')
    audit_end_date = models.DateField(help_text='Audit end date')
    
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
    
    def __str__(self):
        return f"{self.file_name} - {self.client_name} ({self.engagement_id})"

class AnalysisSession(models.Model):
    """Model to track analysis sessions"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    session_name = models.CharField(max_length=255, help_text='Analysis session name')
    description = models.TextField(blank=True, null=True, help_text='Session description')
    
    # Analysis parameters
    date_from = models.DateField(null=True, blank=True)
    date_to = models.DateField(null=True, blank=True)
    min_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    max_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    document_types = models.JSONField(default=list, help_text='Filter by document types')
    gl_accounts = models.JSONField(default=list, help_text='Filter by G/L accounts')
    profit_centers = models.JSONField(default=list, help_text='Filter by profit centers')
    users = models.JSONField(default=list, help_text='Filter by users')
    
    # Analysis results
    total_transactions = models.IntegerField(default=0)
    total_amount = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal('0.00'))
    flagged_transactions = models.IntegerField(default=0)
    high_value_transactions = models.IntegerField(default=0)
    
    # Session status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('RUNNING', 'Running'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'analysis_sessions'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class TransactionAnalysis(models.Model):
    """Model for individual transaction analysis results"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    transaction = models.OneToOneField(SAPGLPosting, on_delete=models.CASCADE, related_name='analysis')
    session = models.ForeignKey(AnalysisSession, on_delete=models.CASCADE, related_name='analyses')
    
    # Risk scoring
    risk_score = models.FloatField(default=0.0, help_text='Overall risk score (0-100)')
    
    # Risk levels
    RISK_LEVELS = [
        ('LOW', 'Low Risk'),
        ('MEDIUM', 'Medium Risk'),
        ('HIGH', 'High Risk'),
        ('CRITICAL', 'Critical Risk'),
    ]
    risk_level = models.CharField(max_length=10, choices=RISK_LEVELS, default='LOW')
    
    # Anomaly flags
    amount_anomaly = models.BooleanField(default=False, help_text='Unusual amount flag')
    timing_anomaly = models.BooleanField(default=False, help_text='Unusual timing flag')
    user_anomaly = models.BooleanField(default=False, help_text='Unusual user behavior flag')
    account_anomaly = models.BooleanField(default=False, help_text='Unusual account usage flag')
    pattern_anomaly = models.BooleanField(default=False, help_text='Unusual pattern flag')
    
    # Detailed analysis
    analysis_details = models.JSONField(default=dict, help_text='Detailed analysis results')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'transaction_analyses'
        ordering = ['-risk_score', '-created_at']
    
    def __str__(self):
        return f"Analysis for {self.transaction.document_number} - {self.risk_level}"

class SystemMetrics(models.Model):
    """Model to track system performance and usage metrics"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    metric_date = models.DateField(db_index=True, help_text='Date of the metric')
    
    # Data volume metrics
    total_transactions = models.IntegerField(default=0)
    total_amount = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal('0.00'))
    new_transactions = models.IntegerField(default=0)
    new_amount = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal('0.00'))
    
    # User activity metrics
    active_users = models.IntegerField(default=0)
    unique_documents = models.IntegerField(default=0)
    unique_accounts = models.IntegerField(default=0)
    
    # Analysis metrics
    analyses_run = models.IntegerField(default=0)
    flagged_transactions = models.IntegerField(default=0)
    high_risk_transactions = models.IntegerField(default=0)
    
    # Performance metrics
    avg_processing_time = models.FloatField(default=0.0, help_text='Average processing time in seconds')
    max_processing_time = models.FloatField(default=0.0, help_text='Maximum processing time in seconds')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'system_metrics'
        unique_together = ['metric_date']
        ordering = ['-metric_date']
    
    def __str__(self):
        return f"Metrics for {self.metric_date}"

class FileProcessingJob(models.Model):
    """Model to track file processing jobs with anomaly detection requests"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File information
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='processing_jobs', help_text='Reference to the uploaded data file')
    file_hash = models.CharField(max_length=64, db_index=True, help_text='SHA256 hash of file content for duplicate detection')
    
    # Anomaly detection configuration
    run_anomalies = models.BooleanField(default=False, help_text='Whether to run anomaly detection')
    requested_anomalies = models.JSONField(default=list, help_text='List of requested anomaly types to run')
    
    # Processing status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('QUEUED', 'Queued for Processing'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('CELERY_ERROR', 'Celery Connection Error'),
        ('SKIPPED', 'Skipped - Duplicate Content'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Processing results
    analytics_results = models.JSONField(default=dict, help_text='Results from default analytics (TB, TE, GL summaries)')
    anomaly_results = models.JSONField(default=dict, help_text='Results from requested anomaly tests')
    ml_training_results = models.JSONField(default=dict, help_text='Results from ML model training')
    
    # Processing metadata
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    error_message = models.TextField(blank=True, null=True)
    
    # Reference to existing results (for duplicate content)
    existing_job = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='duplicate_jobs', help_text='Reference to existing job if content is duplicate')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'file_processing_jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['file_hash', 'status']),
            models.Index(fields=['run_anomalies', 'status']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"Job {self.id} - {self.data_file.file_name} ({self.status})"
    
    @property
    def is_duplicate_content(self):
        """Check if this job has duplicate content with an existing completed job"""
        return FileProcessingJob.objects.filter(
            file_hash=self.file_hash,
            status='COMPLETED'
        ).exclude(id=self.id).exists()
    
    @property
    def duplicate_job(self):
        """Get the existing job with duplicate content if any"""
        return FileProcessingJob.objects.filter(
            file_hash=self.file_hash,
            status='COMPLETED'
        ).exclude(id=self.id).first()
    
    def get_processing_summary(self):
        """Get a summary of processing results"""
        return {
            'job_id': str(self.id),
            'file_name': self.data_file.file_name,
            'status': self.status,
            'run_anomalies': self.run_anomalies,
            'requested_anomalies': self.requested_anomalies,
            'processing_duration': self.processing_duration,
            'analytics_results': self.analytics_results,
            'anomaly_results': self.anomaly_results,
            'is_duplicate_content': self.is_duplicate_content,
            'existing_job_id': str(self.existing_job.id) if self.existing_job else None,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
        }

class DuplicateAnalysisResult(models.Model):
    """Model to store enhanced duplicate analysis results for files"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='duplicate_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='enhanced_duplicate', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # Analysis results - stored as JSON for flexibility
    analysis_info = models.JSONField(default=dict, help_text='General analysis information (total transactions, duplicates, etc.)')
    duplicate_list = models.JSONField(default=list, help_text='List of duplicate transactions found')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns (by type, user, account, etc.)')
    slicer_filters = models.JSONField(default=dict, help_text='Slicer filters for dynamic filtering')
    summary_table = models.JSONField(default=list, help_text='Summary table data')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    detailed_insights = models.JSONField(default=dict, help_text='Detailed insights and recommendations')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='duplicate_results', help_text='Reference to the processing job that generated this analysis')
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
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'duplicate_analysis_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['status', 'analysis_date']),
            models.Index(fields=['analysis_type']),
        ]
    
    def __str__(self):
        return f"Duplicate Analysis for {self.data_file.file_name} ({self.analysis_date.strftime('%Y-%m-%d %H:%M')})"
    
    def get_analysis_summary(self):
        """Get a summary of the analysis results"""
        return {
            'analysis_id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'analysis_date': self.analysis_date.isoformat(),
            'analysis_type': self.analysis_type,
            'status': self.status,
            'total_duplicates': len(self.duplicate_list),
            'total_amount': sum(item.get('amount', 0) for item in self.duplicate_list),
            'processing_duration': self.processing_duration,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
    
    def get_duplicate_count(self):
        """Get the total number of duplicates found"""
        return len(self.duplicate_list)
    
    def get_total_amount(self):
        """Get the total amount involved in duplicates"""
        return sum(item.get('amount', 0) for item in self.duplicate_list)
    
    def get_risk_distribution(self):
        """Get risk level distribution"""
        risk_counts = {}
        for item in self.duplicate_list:
            risk_score = item.get('risk_score', 0)
            if risk_score >= 90:
                risk_level = 'Critical'
            elif risk_score >= 70:
                risk_level = 'High'
            elif risk_score >= 40:
                risk_level = 'Medium'
            else:
                risk_level = 'Low'
            
            risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
        
        return risk_counts

class BackdatedAnalysisResult(models.Model):
    """Model to store enhanced backdated analysis results for files"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='backdated_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='enhanced_backdated', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # Analysis results - stored as JSON for flexibility
    analysis_info = models.JSONField(default=dict, help_text='General analysis information (total backdated entries, amounts, etc.)')
    backdated_entries = models.JSONField(default=list, help_text='List of backdated transactions found')
    backdated_by_document = models.JSONField(default=list, help_text='Backdated entries grouped by document number')
    backdated_by_account = models.JSONField(default=list, help_text='Backdated entries grouped by account')
    backdated_by_user = models.JSONField(default=list, help_text='Backdated entries grouped by user')
    audit_recommendations = models.JSONField(default=dict, help_text='Audit recommendations and priorities')
    compliance_assessment = models.JSONField(default=dict, help_text='Compliance risk assessment')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='backdated_results', help_text='Reference to the processing job that generated this analysis')
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
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'backdated_analysis_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['analysis_type', 'status']),
            models.Index(fields=['processing_job']),
        ]
    
    def __str__(self):
        return f"Backdated Analysis for {self.data_file.file_name} - {self.analysis_date}"
    
    def get_analysis_summary(self):
        """Get summary of backdated analysis"""
        summary = self.analysis_info.copy()
        summary.update({
            'analysis_date': self.analysis_date.isoformat(),
            'analysis_type': self.analysis_type,
            'analysis_version': self.analysis_version,
            'status': self.status,
            'processing_duration': self.processing_duration,
        })
        return summary
    
    def get_backdated_count(self):
        """Get total number of backdated entries"""
        return self.analysis_info.get('total_backdated_entries', 0)
    
    def get_total_amount(self):
        """Get total amount of backdated entries"""
        return self.analysis_info.get('total_amount', 0)
    
    def get_risk_distribution(self):
        """Get risk distribution of backdated entries"""
        return {
            'high_risk': self.analysis_info.get('high_risk_entries', 0),
            'medium_risk': self.analysis_info.get('medium_risk_entries', 0),
            'low_risk': self.analysis_info.get('low_risk_entries', 0),
        }
    
    def get_high_priority_recommendations(self):
        """Get high priority audit recommendations"""
        return self.audit_recommendations.get('high_priority', [])
    
    def get_compliance_issues(self):
        """Get compliance issues identified"""
        return self.compliance_assessment.get('compliance_issues', [])


class AnalyticsResult(models.Model):
    """Model to store analytics results for files"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='analytics_results', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='comprehensive_analytics', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # Analytics results - stored as JSON for flexibility
    trial_balance = models.JSONField(default=dict, help_text='Trial balance analysis results')
    general_ledger_summary = models.JSONField(default=dict, help_text='General ledger summary results')
    account_analysis = models.JSONField(default=dict, help_text='Account-level analysis results')
    transaction_summary = models.JSONField(default=dict, help_text='Transaction summary statistics')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns and summaries')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='analytics_result_objects', help_text='Reference to the processing job that generated this analysis')
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
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'analytics_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['status', 'analysis_date']),
            models.Index(fields=['analysis_type']),
        ]
    
    def __str__(self):
        return f"Analytics for {self.data_file.file_name} ({self.analysis_date.strftime('%Y-%m-%d %H:%M')})"
    
    def get_analysis_summary(self):
        """Get a summary of the analysis results"""
        return {
            'analysis_id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'analysis_date': self.analysis_date.isoformat(),
            'analysis_type': self.analysis_type,
            'status': self.status,
            'processing_duration': self.processing_duration,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

class MLModelTraining(models.Model):
    """Model to track ML model training sessions and performance"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Training session name')
    description = models.TextField(blank=True, null=True, help_text='Training session description')
    
    # Model configuration
    model_type = models.CharField(max_length=50, choices=[
        ('isolation_forest', 'Isolation Forest'),
        ('random_forest', 'Random Forest'),
        ('dbscan', 'DBSCAN'),
        ('ensemble', 'Ensemble'),
        ('all', 'All Models'),
    ], help_text='Type of ML model trained')
    
    # Training data information
    training_data_size = models.IntegerField(help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    feature_count = models.IntegerField(help_text='Number of features used for training')
    
    # Training parameters
    training_parameters = models.JSONField(default=dict, help_text='Model training parameters')
    
    # Performance metrics
    performance_metrics = models.JSONField(default=dict, help_text='Model performance metrics (AUC, accuracy, etc.)')
    validation_metrics = models.JSONField(default=dict, help_text='Cross-validation metrics')
    
    # Training status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Training metadata
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    training_duration = models.FloatField(null=True, blank=True, help_text='Training duration in seconds')
    error_message = models.TextField(blank=True, null=True)
    
    # Model file information
    model_file_path = models.CharField(max_length=500, blank=True, null=True, help_text='Path to saved model files')
    model_version = models.CharField(max_length=20, default='1.0.0', help_text='Model version')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'ml_model_training'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.model_type} ({self.status})"
    
    @property
    def is_latest_model(self):
        """Check if this is the latest trained model of its type"""
        return MLModelTraining.objects.filter(
            model_type=self.model_type,
            status='COMPLETED'
        ).order_by('-created_at').first() == self
    
    def get_training_summary(self):
        """Get a summary of training results"""
        return {
            'training_id': str(self.id),
            'session_name': self.session_name,
            'model_type': self.model_type,
            'status': self.status,
            'training_data_size': self.training_data_size,
            'feature_count': self.feature_count,
            'performance_metrics': self.performance_metrics,
            'training_duration': self.training_duration,
            'model_version': self.model_version,
            'is_latest_model': self.is_latest_model,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
        }

class MLModelProcessingResult(models.Model):
    """Model to store ML model processing results for individual files"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File and job references
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='ml_processing_results', help_text='Reference to the data file')
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='ml_processing_results', help_text='Reference to the processing job')
    
    # ML Model information
    model_type = models.CharField(max_length=50, choices=[
        ('isolation_forest', 'Isolation Forest'),
        ('random_forest', 'Random Forest'),
        ('dbscan', 'DBSCAN'),
        ('ensemble', 'Ensemble'),
        ('duplicate_detection', 'Duplicate Detection'),
        ('anomaly_detection', 'Anomaly Detection'),
        ('all', 'All Models'),
    ], help_text='Type of ML model used')
    
    # Processing results
    processing_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    # Results data
    anomalies_detected = models.IntegerField(default=0, help_text='Number of anomalies detected')
    duplicates_found = models.IntegerField(default=0, help_text='Number of duplicates found')
    risk_score = models.FloatField(default=0.0, help_text='Overall risk score')
    confidence_score = models.FloatField(default=0.0, help_text='Model confidence score')
    
    # Detailed results stored as JSON
    detailed_results = models.JSONField(default=dict, help_text='Detailed ML processing results')
    model_metrics = models.JSONField(default=dict, help_text='Model performance metrics')
    feature_importance = models.JSONField(default=dict, help_text='Feature importance scores')
    
    # Processing metadata
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    data_size = models.IntegerField(default=0, help_text='Number of records processed')
    model_version = models.CharField(max_length=20, default='1.0.0', help_text='Model version used')
    
    # Error handling
    error_message = models.TextField(blank=True, null=True, help_text='Error message if processing failed')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    processed_at = models.DateTimeField(null=True, blank=True, help_text='When processing was completed')
    
    class Meta:
        db_table = 'ml_model_processing_results'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_file', 'model_type']),
            models.Index(fields=['processing_status', 'created_at']),
            models.Index(fields=['model_type', 'processing_status']),
        ]
    
    def __str__(self):
        return f"ML Processing for {self.data_file.file_name} - {self.model_type} ({self.processing_status})"
    
    def get_summary(self):
        """Get a summary of the ML processing results"""
        return {
            'id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'model_type': self.model_type,
            'processing_status': self.processing_status,
            'anomalies_detected': self.anomalies_detected,
            'duplicates_found': self.duplicates_found,
            'risk_score': self.risk_score,
            'confidence_score': self.confidence_score,
            'processing_duration': self.processing_duration,
            'data_size': self.data_size,
            'created_at': self.created_at.isoformat(),
            'processed_at': self.processed_at.isoformat() if self.processed_at else None,
        }

class AnalyticsProcessingResult(models.Model):
    """Model to store comprehensive analytics processing results"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File and job references
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='analytics_processing_results', help_text='Reference to the data file')
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='analytics_processing_results', help_text='Reference to the processing job')
    
    # Analytics type
    analytics_type = models.CharField(max_length=50, choices=[
        ('default_analytics', 'Default Analytics'),
        ('comprehensive_expense', 'Comprehensive Expense Analytics'),
        ('duplicate_analysis', 'Duplicate Analysis'),
        ('anomaly_detection', 'Anomaly Detection'),
        ('risk_assessment', 'Risk Assessment'),
        ('user_patterns', 'User Patterns'),
        ('account_patterns', 'Account Patterns'),
        ('temporal_patterns', 'Temporal Patterns'),
        ('all', 'All Analytics'),
    ], help_text='Type of analytics performed')
    
    # Processing status
    processing_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    # Results summary
    total_transactions = models.IntegerField(default=0, help_text='Total transactions analyzed')
    total_amount = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal('0.00'), help_text='Total amount analyzed')
    unique_users = models.IntegerField(default=0, help_text='Number of unique users')
    unique_accounts = models.IntegerField(default=0, help_text='Number of unique accounts')
    
    # Key metrics
    flagged_transactions = models.IntegerField(default=0, help_text='Number of flagged transactions')
    high_risk_transactions = models.IntegerField(default=0, help_text='Number of high-risk transactions')
    anomalies_found = models.IntegerField(default=0, help_text='Number of anomalies found')
    duplicates_found = models.IntegerField(default=0, help_text='Number of duplicates found')
    
    # Detailed results stored as JSON
    trial_balance_data = models.JSONField(default=dict, help_text='Trial balance analysis results')
    expense_breakdown = models.JSONField(default=dict, help_text='Expense breakdown analysis')
    user_patterns = models.JSONField(default=dict, help_text='User pattern analysis')
    account_patterns = models.JSONField(default=dict, help_text='Account pattern analysis')
    temporal_patterns = models.JSONField(default=dict, help_text='Temporal pattern analysis')
    risk_assessment = models.JSONField(default=dict, help_text='Risk assessment results')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Analysis algorithm version')
    
    # Error handling
    error_message = models.TextField(blank=True, null=True, help_text='Error message if processing failed')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    processed_at = models.DateTimeField(null=True, blank=True, help_text='When processing was completed')
    
    class Meta:
        db_table = 'analytics_processing_results'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_file', 'analytics_type']),
            models.Index(fields=['processing_status', 'created_at']),
            models.Index(fields=['analytics_type', 'processing_status']),
        ]
    
    def __str__(self):
        return f"Analytics for {self.data_file.file_name} - {self.analytics_type} ({self.processing_status})"
    
    def get_summary(self):
        """Get a summary of the analytics processing results"""
        return {
            'id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'analytics_type': self.analytics_type,
            'processing_status': self.processing_status,
            'total_transactions': self.total_transactions,
            'total_amount': float(self.total_amount),
            'unique_users': self.unique_users,
            'unique_accounts': self.unique_accounts,
            'flagged_transactions': self.flagged_transactions,
            'high_risk_transactions': self.high_risk_transactions,
            'anomalies_found': self.anomalies_found,
            'duplicates_found': self.duplicates_found,
            'processing_duration': self.processing_duration,
            'created_at': self.created_at.isoformat(),
            'processed_at': self.processed_at.isoformat() if self.processed_at else None,
        }

class ProcessingJobTracker(models.Model):
    """Model to track overall processing job progress and status"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Job references
    processing_job = models.OneToOneField(FileProcessingJob, on_delete=models.CASCADE, related_name='job_tracker', help_text='Reference to the processing job')
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='job_trackers', help_text='Reference to the data file')
    
    # Overall progress tracking
    total_steps = models.IntegerField(default=0, help_text='Total number of processing steps')
    completed_steps = models.IntegerField(default=0, help_text='Number of completed steps')
    current_step = models.CharField(max_length=100, blank=True, null=True, help_text='Current processing step')
    
    # Step status tracking
    file_processing_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    analytics_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    ml_processing_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    anomaly_detection_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
    # Progress percentages
    overall_progress = models.FloatField(default=0.0, help_text='Overall progress percentage (0-100)')
    file_processing_progress = models.FloatField(default=0.0, help_text='File processing progress percentage')
    analytics_progress = models.FloatField(default=0.0, help_text='Analytics progress percentage')
    ml_progress = models.FloatField(default=0.0, help_text='ML processing progress percentage')
    anomaly_progress = models.FloatField(default=0.0, help_text='Anomaly detection progress percentage')
    
    # Detailed tracking
    step_details = models.JSONField(default=list, help_text='Detailed step-by-step progress')
    error_log = models.JSONField(default=list, help_text='Error log for failed steps')
    
    # Performance metrics
    total_processing_time = models.FloatField(null=True, blank=True, help_text='Total processing time in seconds')
    memory_usage_mb = models.FloatField(null=True, blank=True, help_text='Peak memory usage in MB')
    cpu_usage_percent = models.FloatField(null=True, blank=True, help_text='Peak CPU usage percentage')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True, help_text='When processing started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When processing completed')
    
    class Meta:
        db_table = 'processing_job_trackers'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['processing_job']),
            models.Index(fields=['data_file', 'created_at']),
            models.Index(fields=['overall_progress', 'created_at']),
        ]
    
    def __str__(self):
        return f"Tracker for {self.processing_job.id} - {self.overall_progress}% complete"
    
    def get_progress_summary(self):
        """Get a summary of the processing progress"""
        return {
            'id': str(self.id),
            'job_id': str(self.processing_job.id),
            'file_name': self.data_file.file_name,
            'overall_progress': self.overall_progress,
            'current_step': self.current_step,
            'completed_steps': self.completed_steps,
            'total_steps': self.total_steps,
            'status_breakdown': {
                'file_processing': self.file_processing_status,
                'analytics': self.analytics_status,
                'ml_processing': self.ml_processing_status,
                'anomaly_detection': self.anomaly_detection_status,
            },
            'progress_breakdown': {
                'file_processing': self.file_processing_progress,
                'analytics': self.analytics_progress,
                'ml_processing': self.ml_progress,
                'anomaly_detection': self.anomaly_progress,
            },
            'performance': {
                'total_time': self.total_processing_time,
                'memory_usage': self.memory_usage_mb,
                'cpu_usage': self.cpu_usage_percent,
            },
            'timestamps': {
                'created': self.created_at.isoformat(),
                'started': self.started_at.isoformat() if self.started_at else None,
                'completed': self.completed_at.isoformat() if self.completed_at else None,
            }
        }
    
    def update_progress(self, step_name, progress_percentage, status='PROCESSING'):
        """Update progress for a specific step"""
        self.current_step = step_name
        self.overall_progress = progress_percentage
        
        # Update specific step progress based on step name
        if 'file' in step_name.lower():
            self.file_processing_progress = progress_percentage
            self.file_processing_status = status
        elif 'analytics' in step_name.lower():
            self.analytics_progress = progress_percentage
            self.analytics_status = status
        elif 'ml' in step_name.lower() or 'model' in step_name.lower():
            self.ml_progress = progress_percentage
            self.ml_processing_status = status
        elif 'anomaly' in step_name.lower():
            self.anomaly_progress = progress_percentage
            self.anomaly_detection_status = status
        
        # Add step to details
        step_detail = {
            'step': step_name,
            'progress': progress_percentage,
            'status': status,
            'timestamp': timezone.now().isoformat()
        }
        self.step_details.append(step_detail)
        
        self.save()
