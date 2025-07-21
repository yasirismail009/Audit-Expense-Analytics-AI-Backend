from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from decimal import Decimal
import uuid

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
        """Check if this is a high-value transaction (> 1M SAR)"""
        return self.amount_local_currency > 1000000
    
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
