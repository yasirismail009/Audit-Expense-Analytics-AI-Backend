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
    
    # File association
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='postings', null=True, blank=True, help_text='Reference to the uploaded data file')
    
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
    
    # Analysis flags
    is_flagged_expense = models.BooleanField(default=False, help_text='Flagged as expense transaction')
    expense_category = models.CharField(max_length=100, blank=True, null=True, help_text='Expense category classification')
    expense_risk_level = models.CharField(
        max_length=20, 
        choices=[('LOW', 'Low Risk'), ('MEDIUM', 'Medium Risk'), ('HIGH', 'High Risk'), ('CRITICAL', 'Critical Risk')],
        default='LOW',
        help_text='Risk level for expense transaction'
    )
    expense_risk_score = models.FloatField(default=0.0, help_text='Risk score for expense transaction (0-100)')
    expense_analysis_details = models.JSONField(default=dict, help_text='Detailed expense analysis results')
    
    # Anomaly detection tracking
    is_duplicate = models.BooleanField(default=False, help_text='Flagged as duplicate transaction')
    duplicate_type = models.CharField(max_length=20, blank=True, null=True, help_text='Type of duplicate (type_1, type_2, etc.)')
    duplicate_risk_score = models.FloatField(default=0.0, help_text='Risk score for duplicate detection (0-100)')
    duplicate_analysis_details = models.JSONField(default=dict, help_text='Detailed duplicate analysis results')
    
    is_backdated = models.BooleanField(default=False, help_text='Flagged as backdated transaction')
    backdated_days = models.IntegerField(default=0, help_text='Number of days between document date and posting date')
    backdated_risk_score = models.FloatField(default=0.0, help_text='Risk score for backdated detection (0-100)')
    backdated_analysis_details = models.JSONField(default=dict, help_text='Detailed backdated analysis results')
    
    # Overall anomaly tracking
    overall_risk_score = models.FloatField(default=0.0, help_text='Overall risk score combining all analyses (0-100)')
    anomaly_types = models.JSONField(default=list, help_text='List of anomaly types detected for this transaction')
    anomaly_analysis_summary = models.JSONField(default=dict, help_text='Summary of all anomaly analyses for this transaction')
    
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
            models.Index(fields=['data_file', 'document_number']),
        ]
        ordering = ['-posting_date', '-created_at']
    
    def __str__(self):
        return f"{self.document_number} - {self.gl_account} - {self.amount_local_currency}"
    
    @property
    def is_high_value(self):
        """Check if transaction is high value (over 1,000,000 SAR)"""
        return self.amount_local_currency > Decimal('1000000.00')
    
    @property
    def is_cleared(self):
        """Check if transaction is cleared (has clearing document)"""
        return bool(self.clearing_document)
    
    @property
    def has_arabic_text(self):
        """Check if text contains Arabic characters"""
        if not self.text:
            return False
        # Simple check for Arabic Unicode range
        arabic_range = range(0x0600, 0x06FF)
        return any(ord(char) in arabic_range for char in self.text)
    
    @property
    def is_expense_account(self):
        """Check if GL account is an expense account"""
        # Common expense account patterns
        expense_patterns = ['5', '6', '7']  # 5xxx, 6xxx, 7xxx series
        return any(self.gl_account.startswith(pattern) for pattern in expense_patterns)
    
    @property
    def expense_type(self):
        """Determine expense type based on GL account"""
        if not self.is_expense_account:
            return None
        
        # Map GL account ranges to expense types
        account_mapping = {
            '5000': 'Cost of Goods Sold',
            '5100': 'Direct Labor',
            '5200': 'Direct Materials',
            '5300': 'Manufacturing Overhead',
            '6000': 'Selling Expenses',
            '6100': 'Advertising',
            '6200': 'Sales Commissions',
            '6300': 'Travel & Entertainment',
            '7000': 'General & Administrative',
            '7100': 'Office Supplies',
            '7200': 'Utilities',
            '7300': 'Rent',
            '7400': 'Insurance',
            '7500': 'Professional Services',
        }
        
        for prefix, expense_type in account_mapping.items():
            if self.gl_account.startswith(prefix):
                return expense_type
        
        return 'Other Expenses'
    
    def save(self, *args, **kwargs):
        # Auto-determine transaction type based on GL account if not set
        if not self.transaction_type and self.gl_account:
            # Asset and expense accounts normally have debit balances
            if self.gl_account.startswith(('1', '5', '6', '7')):
                self.transaction_type = 'DEBIT'
            # Liability, equity, and revenue accounts normally have credit balances
            elif self.gl_account.startswith(('2', '3', '4')):
                self.transaction_type = 'CREDIT'
        
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

    def get_transaction_document_numbers(self):
        """Get a list of unique document numbers from postings in this data file."""
        return list(set(posting.document_number for posting in self.postings.all()))

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
    
    # Analysis flags
    general_analysis_flag = models.BooleanField(default=False, help_text='General analysis flag')
    duplicate_analysis_flag = models.BooleanField(default=False, help_text='Duplicate analysis flag')
    backdated_analysis_flag = models.BooleanField(default=False, help_text='Backdated analysis flag')
    overall_analysis_flag = models.BooleanField(default=False, help_text='Overall analysis flag')
    risk_analysis_flag = models.BooleanField(default=False, help_text='Risk analysis flag')
    
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
    
    # Additional analysis fields for comprehensive reporting
    audit_recommendations = models.JSONField(default=dict, help_text='Audit recommendations and priorities')
    compliance_assessment = models.JSONField(default=dict, help_text='Compliance risk assessment')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    
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
        total = 0
        for item in self.duplicate_list:
            # Handle the new structure where amounts are in transaction1 and transaction2
            if 'transaction1' in item and 'amount' in item['transaction1']:
                total += item['transaction1']['amount']
            elif 'amount' in item:
                # Fallback for old structure
                total += item['amount']
        return total
    
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
    
    def get_compliance_issues(self):
        """Get compliance issues based on duplicate analysis results"""
        compliance_issues = []
        
        # Check for high-risk duplicates
        high_risk_duplicates = [item for item in self.duplicate_list if item.get('risk_score', 0) >= 80]
        if high_risk_duplicates:
            # Calculate total amount for high-risk duplicates
            high_risk_amount = 0
            for item in high_risk_duplicates:
                if 'transaction1' in item and 'amount' in item['transaction1']:
                    high_risk_amount += item['transaction1']['amount']
                elif 'amount' in item:
                    high_risk_amount += item['amount']
            
            compliance_issues.append({
                'type': 'high_risk_duplicates',
                'severity': 'HIGH',
                'description': f'Found {len(high_risk_duplicates)} high-risk duplicate transactions',
                'count': len(high_risk_duplicates),
                'total_amount': high_risk_amount
            })
        
        # Check for duplicate percentage
        total_duplicates = len(self.duplicate_list)
        if total_duplicates > 0:
            duplicate_percentage = self.analysis_info.get('duplicate_percentage', 0)
            if duplicate_percentage > 10:  # More than 10% duplicates
                compliance_issues.append({
                    'type': 'high_duplicate_percentage',
                    'severity': 'MEDIUM',
                    'description': f'High duplicate percentage: {duplicate_percentage:.2f}%',
                    'percentage': duplicate_percentage
                })
        
        # Check for large amount duplicates
        large_amount_duplicates = []
        large_amount_total = 0
        for item in self.duplicate_list:
            amount = 0
            if 'transaction1' in item and 'amount' in item['transaction1']:
                amount = item['transaction1']['amount']
            elif 'amount' in item:
                amount = item['amount']
            
            if amount > 1000000:  # Over 1M SAR
                large_amount_duplicates.append(item)
                large_amount_total += amount
        
        if large_amount_duplicates:
            compliance_issues.append({
                'type': 'large_amount_duplicates',
                'severity': 'HIGH',
                'description': f'Found {len(large_amount_duplicates)} duplicate transactions with amounts over 1M SAR',
                'count': len(large_amount_duplicates),
                'total_amount': large_amount_total
            })
        
        # Check for user concentration in duplicates
        user_duplicate_counts = {}
        for item in self.duplicate_list:
            # Handle new structure where user is in transaction1
            if 'transaction1' in item and 'user' in item['transaction1']:
                user = item['transaction1']['user']
            elif 'user' in item:
                user = item['user']
            else:
                user = 'Unknown'
            user_duplicate_counts[user] = user_duplicate_counts.get(user, 0) + 1
        
        high_duplicate_users = [user for user, count in user_duplicate_counts.items() if count > 5]
        if high_duplicate_users:
            compliance_issues.append({
                'type': 'user_duplicate_concentration',
                'severity': 'MEDIUM',
                'description': f'Users with high duplicate counts: {", ".join(high_duplicate_users)}',
                'users': high_duplicate_users,
                'counts': {user: user_duplicate_counts[user] for user in high_duplicate_users}
            })
        
        return compliance_issues
    
    def get_high_priority_recommendations(self):
        """Get high priority recommendations based on duplicate analysis results"""
        recommendations = []
        
        # Check for critical risk duplicates
        critical_duplicates = [item for item in self.duplicate_list if item.get('risk_score', 0) >= 90]
        if critical_duplicates:
            # Calculate total amount for critical duplicates
            critical_amount = 0
            for item in critical_duplicates:
                if 'transaction1' in item and 'amount' in item['transaction1']:
                    critical_amount += item['transaction1']['amount']
                elif 'amount' in item:
                    critical_amount += item['amount']
            
            recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Immediate investigation required',
                'description': f'Found {len(critical_duplicates)} critical-risk duplicate transactions',
                'count': len(critical_duplicates),
                'total_amount': critical_amount,
                'recommendation': 'Review and investigate these transactions immediately for potential fraud or errors'
            })
        
        # Check for high-risk duplicates
        high_risk_duplicates = [item for item in self.duplicate_list if 80 <= item.get('risk_score', 0) < 90]
        if high_risk_duplicates:
            # Calculate total amount for high-risk duplicates
            high_risk_amount = 0
            for item in high_risk_duplicates:
                if 'transaction1' in item and 'amount' in item['transaction1']:
                    high_risk_amount += item['transaction1']['amount']
                elif 'amount' in item:
                    high_risk_amount += item['amount']
            
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Priority investigation',
                'description': f'Found {len(high_risk_duplicates)} high-risk duplicate transactions',
                'count': len(high_risk_duplicates),
                'total_amount': high_risk_amount,
                'recommendation': 'Investigate these transactions within 48 hours'
            })
        
        # Check for large amount duplicates
        large_amount_duplicates = []
        large_amount_total = 0
        for item in self.duplicate_list:
            amount = 0
            if 'transaction1' in item and 'amount' in item['transaction1']:
                amount = item['transaction1']['amount']
            elif 'amount' in item:
                amount = item['amount']
            
            if amount > 1000000:  # Over 1M SAR
                large_amount_duplicates.append(item)
                large_amount_total += amount
        
        if large_amount_duplicates:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Large amount duplicate review',
                'description': f'Found {len(large_amount_duplicates)} duplicate transactions with amounts over 1M SAR',
                'count': len(large_amount_duplicates),
                'total_amount': large_amount_total,
                'recommendation': 'Review these large amount duplicates for potential financial statement impact'
            })
        
        # Check for user concentration
        user_duplicate_counts = {}
        for item in self.duplicate_list:
            # Handle new structure where user is in transaction1
            if 'transaction1' in item and 'user' in item['transaction1']:
                user = item['transaction1']['user']
            elif 'user' in item:
                user = item['user']
            else:
                user = 'Unknown'
            user_duplicate_counts[user] = user_duplicate_counts.get(user, 0) + 1
        
        high_duplicate_users = [user for user, count in user_duplicate_counts.items() if count > 10]
        if high_duplicate_users:
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'User behavior review',
                'description': f'Users with excessive duplicates: {", ".join(high_duplicate_users)}',
                'users': high_duplicate_users,
                'counts': {user: user_duplicate_counts[user] for user in high_duplicate_users},
                'recommendation': 'Review user training and system controls for these users'
            })
        
        # Check for duplicate percentage
        total_duplicates = len(self.duplicate_list)
        if total_duplicates > 0:
            duplicate_percentage = self.analysis_info.get('duplicate_percentage', 0)
            if duplicate_percentage > 15:  # More than 15% duplicates
                recommendations.append({
                    'priority': 'HIGH',
                    'action': 'System control review',
                    'description': f'Very high duplicate percentage: {duplicate_percentage:.2f}%',
                    'percentage': duplicate_percentage,
                    'recommendation': 'Review and strengthen system controls to prevent duplicate entries'
                })
            elif duplicate_percentage > 5:  # More than 5% duplicates
                recommendations.append({
                    'priority': 'MEDIUM',
                    'action': 'Process improvement',
                    'description': f'High duplicate percentage: {duplicate_percentage:.2f}%',
                    'percentage': duplicate_percentage,
                    'recommendation': 'Consider process improvements to reduce duplicate entries'
                })
        
        return recommendations

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
            models.Index(fields=['processing_job', 'status']),
        ]
    
    def __str__(self):
        return f"Backdated Analysis for {self.data_file.file_name} - {self.analysis_date}"
    
    def get_analysis_summary(self):
        """Get summary of backdated analysis results"""
        return {
            'total_backdated': self.get_backdated_count(),
            'total_amount': self.get_total_amount(),
            'risk_distribution': self.get_risk_distribution(),
            'processing_duration': self.processing_duration,
            'analysis_date': self.analysis_date.isoformat()
        }
    
    def get_backdated_count(self):
        """Get count of backdated entries"""
        return len(self.backdated_entries) if self.backdated_entries else 0
    
    def get_total_amount(self):
        """Get total amount of backdated entries"""
        if not self.backdated_entries:
            return Decimal('0.00')
        return sum(Decimal(str(entry.get('amount_local_currency', 0))) for entry in self.backdated_entries)
    
    def get_risk_distribution(self):
        """Get risk distribution of backdated entries"""
        if not self.backdated_entries:
            return {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        
        risk_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for entry in self.backdated_entries:
            risk_level = entry.get('risk_level', 'low').lower()
            if risk_level in risk_counts:
                risk_counts[risk_level] += 1
        
        return risk_counts
    
    def get_high_priority_recommendations(self):
        """Get high priority audit recommendations"""
        return self.audit_recommendations.get('high_priority', [])
    
    def get_compliance_issues(self):
        """Get compliance issues identified"""
        return self.compliance_assessment.get('compliance_issues', [])

class UserAnalysisResult(models.Model):
    """Model to store user analysis results for identifying user activity patterns and anomalies"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='user_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='user_analysis', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # Analysis results - stored as JSON for flexibility
    analysis_info = models.JSONField(default=dict, help_text='General analysis information (total users, transactions per user, etc.)')
    user_transaction_summary = models.JSONField(default=list, help_text='Summary of transactions per user')
    user_debit_analysis = models.JSONField(default=list, help_text='Debit value analysis per user')
    user_account_distribution = models.JSONField(default=list, help_text='Number of unique users per account')
    user_fs_line_distribution = models.JSONField(default=list, help_text='Number of unique users per FS line')
    user_anomalies = models.JSONField(default=list, help_text='List of user anomalies detected')
    user_risk_assessment = models.JSONField(default=dict, help_text='User risk assessment and scoring')
    user_patterns = models.JSONField(default=dict, help_text='User activity patterns and trends')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='user_results', help_text='Reference to the processing job that generated this analysis')
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
        db_table = 'user_analysis_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['analysis_type', 'status']),
            models.Index(fields=['processing_job', 'status']),
        ]
    
    def __str__(self):
        return f"User Analysis for {self.data_file.file_name} - {self.analysis_date}"
    
    def get_analysis_summary(self):
        """Get summary of user analysis results"""
        return {
            'total_users': self.get_total_users(),
            'total_transactions': self.get_total_transactions(),
            'anomalies_detected': self.get_anomalies_count(),
            'high_risk_users': self.get_high_risk_users_count(),
            'processing_duration': self.processing_duration,
            'analysis_date': self.analysis_date.isoformat()
        }
    
    def get_total_users(self):
        """Get total number of unique users"""
        return len(self.user_transaction_summary) if self.user_transaction_summary else 0
    
    def get_total_transactions(self):
        """Get total number of transactions"""
        if not self.user_transaction_summary:
            return 0
        return sum(user.get('transaction_count', 0) for user in self.user_transaction_summary)
    
    def get_anomalies_count(self):
        """Get count of user anomalies detected"""
        return len(self.user_anomalies) if self.user_anomalies else 0
    
    def get_high_risk_users_count(self):
        """Get count of high-risk users"""
        if not self.user_risk_assessment:
            return 0
        
        # Handle list-based user risk assessment
        if isinstance(self.user_risk_assessment, list):
            return len([user for user in self.user_risk_assessment 
                       if isinstance(user, dict) and user.get('risk_level', 'low') in ['high', 'critical']])
        
        # Handle dictionary-based user risk assessment (legacy)
        if isinstance(self.user_risk_assessment, dict):
            return len([user for user in self.user_risk_assessment.get('user_risk_scores', []) 
                       if isinstance(user, dict) and user.get('risk_level', 'low') in ['high', 'critical']])
        
        return 0




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



class AnalyticsProcessingResult(models.Model):
    """Model to store comprehensive analytics processing results"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File and job references
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='analytics_processing_results', help_text='Reference to the data file')
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='analytics_processing_results', help_text='Reference to the processing job')
    
    # Analytics type
    analytics_type = models.CharField(max_length=50, choices=[
        ('general_analysis', 'General Analysis'),
        ('duplicate_analysis', 'Duplicate Analysis'),
        ('backdated_analysis', 'Backdated Analysis'),
        ('overall_analysis', 'Overall Analysis'),
        ('risk_analysis', 'Risk Analysis'),
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

class GeneralAnalysisResult(models.Model):
    """Model to store general analysis results including trial balance, GL account summaries, and statistical calculations"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='general_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='general_analysis', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # General Analysis Results - stored as JSON for flexibility
    trial_balance_summary = models.JSONField(default=dict, help_text='Trial balance summary (total debits, credits, net)')
    gl_account_summaries = models.JSONField(default=list, help_text='Detailed GL account summaries with debits, credits, balances')
    user_summaries = models.JSONField(default=list, help_text='User activity summaries per GL account')
    statistical_calculations = models.JSONField(default=dict, help_text='Mean, standard deviation, and other statistical measures')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='general_results', help_text='Reference to the processing job that generated this analysis')
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
        db_table = 'general_analysis_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['status', 'analysis_date']),
            models.Index(fields=['analysis_type']),
        ]
    
    def __str__(self):
        return f"General Analysis for {self.data_file.file_name} ({self.analysis_date.strftime('%Y-%m-%d %H:%M')})"
    
    def get_analysis_summary(self):
        """Get a summary of the general analysis results"""
        return {
            'analysis_id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'analysis_date': self.analysis_date.isoformat(),
            'analysis_type': self.analysis_type,
            'status': self.status,
            'processing_duration': self.processing_duration,
            'trial_balance_summary': self.trial_balance_summary,
            'total_accounts': len(self.gl_account_summaries),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

class OverallAnalysisResult(models.Model):
    """Model to store overall analysis results combining all analysis types with risk calculations"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='overall_analyses', help_text='Reference to the data file')
    
    # Analysis metadata
    analysis_date = models.DateTimeField(auto_now_add=True, help_text='When the analysis was performed')
    analysis_type = models.CharField(max_length=50, default='overall_analysis', help_text='Type of analysis performed')
    analysis_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of analysis algorithm')
    
    # Overall Analysis Results - stored as JSON for flexibility
    transaction_summary = models.JSONField(default=dict, help_text='Overall transaction summary statistics')
    flagged_transactions = models.JSONField(default=list, help_text='List of all flagged transactions with their flag types')
    flag_summary = models.JSONField(default=dict, help_text='Summary of flags by type (duplicate, backdated, etc.)')
    expense_analysis = models.JSONField(default=dict, help_text='Expense data analysis and categorization')
    risk_assessment = models.JSONField(default=dict, help_text='Overall risk assessment and scoring')
    chart_data = models.JSONField(default=dict, help_text='Chart data for visualizations')
    export_data = models.JSONField(default=list, help_text='Export-ready data')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='overall_results', help_text='Reference to the processing job that generated this analysis')
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
        db_table = 'overall_analysis_results'
        ordering = ['-analysis_date']
        indexes = [
            models.Index(fields=['data_file', 'analysis_date']),
            models.Index(fields=['status', 'analysis_date']),
            models.Index(fields=['analysis_type']),
        ]
    
    def __str__(self):
        return f"Overall Analysis for {self.data_file.file_name} ({self.analysis_date.strftime('%Y-%m-%d %H:%M')})"
    
    def get_analysis_summary(self):
        """Get a summary of the overall analysis results"""
        return {
            'analysis_id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'analysis_date': self.analysis_date.isoformat(),
            'analysis_type': self.analysis_type,
            'status': self.status,
            'processing_duration': self.processing_duration,
            'total_flagged': len(self.flagged_transactions),
            'flag_summary': self.flag_summary,
            'overall_risk_score': self.risk_assessment.get('overall_risk_score', 0),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

class RiskScoringDocument(models.Model):
    """Model to store comprehensive risk scoring documentation and methodology"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='risk_scoring_documents', help_text='Reference to the data file')
    
    # Document metadata
    document_date = models.DateTimeField(auto_now_add=True, help_text='When the risk scoring document was generated')
    document_version = models.CharField(max_length=20, default='1.0.0', help_text='Version of the risk scoring methodology')
    document_type = models.CharField(max_length=50, default='comprehensive_risk_scoring', help_text='Type of risk scoring document')
    
    # Risk Scoring Methodology - stored as JSON for flexibility
    methodology_overview = models.JSONField(default=dict, help_text='Overview of risk scoring methodology')
    risk_factors = models.JSONField(default=dict, help_text='Detailed risk factors and their weights')
    scoring_criteria = models.JSONField(default=dict, help_text='Scoring criteria for different risk levels')
    risk_calculations = models.JSONField(default=dict, help_text='Detailed risk calculations for each transaction')
    risk_distributions = models.JSONField(default=dict, help_text='Risk score distributions and statistics')
    recommendations = models.JSONField(default=dict, help_text='Risk-based recommendations and actions')
    audit_implications = models.JSONField(default=dict, help_text='Audit implications and follow-up actions')
    
    # Summary statistics
    total_transactions = models.IntegerField(default=0, help_text='Total transactions analyzed')
    high_risk_transactions = models.IntegerField(default=0, help_text='Number of high-risk transactions')
    medium_risk_transactions = models.IntegerField(default=0, help_text='Number of medium-risk transactions')
    low_risk_transactions = models.IntegerField(default=0, help_text='Number of low-risk transactions')
    overall_risk_score = models.FloatField(default=0.0, help_text='Overall risk score for the dataset')
    
    # Processing metadata
    processing_job = models.ForeignKey(FileProcessingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='risk_scoring_documents', help_text='Reference to the processing job that generated this document')
    processing_duration = models.FloatField(null=True, blank=True, help_text='Processing duration in seconds')
    
    # Document status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='COMPLETED')
    error_message = models.TextField(blank=True, null=True, help_text='Error message if document generation failed')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'risk_scoring_documents'
        ordering = ['-document_date']
        indexes = [
            models.Index(fields=['data_file', 'document_date']),
            models.Index(fields=['status', 'document_date']),
            models.Index(fields=['document_type']),
        ]
    
    def __str__(self):
        return f"Risk Scoring Document for {self.data_file.file_name} ({self.document_date.strftime('%Y-%m-%d %H:%M')})"
    
    def get_document_summary(self):
        """Get a summary of the risk scoring document"""
        return {
            'document_id': str(self.id),
            'file_name': self.data_file.file_name,
            'file_id': str(self.data_file.id),
            'document_date': self.document_date.isoformat(),
            'document_type': self.document_type,
            'document_version': self.document_version,
            'status': self.status,
            'processing_duration': self.processing_duration,
            'total_transactions': self.total_transactions,
            'high_risk_transactions': self.high_risk_transactions,
            'medium_risk_transactions': self.medium_risk_transactions,
            'low_risk_transactions': self.low_risk_transactions,
            'overall_risk_score': self.overall_risk_score,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
    
    def get_risk_distribution(self):
        """Get risk distribution summary"""
        return {
            'high_risk': self.high_risk_transactions,
            'medium_risk': self.medium_risk_transactions,
            'low_risk': self.low_risk_transactions,
            'total': self.total_transactions,
            'high_risk_percentage': (self.high_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'medium_risk_percentage': (self.medium_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'low_risk_percentage': (self.low_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
        }
