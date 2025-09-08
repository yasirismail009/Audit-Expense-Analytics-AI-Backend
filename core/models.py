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

class BaseAnalysisResult(models.Model):
    """Base model for all analysis results with unified structure"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
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
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
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
    
    def get_compliance_issues(self):
        """Get compliance issues"""
        return self.compliance_assessment.get('compliance_issues', [])
    
    def get_high_priority_recommendations(self):
        """Get high priority recommendations"""
        return self.audit_recommendations.get('high_priority', [])

class BaseModelTraining(models.Model):
    """Base model for all ML model training sessions"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        abstract = True
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"
    
    def get_training_summary(self):
        """Get a summary of the training results"""
        return {
            'session_name': self.session_name,
            'model_type': self.model_type,
            'training_data_size': self.training_data_size,
            'training_duration': self.training_duration,
            'status': self.status,
            'performance_metrics': self.performance_metrics
        }

class BaseProcessingResult(models.Model):
    """Base model for processing results with common fields"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File and job references
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='%(class)s_results', help_text='Reference to the data file')
    processing_job = models.ForeignKey('FileProcessingJob', on_delete=models.SET_NULL, null=True, blank=True, related_name='%(class)s_results', help_text='Reference to the processing job')
    
    # Processing status
    processing_status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ], default='PENDING')
    
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
        abstract = True
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_file', 'created_at']),
            models.Index(fields=['processing_status', 'created_at']),
        ]
    
    def __str__(self):
        return f"{self.__class__.__name__} for {self.data_file.file_name} ({self.processing_status})"

# ============================================================================
# EXISTING MODELS
# ============================================================================

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
    
    # Holiday analysis tracking
    is_holiday_posting = models.BooleanField(default=False, help_text='Flagged as holiday posting')
    holiday_name = models.CharField(max_length=255, blank=True, null=True, help_text='Name of the holiday')
    holiday_type = models.CharField(max_length=50, blank=True, null=True, help_text='Type of holiday (Public holiday, Observance, etc.)')
    holiday_risk_score = models.FloatField(default=0.0, help_text='Risk score for holiday detection (0-100)')
    holiday_analysis_details = models.JSONField(default=dict, help_text='Detailed holiday analysis results')
    
    # Unusual Days analysis tracking
    is_unusual_days_posting = models.BooleanField(default=False, help_text='Flagged as unusual days posting')
    unusual_days_type = models.CharField(max_length=50, blank=True, null=True, help_text='Type of unusual day (weekend, holiday, etc.)')
    unusual_days_risk_score = models.FloatField(default=0.0, help_text='Risk score for unusual days detection (0-100)')
    unusual_days_analysis_details = models.JSONField(default=dict, help_text='Detailed unusual days analysis results')
    
    # User analysis tracking
    is_user_anomaly = models.BooleanField(default=False, help_text='Flagged as user anomaly')
    user_anomaly_type = models.CharField(max_length=50, blank=True, null=True, help_text='Type of user anomaly')
    user_anomaly_risk_score = models.FloatField(default=0.0, help_text='Risk score for user anomaly detection (0-100)')
    user_anomaly_analysis_details = models.JSONField(default=dict, help_text='Detailed user anomaly analysis results')
    
    # Closing Entries analysis tracking
    is_closing_entry = models.BooleanField(default=False, help_text='Flagged as closing entry')
    closing_entry_type = models.CharField(max_length=50, blank=True, null=True, help_text='Type of closing entry')
    closing_entry_risk_score = models.FloatField(default=0.0, help_text='Risk score for closing entry detection (0-100)')
    closing_entry_analysis_details = models.JSONField(default=dict, help_text='Detailed closing entry analysis results')
    
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
    def is_manual_entry(self):
        """Check if transaction is a manual journal entry (high risk for management override)"""
        # Check document type for manual indicators
        if self.document_type:
            manual_indicators = ['MANUAL', 'ADJUSTMENT', 'CORRECTION', 'REVERSAL', 'AJUST']
            if any(indicator in self.document_type.upper() for indicator in manual_indicators):
                return True
        
        # Check text for manual indicators
        if self.text:
            manual_text_indicators = ['manual', 'adjustment', 'correction', 'reversal', 'ajust', 'manual entry']
            if any(indicator in self.text.lower() for indicator in manual_text_indicators):
                return True
        
        # Check for specific account types that are commonly manual
        manual_accounts = ['999999', '888888', '777777', '666666']  # Common manual entry accounts
        if self.gl_account in manual_accounts:
            return True
        
        return False
    
    @property
    def is_period_end_adjustment(self):
        """Check if transaction is a period-end adjustment (high risk for management override)"""
        if not self.posting_date:
            return False
        
        # Check if posting is in last 3 days of month or first 3 days of next month
        day_of_month = self.posting_date.day
        if day_of_month >= 28 or day_of_month <= 3:
            return True
        
        # Check if posting is in last month of fiscal year
        if self.posting_period in [12, 16]:  # December or last period
            return True
        
        return False
    
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
    
    def get_proper_transaction_type(self):
        """Get the proper transaction type considering account type and amount sign"""
        if not self.gl_account:
            return self.transaction_type
        
        amount = self.amount_local_currency
        
        # Asset accounts (1xxx) - normally have debit balances
        if self.gl_account.startswith('1'):
            return 'DEBIT' if amount >= 0 else 'CREDIT'
        
        # Liability accounts (2xxx) - normally have credit balances
        elif self.gl_account.startswith('2'):
            return 'CREDIT' if amount >= 0 else 'DEBIT'
        
        # Equity accounts (3xxx) - normally have credit balances
        elif self.gl_account.startswith('3'):
            return 'CREDIT' if amount >= 0 else 'DEBIT'
        
        # Revenue accounts (4xxx) - normally have credit balances
        elif self.gl_account.startswith('4'):
            return 'CREDIT' if amount >= 0 else 'DEBIT'
        
        # Expense accounts (5xxx, 6xxx, 7xxx) - normally have debit balances
        elif self.gl_account.startswith(('5', '6', '7')):
            return 'DEBIT' if amount >= 0 else 'CREDIT'
        
        # Default to current transaction type if account pattern not recognized
        return self.transaction_type
    
    def save(self, *args, **kwargs):
        # Auto-determine transaction type based on GL account AND amount
        if not self.transaction_type and self.gl_account:
            amount = self.amount_local_currency
            
            # Asset and expense accounts (1xxx, 5xxx, 6xxx, 7xxx)
            if self.gl_account.startswith(('1', '5', '6', '7')):
                # Positive amount = DEBIT (normal balance), Negative amount = CREDIT (opposite)
                self.transaction_type = 'DEBIT' if amount >= 0 else 'CREDIT'
            # Liability, equity, and revenue accounts (2xxx, 3xxx, 4xxx)
            elif self.gl_account.startswith(('2', '3', '4')):
                # Positive amount = CREDIT (normal balance), Negative amount = DEBIT (opposite)
                self.transaction_type = 'CREDIT' if amount >= 0 else 'DEBIT'
        
        super().save(*args, **kwargs)
    
    def recalculate_transaction_type(self):
        """Recalculate and update transaction type based on current account and amount"""
        proper_type = self.get_proper_transaction_type()
        if proper_type != self.transaction_type:
            self.transaction_type = proper_type
            self.save(update_fields=['transaction_type'])
        return self.transaction_type
    
    @classmethod
    def recalculate_all_transaction_types(cls):
        """Recalculate transaction types for all records"""
        updated_count = 0
        for posting in cls.objects.all():
            if posting.recalculate_transaction_type():
                updated_count += 1
        return updated_count

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

class DuplicateAnalysisResult(BaseAnalysisResult):
    """Enhanced duplicate analysis results with unified structure"""
    
    # Additional duplicate-specific fields
    duplicate_list = models.JSONField(default=list, help_text='List of duplicate transactions found')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns (by type, user, account, etc.)')
    detailed_insights = models.JSONField(default=dict, help_text='Detailed insights and recommendations')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    
    class Meta:
        db_table = 'duplicate_analysis_results'
    
    def get_duplicate_count(self):
        """Get the total number of duplicates found"""
        return len(self.duplicate_list)
    
    def get_total_amount(self):
        """Get the total amount involved in duplicates"""
        total = 0
        for item in self.duplicate_list:
            if 'transaction1' in item and 'amount' in item['transaction1']:
                total += item['transaction1']['amount']
            elif 'amount' in item:
                total += item['amount']
        return total

class BackdatedAnalysisResult(BaseAnalysisResult):
    """Enhanced backdated analysis results with unified structure"""
    
    # Additional backdated-specific fields
    backdated_entries = models.JSONField(default=list, help_text='List of backdated transactions found')
    backdated_by_document = models.JSONField(default=list, help_text='Backdated entries grouped by document number')
    backdated_by_account = models.JSONField(default=list, help_text='Backdated entries grouped by account')
    backdated_by_user = models.JSONField(default=list, help_text='Backdated entries grouped by user')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns including ML insights')
    
    class Meta:
        db_table = 'backdated_analysis_results'
    
    def get_backdated_count(self):
        """Get count of backdated entries"""
        return len(self.backdated_entries) if self.backdated_entries else 0

class UserAnalysisResult(BaseAnalysisResult):
    """Enhanced user analysis results with unified structure"""
    
    # Additional user-specific fields
    user_transaction_summary = models.JSONField(default=list, help_text='Summary of transactions per user')
    user_debit_analysis = models.JSONField(default=list, help_text='Debit value analysis per user')
    user_account_distribution = models.JSONField(default=list, help_text='Number of unique users per account')
    user_fs_line_distribution = models.JSONField(default=list, help_text='Number of unique users per FS line')
    user_anomalies = models.JSONField(default=list, help_text='List of user anomalies detected')
    user_risk_assessment = models.JSONField(default=dict, help_text='User risk assessment and scoring')
    user_patterns = models.JSONField(default=dict, help_text='User activity patterns and trends')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns including ML insights')
    
    class Meta:
        db_table = 'user_analysis_results'
    
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
        
        if isinstance(self.user_risk_assessment, list):
            return len([user for user in self.user_risk_assessment 
                       if isinstance(user, dict) and user.get('risk_level', 'low') in ['high', 'critical']])
        
        if isinstance(self.user_risk_assessment, dict):
            return len([user for user in self.user_risk_assessment.get('user_risk_scores', []) 
                       if isinstance(user, dict) and user.get('risk_level', 'low') in ['high', 'critical']])
        
        return 0

class UnusualDaysAnalysisResult(BaseAnalysisResult):
    """Enhanced unusual days analysis results with unified structure"""
    
    # Additional unusual days-specific fields
    weekend_postings = models.JSONField(default=list, help_text='List of weekend postings (Friday/Saturday)')
    day_of_week_activity = models.JSONField(default=dict, help_text='Activity patterns by day of week')
    user_day_patterns = models.JSONField(default=list, help_text='User posting patterns by day of week')
    fs_line_day_patterns = models.JSONField(default=list, help_text='FS line activity by day of week')
    unusual_days = models.JSONField(default=list, help_text='List of unusual day patterns detected')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns including ML insights')
    
    class Meta:
        db_table = 'unusual_days_analysis_results'
    
    def get_weekend_transactions_count(self):
        """Get count of weekend transactions"""
        return len(self.weekend_postings) if self.weekend_postings else 0
    
    def get_unusual_days_count(self):
        """Get count of unusual days"""
        return len(self.unusual_days) if self.unusual_days else 0

class ClosingEntriesAnalysisResult(BaseAnalysisResult):
    """Enhanced closing entries analysis results with unified structure"""
    
    # Additional closing entries-specific fields
    closing_entries = models.JSONField(default=list, help_text='List of closing entries detected')
    post_close_entries = models.JSONField(default=list, help_text='List of post-close entries detected')
    closing_patterns = models.JSONField(default=dict, help_text='Closing entry patterns and trends')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns including ML insights')
    
    # Additional fields referenced in views
    post_close_analysis = models.JSONField(default=dict, help_text='Post-close analysis data')
    fs_line_closing = models.JSONField(default=dict, help_text='Financial statement line closing data')
    user_closing = models.JSONField(default=dict, help_text='User closing patterns')
    month_end_patterns = models.JSONField(default=dict, help_text='Month-end closing patterns')
    closing_window_analysis = models.JSONField(default=dict, help_text='Closing window analysis data')
    
    class Meta:
        db_table = 'closing_entries_analysis_results'
    
    def get_closing_entries_count(self):
        """Get count of closing entries"""
        return len(self.closing_entries) if self.closing_entries else 0
    
    def get_post_close_entries_count(self):
        """Get count of post-close entries"""
        return len(self.post_close_entries) if self.post_close_entries else 0
    
    def get_total_transactions(self):
        """Get total number of transactions"""
        if not self.analysis_summary:
            return 0
        return self.analysis_summary.get('total_transactions', 0)
    
    def get_closing_entries_risk_score(self):
        """Get risk score for closing entries"""
        if not self.closing_entries:
            return 0.0
        # Calculate risk based on closing entries
        high_value_count = sum(1 for entry in self.closing_entries 
                              if float(entry.get('amount', 0)) > 1000000)
        return min(100.0, len(self.closing_entries) * 5 + high_value_count * 10)
    
    def get_post_close_risk_score(self):
        """Get risk score for post-close entries"""
        if not self.post_close_entries:
            return 0.0
        # Calculate risk based on post-close entries
        high_value_count = sum(1 for entry in self.post_close_entries 
                              if float(entry.get('amount', 0)) > 1000000)
        return min(100.0, len(self.post_close_entries) * 8 + high_value_count * 15)
    
    def get_high_value_post_close_risk_score(self):
        """Get risk score for high-value post-close entries"""
        if not self.post_close_entries:
            return 0.0
        # Calculate risk based on high-value post-close entries
        high_value_entries = [entry for entry in self.post_close_entries 
                             if float(entry.get('amount', 0)) > 1000000]
        return min(100.0, len(high_value_entries) * 20)

class HolidayAnalysisResult(BaseAnalysisResult):
    """Enhanced holiday analysis results with unified structure"""
    
    # Additional holiday-specific fields
    holiday_postings = models.JSONField(default=list, help_text='List of holiday postings detected')
    holiday_by_fs_line = models.JSONField(default=list, help_text='Holiday postings grouped by financial statement line')
    holiday_by_account = models.JSONField(default=list, help_text='Holiday postings grouped by account')
    holiday_by_user = models.JSONField(default=list, help_text='Holiday postings grouped by user')
    holiday_by_holiday_type = models.JSONField(default=list, help_text='Holiday postings grouped by holiday type')
    gl_activity_by_holiday = models.JSONField(default=list, help_text='GL activity by holiday')
    holiday_breakdown = models.JSONField(default=list, help_text='Breakdown by holiday type')
    holiday_patterns = models.JSONField(default=dict, help_text='Holiday posting patterns and trends')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    breakdowns = models.JSONField(default=dict, help_text='Various breakdowns including ML insights')
    
    class Meta:
        db_table = 'holiday_analysis_results'
    
    def get_holiday_postings_count(self):
        """Get count of holiday postings"""
        return len(self.holiday_postings) if self.holiday_postings else 0
    
    def get_holiday_percentage(self):
        """Get percentage of holiday postings"""
        if not self.analysis_summary:
            return 0
        return self.analysis_summary.get('holiday_percentage', 0)
    
    def get_unique_holidays(self):
        """Get count of unique holidays"""
        if not self.holiday_breakdown:
            return 0
        return len([h for h in self.holiday_breakdown if h and len(h) > 1 and h[1] is not None and h[1] > 0])
    
    def get_overall_risk_score(self):
        """Get overall risk score"""
        if not self.risk_assessment:
            return 0
        return self.risk_assessment.get('overall_risk_score', 0)

class GeneralAnalysisResult(BaseAnalysisResult):
    """Enhanced general analysis results with unified structure"""
    
    # Additional general-specific fields
    trial_balance_summary = models.JSONField(default=dict, help_text='Trial balance summary')
    gl_account_summaries = models.JSONField(default=list, help_text='GL account summaries')
    user_summaries = models.JSONField(default=list, help_text='User summaries')
    statistical_calculations = models.JSONField(default=dict, help_text='Statistical calculations')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    
    class Meta:
        db_table = 'general_analysis_results'
    
    def get_gl_account_summaries_count(self):
        """Get count of GL account summaries"""
        return len(self.gl_account_summaries) if self.gl_account_summaries else 0
    
    def get_user_summaries_count(self):
        """Get count of user summaries"""
        return len(self.user_summaries) if self.user_summaries else 0

class MLModelTraining(BaseModelTraining):
    """Model to track ML model training sessions and performance"""
    
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



class AnalyticsProcessingResult(BaseProcessingResult):
    """Model to store comprehensive analytics processing results"""
    
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

class ProcessingJobTracker(BaseProcessingResult):
    """Model to track overall processing job progress and status"""
    
    # Job references (override the base field to make it OneToOne)
    processing_job = models.OneToOneField('FileProcessingJob', on_delete=models.CASCADE, related_name='job_tracker', help_text='Reference to the processing job')
    
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
    
    # Additional timestamps
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



class OverallAnalysisResult(BaseAnalysisResult):
    """Model to store overall analysis results combining all analysis types with risk calculations"""
    
    # Additional overall-specific fields
    transaction_summary = models.JSONField(default=dict, help_text='Overall transaction summary statistics')
    flagged_transactions = models.JSONField(default=list, help_text='List of all flagged transactions with their flag types')
    flag_summary = models.JSONField(default=dict, help_text='Summary of flags by type (duplicate, backdated, etc.)')
    expense_analysis = models.JSONField(default=dict, help_text='Expense data analysis and categorization')
    financial_statement_impact = models.JSONField(default=dict, help_text='Financial statement impact analysis')
    
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

class RiskScoringDocument(BaseAnalysisResult):
    """Model to store comprehensive risk scoring documentation and methodology"""
    
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
    critical_risk_transactions = models.IntegerField(default=0, help_text='Number of critical-risk transactions')
    overall_risk_score = models.FloatField(default=0.0, help_text='Overall risk score for the dataset')
    
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
            'critical_risk': self.critical_risk_transactions,
            'total': self.total_transactions,
            'high_risk_percentage': (self.high_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'medium_risk_percentage': (self.medium_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'low_risk_percentage': (self.low_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'critical_risk_percentage': (self.critical_risk_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
        }
    
    def get_risk_level(self):
        """Get overall risk level based on overall risk score"""
        if self.overall_risk_score >= 80:
            return 'CRITICAL'
        elif self.overall_risk_score >= 60:
            return 'HIGH'
        elif self.overall_risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'

class RuleBasedModelTraining(BaseModelTraining):
    """Model to store Rule-based Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='rule_based', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for each rule type')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'rule_based_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"
    
    def get_training_summary(self):
        """Get a summary of the training results"""
        return {
            'session_name': self.session_name,
            'model_type': self.model_type,
            'training_data_size': self.training_data_size,
            'training_duration': self.training_duration,
            'status': self.status,
            'models_trained': len(self.training_results) if self.training_results else 0,
            'performance_metrics': self.performance_metrics
        }
    
    def get_rule_thresholds(self):
        """Get optimized rule thresholds from training results"""
        thresholds = {}
        
        if self.training_results:
            # Extract thresholds from each rule type
            for rule_type, results in self.training_results.items():
                if 'optimal_thresholds' in results:
                    thresholds[rule_type] = results['optimal_thresholds']
        
        return thresholds
    
    def get_training_accuracy(self):
        """Get overall training accuracy"""
        if self.performance_metrics and 'training_accuracy' in self.performance_metrics:
            return self.performance_metrics['training_accuracy']
        return 0.0

class DuplicateAnalysisModelTraining(BaseModelTraining):
    """Model to store Duplicate Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='duplicate_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for duplicate detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'duplicate_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"
    
    def get_training_summary(self):
        """Get a summary of the training results"""
        return {
            'session_name': self.session_name,
            'model_type': self.model_type,
            'training_data_size': self.training_data_size,
            'training_duration': self.training_duration,
            'status': self.status,
            'performance_metrics': self.performance_metrics
        }

class BackdatedAnalysisModelTraining(BaseModelTraining):
    """Model to store Backdated Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='backdated_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for backdated detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'backdated_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class UserAnalysisModelTraining(BaseModelTraining):
    """Model to store User Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='user_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for user anomaly detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'user_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class UnusualDaysAnalysisModelTraining(BaseModelTraining):
    """Model to store Unusual Days Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='unusual_days_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for unusual days detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'unusual_days_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class ClosingEntriesAnalysisModelTraining(BaseModelTraining):
    """Model to store Closing Entries Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='closing_entries_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for closing entries detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'closing_entries_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class HolidayAnalysisModelTraining(BaseModelTraining):
    """Model to store Holiday Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='holiday_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for holiday detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'holiday_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

class OverallRiskAnalysisModelTraining(BaseModelTraining):
    """Model to store Overall Risk Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='overall_risk_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for overall risk analysis')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'overall_risk_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"

# ============================================================================
# ENHANCED AI RISK ASSESSMENT MODELS
# ============================================================================

class AIRiskAssessment(BaseAnalysisResult):
    """
    AI-powered comprehensive risk assessment model
    Stores advanced ML-based risk analysis results
    """
    
    analysis_type = 'ai_risk_assessment'
    
    # AI Assessment metadata
    ai_model_version = models.CharField(max_length=20, default='2.0.0')
    
    # AI Risk Scores
    overall_ai_risk_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(100.0)],
        help_text='AI-calculated overall risk score (0-100)'
    )
    ai_confidence_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='AI model confidence in predictions (0-1)'
    )
    
    # Risk Level Classification
    ai_risk_level = models.CharField(max_length=20, choices=[
        ('LOW', 'Low Risk'),
        ('MEDIUM', 'Medium Risk'),
        ('HIGH', 'High Risk'),
        ('CRITICAL', 'Critical Risk'),
    ])
    
    # ML Model Results
    ml_predictions = models.JSONField(default=dict, help_text='Raw ML model predictions')
    feature_importance = models.JSONField(default=dict, help_text='Feature importance scores')
    model_performance = models.JSONField(default=dict, help_text='Model performance metrics')
    
    # Anomaly Clustering
    anomaly_clusters = models.JSONField(default=dict, help_text='Anomaly clustering results')
    cluster_analysis = models.JSONField(default=dict, help_text='Detailed cluster analysis')
    
    # NLP Insights
    nlp_insights = models.JSONField(default=dict, help_text='NLP-generated insights')
    risk_patterns = models.JSONField(default=dict, help_text='Identified risk patterns')
    trend_analysis = models.JSONField(default=dict, help_text='Risk trend analysis')
    
    # AI Recommendations
    ai_recommendations = models.JSONField(default=dict, help_text='AI-generated recommendations')
    immediate_actions = models.JSONField(default=list, help_text='Immediate action items')
    investigation_priorities = models.JSONField(default=list, help_text='Investigation priorities')
    audit_procedures = models.JSONField(default=list, help_text='Recommended audit procedures')
    risk_mitigation = models.JSONField(default=list, help_text='Risk mitigation strategies')
    
    # Processing metadata
    models_used = models.JSONField(default=list, help_text='List of ML models used')
    
    class Meta:
        db_table = 'ai_risk_assessments'
        verbose_name = 'AI Risk Assessment'
        verbose_name_plural = 'AI Risk Assessments'
    
    def __str__(self):
        return f"AI Risk Assessment for {self.data_file.file_name} - {self.ai_risk_level}"
    
    def get_risk_summary(self):
        """Get a summary of the AI risk assessment"""
        return {
            'assessment_id': str(self.id),
            'file_name': self.data_file.file_name,
            'ai_risk_score': self.overall_ai_risk_score,
            'ai_risk_level': self.ai_risk_level,
            'confidence_score': self.ai_confidence_score,
            'assessment_date': self.analysis_date.isoformat(),
            'status': self.status,
        }
    
    def get_key_recommendations(self, limit=5):
        """Get key AI recommendations"""
        recommendations = self.ai_recommendations.get('immediate_actions', [])
        return recommendations[:limit]
    
    def get_investigation_priorities(self, limit=5):
        """Get investigation priorities"""
        priorities = self.investigation_priorities
        return priorities[:limit]


class RiskPattern(models.Model):
    """
    Model to store identified risk patterns and their characteristics
    """
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Pattern identification
    pattern_name = models.CharField(max_length=255, help_text='Name of the risk pattern')
    pattern_type = models.CharField(max_length=100, help_text='Type of risk pattern')
    pattern_description = models.TextField(help_text='Description of the pattern')
    
    # Pattern characteristics
    pattern_indicators = models.JSONField(default=list, help_text='Indicators that define this pattern')
    pattern_frequency = models.IntegerField(default=0, help_text='How often this pattern occurs')
    pattern_severity = models.CharField(max_length=20, choices=[
        ('LOW', 'Low Severity'),
        ('MEDIUM', 'Medium Severity'),
        ('HIGH', 'High Severity'),
        ('CRITICAL', 'Critical Severity'),
    ])
    
    # Pattern analysis
    affected_transactions = models.JSONField(default=list, help_text='Transaction IDs affected by this pattern')
    pattern_risk_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(100.0)],
        help_text='Risk score for this pattern'
    )
    pattern_confidence = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Confidence in pattern detection'
    )
    
    # Pattern metadata
    first_detected = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True, help_text='Whether this pattern is currently active')
    
    # File association
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='risk_patterns')
    ai_assessment = models.ForeignKey('AIRiskAssessment', on_delete=models.CASCADE, related_name='patterns')
    
    class Meta:
        db_table = 'risk_patterns'
        verbose_name = 'Risk Pattern'
        verbose_name_plural = 'Risk Patterns'
        ordering = ['-pattern_risk_score', '-first_detected']
        indexes = [
            models.Index(fields=['pattern_type', 'pattern_severity']),
            models.Index(fields=['pattern_risk_score', 'first_detected']),
            models.Index(fields=['data_file', 'is_active']),
        ]
    
    def __str__(self):
        return f"{self.pattern_name} - {self.pattern_severity}"


class AnomalyCluster(models.Model):
    """
    Model to store anomaly clusters identified by AI
    """
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Cluster identification
    cluster_id = models.CharField(max_length=50, help_text='Unique cluster identifier')
    cluster_name = models.CharField(max_length=255, help_text='Descriptive name for the cluster')
    cluster_type = models.CharField(max_length=100, help_text='Type of anomaly cluster')
    
    # Cluster characteristics
    cluster_size = models.IntegerField(help_text='Number of transactions in this cluster')
    cluster_characteristics = models.JSONField(default=dict, help_text='Characteristics of this cluster')
    cluster_anomaly_profile = models.JSONField(default=dict, help_text='Anomaly profile for this cluster')
    
    # Cluster analysis
    cluster_risk_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(100.0)],
        help_text='Overall risk score for this cluster'
    )
    cluster_confidence = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Confidence in cluster analysis'
    )
    
    # Cluster transactions
    transaction_ids = models.JSONField(default=list, help_text='List of transaction IDs in this cluster')
    representative_transactions = models.JSONField(default=list, help_text='Representative transactions for this cluster')
    
    # Cluster metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Associations
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='anomaly_clusters')
    ai_assessment = models.ForeignKey('AIRiskAssessment', on_delete=models.CASCADE, related_name='clusters')
    
    class Meta:
        db_table = 'anomaly_clusters'
        verbose_name = 'Anomaly Cluster'
        verbose_name_plural = 'Anomaly Clusters'
        ordering = ['-cluster_risk_score', '-cluster_size']
        indexes = [
            models.Index(fields=['cluster_type', 'cluster_risk_score']),
            models.Index(fields=['cluster_size', 'created_at']),
            models.Index(fields=['data_file', 'cluster_type']),
        ]
    
    def __str__(self):
        return f"Cluster {self.cluster_id} - {self.cluster_size} transactions"


class AIRiskRecommendation(models.Model):
    """
    Model to store AI-generated risk recommendations
    """
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Recommendation details
    recommendation_title = models.CharField(max_length=255, help_text='Title of the recommendation')
    recommendation_type = models.CharField(max_length=100, help_text='Type of recommendation')
    recommendation_description = models.TextField(help_text='Detailed description of the recommendation')
    
    # Recommendation categorization
    RECOMMENDATION_CATEGORIES = [
        ('IMMEDIATE_ACTION', 'Immediate Action'),
        ('INVESTIGATION', 'Investigation'),
        ('AUDIT_PROCEDURE', 'Audit Procedure'),
        ('RISK_MITIGATION', 'Risk Mitigation'),
        ('COMPLIANCE', 'Compliance'),
        ('MONITORING', 'Monitoring'),
        ('CONTROL_IMPROVEMENT', 'Control Improvement'),
    ]
    category = models.CharField(max_length=50, choices=RECOMMENDATION_CATEGORIES)
    
    # Priority and urgency
    PRIORITY_LEVELS = [
        ('LOW', 'Low Priority'),
        ('MEDIUM', 'Medium Priority'),
        ('HIGH', 'High Priority'),
        ('CRITICAL', 'Critical Priority'),
    ]
    priority = models.CharField(max_length=20, choices=PRIORITY_LEVELS)
    
    URGENCY_LEVELS = [
        ('LOW', 'Low Urgency'),
        ('MEDIUM', 'Medium Urgency'),
        ('HIGH', 'High Urgency'),
        ('IMMEDIATE', 'Immediate'),
    ]
    urgency = models.CharField(max_length=20, choices=URGENCY_LEVELS)
    
    # Recommendation details
    affected_transactions = models.JSONField(default=list, help_text='Transaction IDs affected by this recommendation')
    estimated_impact = models.CharField(max_length=100, help_text='Estimated impact of implementing this recommendation')
    implementation_effort = models.CharField(max_length=100, help_text='Estimated effort to implement')
    
    # AI confidence
    ai_confidence = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='AI confidence in this recommendation'
    )
    
    # Status tracking
    RECOMMENDATION_STATUS = [
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('IMPLEMENTED', 'Implemented'),
        ('REJECTED', 'Rejected'),
        ('ON_HOLD', 'On Hold'),
    ]
    status = models.CharField(max_length=20, choices=RECOMMENDATION_STATUS, default='PENDING')
    
    # Implementation tracking
    assigned_to = models.CharField(max_length=100, blank=True, null=True, help_text='Person assigned to implement')
    due_date = models.DateField(blank=True, null=True, help_text='Due date for implementation')
    implemented_date = models.DateField(blank=True, null=True, help_text='Date when implemented')
    implementation_notes = models.TextField(blank=True, null=True, help_text='Notes about implementation')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Associations
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='ai_recommendations')
    ai_assessment = models.ForeignKey('AIRiskAssessment', on_delete=models.CASCADE, related_name='recommendations')
    
    class Meta:
        db_table = 'ai_risk_recommendations'
        verbose_name = 'AI Risk Recommendation'
        verbose_name_plural = 'AI Risk Recommendations'
        ordering = ['-priority', '-urgency', '-created_at']
        indexes = [
            models.Index(fields=['category', 'priority']),
            models.Index(fields=['status', 'due_date']),
            models.Index(fields=['data_file', 'category']),
        ]
    
    def __str__(self):
        return f"{self.recommendation_title} - {self.priority}"


class RiskTrend(models.Model):
    """
    Model to store risk trends and patterns over time
    """
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Trend identification
    trend_name = models.CharField(max_length=255, help_text='Name of the risk trend')
    trend_type = models.CharField(max_length=100, help_text='Type of risk trend')
    trend_period = models.CharField(max_length=50, help_text='Period for trend analysis (daily, weekly, monthly)')
    
    # Trend data
    trend_data = models.JSONField(default=dict, help_text='Trend data points')
    trend_direction = models.CharField(max_length=20, choices=[
        ('INCREASING', 'Increasing'),
        ('DECREASING', 'Decreasing'),
        ('STABLE', 'Stable'),
        ('VOLATILE', 'Volatile'),
    ])
    
    # Trend analysis
    trend_significance = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Statistical significance of the trend'
    )
    trend_confidence = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Confidence in trend analysis'
    )
    
    # Trend metadata
    start_date = models.DateField(help_text='Start date of trend period')
    end_date = models.DateField(help_text='End date of trend period')
    created_at = models.DateTimeField(auto_now_add=True)
    
    # Associations
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='risk_trends')
    ai_assessment = models.ForeignKey('AIRiskAssessment', on_delete=models.CASCADE, related_name='trends')
    
    class Meta:
        db_table = 'risk_trends'
        verbose_name = 'Risk Trend'
        verbose_name_plural = 'Risk Trends'
        ordering = ['-end_date', '-trend_significance']
        indexes = [
            models.Index(fields=['trend_type', 'trend_direction']),
            models.Index(fields=['start_date', 'end_date']),
            models.Index(fields=['data_file', 'trend_type']),
        ]
    
    def __str__(self):
        return f"{self.trend_name} - {self.trend_direction}"


class ModelPerformance(models.Model):
    """
    Model to track ML model performance and accuracy
    """
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Model identification
    model_name = models.CharField(max_length=100, help_text='Name of the ML model')
    model_version = models.CharField(max_length=20, help_text='Version of the model')
    model_type = models.CharField(max_length=50, help_text='Type of model (classification, regression, clustering)')
    
    # Performance metrics
    accuracy = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        null=True, blank=True,
        help_text='Model accuracy score'
    )
    precision = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        null=True, blank=True,
        help_text='Model precision score'
    )
    recall = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        null=True, blank=True,
        help_text='Model recall score'
    )
    f1_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        null=True, blank=True,
        help_text='Model F1 score'
    )
    
    # Additional metrics
    performance_metrics = models.JSONField(default=dict, help_text='Additional performance metrics')
    confusion_matrix = models.JSONField(default=dict, help_text='Confusion matrix for classification models')
    
    # Training information
    training_data_size = models.IntegerField(help_text='Size of training dataset')
    training_duration = models.FloatField(help_text='Training duration in seconds')
    training_timestamp = models.DateTimeField(help_text='When the model was trained')
    
    # Model metadata
    created_at = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True, help_text='Whether this model is currently active')
    
    # Associations
    data_file = models.ForeignKey('DataFile', on_delete=models.CASCADE, related_name='model_performances')
    ai_assessment = models.ForeignKey('AIRiskAssessment', on_delete=models.CASCADE, related_name='model_performances')
    
    class Meta:
        db_table = 'model_performances'
        verbose_name = 'Model Performance'
        verbose_name_plural = 'Model Performances'
        ordering = ['-training_timestamp', '-accuracy']
        indexes = [
            models.Index(fields=['model_name', 'model_version']),
            models.Index(fields=['accuracy', 'training_timestamp']),
            models.Index(fields=['data_file', 'is_active']),
        ]
    
    def __str__(self):
        return f"{self.model_name} v{self.model_version} - {self.accuracy:.3f} accuracy"


class ManualEntryAnalysisResult(BaseAnalysisResult):
    """Results from manual entry analysis (High Risk for Management Override)"""
    
    analysis_type = 'manual_entry_analysis'
    
    # Manual entry-specific fields
    manual_entries = models.JSONField(default=list, help_text='List of manual journal entries detected')
    manual_entry_risk_distribution = models.JSONField(default=dict, help_text='Risk distribution by manual entry type')
    manual_entry_amount_analysis = models.JSONField(default=dict, help_text='Amount analysis for manual entries')
    manual_entry_user_analysis = models.JSONField(default=dict, help_text='User analysis for manual entries')
    manual_entry_account_analysis = models.JSONField(default=dict, help_text='Account analysis for manual entries')
    period_end_adjustments = models.JSONField(default=list, help_text='Period-end adjustment entries')
    management_override_indicators = models.JSONField(default=list, help_text='Specific indicators of potential management override')
    
    class Meta:
        db_table = 'manual_entry_analysis_results'
        verbose_name = 'Manual Entry Analysis Result'
        verbose_name_plural = 'Manual Entry Analysis Results'

class ManualEntryAnalysisModelTraining(BaseModelTraining):
    """Model to store Manual Entry Analysis Model Training results"""
    
    # Training session information
    session_name = models.CharField(max_length=255, help_text='Name of the training session')
    description = models.TextField(blank=True, help_text='Description of the training session')
    model_type = models.CharField(max_length=50, default='manual_entry_analysis', help_text='Type of model trained')
    
    # Training data information
    training_data_size = models.IntegerField(default=0, help_text='Number of transactions used for training')
    training_data_date_range = models.JSONField(default=dict, help_text='Date range of training data')
    
    # Training results
    training_results = models.JSONField(default=dict, help_text='Detailed training results for manual entry detection')
    performance_metrics = models.JSONField(default=dict, help_text='Performance metrics from training')
    
    # Training metadata
    started_at = models.DateTimeField(auto_now_add=True, help_text='When training started')
    completed_at = models.DateTimeField(null=True, blank=True, help_text='When training completed')
    training_duration = models.FloatField(default=0.0, help_text='Training duration in seconds')
    
    # Status
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending'),
        ('TRAINING', 'Training'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed')
    ], default='PENDING', help_text='Training status')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if training failed')
    
    class Meta:
        db_table = 'manual_entry_analysis_model_training'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.session_name} - {self.status}"


class FileProcessingTask(models.Model):
    """Model to track individual file processing tasks (GL, TB, Chart of Accounts)"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File information
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='processing_tasks', help_text='Reference to the uploaded data file')
    
    # Task information
    TASK_TYPE_CHOICES = [
        ('GL_LIST', 'General Ledger List'),
        ('TB_LIST', 'Trial Balance List'),
        ('CHART_OF_ACCOUNTS', 'Chart of Accounts'),
    ]
    task_type = models.CharField(max_length=20, choices=TASK_TYPE_CHOICES, help_text='Type of processing task')
    
    # Processing status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('SUCCESS', 'Success'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Celery task information
    celery_task_id = models.CharField(max_length=255, null=True, blank=True, help_text='Celery task ID for tracking')
    
    # Processing results
    extracted_data = models.JSONField(default=dict, help_text='Extracted and processed data from the file')
    processing_metadata = models.JSONField(default=dict, help_text='Metadata about the processing (row counts, etc.)')
    
    # Error handling
    error_message = models.TextField(blank=True, null=True, help_text='Error message if processing failed')
    
    # Timestamps
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'file_processing_tasks'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_file', 'task_type']),
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
        ]
        unique_together = ['data_file', 'task_type']
    
    def __str__(self):
        return f"{self.data_file.filename} - {self.get_task_type_display()} - {self.status}"


class CompletenessJob(models.Model):
    """Model to track completeness job execution after all file processing tasks complete"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # File information
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='completeness_jobs', help_text='Reference to the uploaded data file')
    
    # Related processing tasks
    gl_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='gl_completeness_jobs', null=True, blank=True)
    tb_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='tb_completeness_jobs', null=True, blank=True)
    chart_task = models.ForeignKey(FileProcessingTask, on_delete=models.CASCADE, related_name='chart_completeness_jobs', null=True, blank=True)
    
    # Processing status
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('CREATED', 'Created'),
        ('SUCCESS', 'Success'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Celery task information
    celery_task_id = models.CharField(max_length=255, null=True, blank=True, help_text='Celery task ID for tracking')
    
    # Completeness results
    completeness_results = models.JSONField(default=dict, help_text='Results from completeness analysis')
    debit_credit_summary = models.JSONField(default=dict, help_text='Summary of debit and credit entries')
    audit_checks = models.JSONField(default=dict, help_text='Results of audit-style completeness checks')
    
    # Error handling
    error_message = models.TextField(blank=True, null=True, help_text='Error message if processing failed')
    
    # Timestamps
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'completeness_jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_file']),
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"Completeness Job - {self.data_file.filename} - {self.status}"
