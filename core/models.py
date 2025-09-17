from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from decimal import Decimal, InvalidOperation
import uuid
from django.utils import timezone
from django.core.cache import cache
from django.db.models import JSONField
import json
import re

# ============================================================================
# UTILITY CLASSES FOR DATA PARSING
# ============================================================================

class AmountParser:
    """
    Utility class for parsing string amounts in various formats commonly found in audit data
    
    Handles formats like:
    - "1,234.56" (US format)
    - "1.234,56" (European format)  
    - "1 234.56" (Space thousands separator)
    - "(1,234.56)" (Negative in parentheses)
    - "1,234.56-" (Negative with trailing minus)
    - "SAR 1,234.56" (With currency prefix)
    - "1,234.56 SAR" (With currency suffix)
    - "1234.56" (Plain decimal)
    - "" or "NULL" or "-" (Empty/null values)
    """
    
    @staticmethod
    def clean_amount_string(amount_str):
        """Clean and normalize amount string before parsing"""
        if not amount_str or amount_str in ['', 'NULL', 'null', 'None', 'none', '-', 'N/A', 'n/a']:
            return None
        
        # Convert to string and strip whitespace
        amount_str = str(amount_str).strip()
        
        if not amount_str:
            return None
            
        # Remove currency symbols and text
        currency_patterns = [
            r'SAR\s*',
            r'USD\s*',
            r'EUR\s*',
            r'GBP\s*',
            r'\$\s*',
            r'€\s*',
            r'£\s*',
            r'ريال\s*',
        ]
        
        for pattern in currency_patterns:
            amount_str = re.sub(pattern, '', amount_str, flags=re.IGNORECASE)
        
        return amount_str.strip()
    
    @staticmethod
    def parse_amount(amount_str, default_zero=False):
        """
        Parse amount string to Decimal
        
        Args:
            amount_str: String representation of amount
            default_zero: If True, return 0 for invalid/empty amounts. If False, return None.
        
        Returns:
            Decimal object or None
        """
        try:
            # Clean the amount string
            cleaned = AmountParser.clean_amount_string(amount_str)
            if cleaned is None:
                return Decimal('0') if default_zero else None
            
            # Check for negative indicators
            is_negative = False
            
            # Parentheses indicate negative
            if cleaned.startswith('(') and cleaned.endswith(')'):
                is_negative = True
                cleaned = cleaned[1:-1].strip()
            
            # Trailing minus indicates negative
            elif cleaned.endswith('-'):
                is_negative = True
                cleaned = cleaned[:-1].strip()
            
            # Leading minus
            elif cleaned.startswith('-'):
                is_negative = True
                cleaned = cleaned[1:].strip()
            
            if not cleaned:
                return Decimal('0') if default_zero else None
            
            # Handle different decimal separators
            # First, determine the format by looking at the last separator
            comma_pos = cleaned.rfind(',')
            dot_pos = cleaned.rfind('.')
            
            if comma_pos > dot_pos:
                # European format: 1.234.567,89
                # Replace dots with empty string, comma with dot
                cleaned = cleaned.replace('.', '').replace(',', '.')
            elif dot_pos > comma_pos:
                # US format: 1,234,567.89
                # Remove commas, keep dot
                cleaned = cleaned.replace(',', '')
            else:
                # No separators or only one type
                if ',' in cleaned and '.' not in cleaned:
                    # Only commas - could be thousands or decimal
                    # If more than 3 digits after last comma, treat as thousands
                    parts = cleaned.split(',')
                    if len(parts[-1]) <= 2:
                        # Likely decimal separator
                        cleaned = cleaned.replace(',', '.')
                    else:
                        # Likely thousands separator
                        cleaned = cleaned.replace(',', '')
            
            # Remove any remaining non-numeric characters except decimal point
            cleaned = re.sub(r'[^\d.]', '', cleaned)
            
            if not cleaned:
                return Decimal('0') if default_zero else None
            
            # Convert to Decimal
            result = Decimal(cleaned)
            
            # Apply negative sign
            if is_negative:
                result = -result
            
            return result
            
        except (ValueError, InvalidOperation, Exception) as e:
            # Log the error in a real application
            print(f"Warning: Could not parse amount '{amount_str}': {e}")
            return Decimal('0') if default_zero else None
    
    @staticmethod
    def parse_debit_credit_amounts(amount_str):
        """
        Parse amount and return separate debit/credit values
        
        Returns:
            tuple: (debit_amount, credit_amount) where one is None
        """
        parsed_amount = AmountParser.parse_amount(amount_str)
        
        if parsed_amount is None:
            return None, None
        elif parsed_amount >= 0:
            return parsed_amount, None
        else:
            return None, abs(parsed_amount)
    
    @staticmethod
    def format_amount(amount, currency='SAR', include_currency=True):
        """Format amount for display"""
        if amount is None:
            return ''
        
        amount_str = f"{amount:,.2f}"
        
        if include_currency:
            return f"{amount_str} {currency}"
        
        return amount_str
    
    @staticmethod
    def validate_amount_format(amount_str):
        """
        Validate if amount string can be parsed
        
        Returns:
            tuple: (is_valid, error_message)
        """
        try:
            parsed = AmountParser.parse_amount(amount_str)
            if parsed is None and amount_str and amount_str.strip():
                return False, f"Could not parse amount: '{amount_str}'"
            return True, None
        except Exception as e:
            return False, str(e)


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
# CORE AUDIT STRUCTURE MODELS
# ============================================================================

class Client(BaseModel):
    """Client information for audit engagements"""
    
    # Primary client information
    client_code = models.CharField(max_length=20, db_index=True, help_text='Client identifier code (can be duplicate across different clients)')
    client_name = models.CharField(max_length=255, db_index=True, help_text='Client company name')
    
    # Client organization details
    company_name = models.CharField(max_length=255, help_text='Full legal company name')
    industry = models.CharField(max_length=100, blank=True, help_text='Industry classification')
    country = models.CharField(max_length=50, default='Saudi Arabia', help_text='Country of operation')
    currency = models.CharField(max_length=3, default='SAR', help_text='Primary currency')
    
    # Client contact information
    primary_contact_name = models.CharField(max_length=255, blank=True, help_text='Primary contact person')
    primary_contact_email = models.EmailField(blank=True, help_text='Primary contact email')
    primary_contact_phone = models.CharField(max_length=20, blank=True, help_text='Primary contact phone')
    
    # Client business information
    tax_id = models.CharField(max_length=50, blank=True, help_text='Tax identification number')
    registration_number = models.CharField(max_length=50, blank=True, help_text='Company registration number')
    
    # Audit configuration
    fiscal_year_end = models.CharField(max_length=5, default='12-31', help_text='Fiscal year end (MM-DD format)')
    audit_firm = models.CharField(max_length=255, blank=True, help_text='Audit firm name')
    
    # Status and metadata
    is_active = models.BooleanField(default=True, help_text='Is client active')
    notes = models.TextField(blank=True, help_text='Additional notes about the client')
    
    class Meta:
        db_table = 'audit_clients'
        ordering = ['client_name']
        indexes = [
            models.Index(fields=['client_code']),
            models.Index(fields=['client_name']),
            models.Index(fields=['is_active', 'client_name']),
            models.Index(fields=['industry']),
        ]
    
    def __str__(self):
        return f"{self.client_code} - {self.client_name}"
    
    def get_active_engagements(self):
        """Get all active engagements for this client"""
        return self.engagements.filter(status__in=['ACTIVE', 'IN_PROGRESS', 'COMPLETED'])


class Engagement(BaseModel):
    """Audit engagement - unique per client and fiscal year"""
    
    # Engagement identification
    engagement_id = models.CharField(max_length=100, unique=True, db_index=True, help_text='Unique engagement identifier')
    engagement_name = models.CharField(max_length=255, help_text='Descriptive engagement name')
    
    # Client relationship
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='engagements', help_text='Client for this engagement')
    
    # Fiscal period
    fiscal_year = models.IntegerField(db_index=True, help_text='Fiscal year for the audit')
    fiscal_year_start = models.DateField(help_text='Fiscal year start date')
    fiscal_year_end = models.DateField(help_text='Fiscal year end date')
    
    # Audit period (may differ from fiscal year)
    audit_start_date = models.DateField(help_text='Audit start date')
    audit_end_date = models.DateField(help_text='Audit end date')
    
    # Engagement details
    engagement_type = models.CharField(max_length=50, default='STATUTORY_AUDIT', help_text='Type of engagement')
    audit_opinion = models.CharField(max_length=50, blank=True, help_text='Audit opinion')
    
    # Engagement team
    engagement_partner = models.CharField(max_length=255, blank=True, help_text='Engagement partner')
    engagement_manager = models.CharField(max_length=255, blank=True, help_text='Engagement manager')
    audit_team_members = models.JSONField(default=list, help_text='List of audit team members')
    
    # Status tracking
    STATUS_CHOICES = [
        ('PLANNING', 'Planning'),
        ('ACTIVE', 'Active'),
        ('IN_PROGRESS', 'In Progress'),
        ('FIELDWORK_COMPLETE', 'Fieldwork Complete'),
        ('COMPLETED', 'Completed'),
        ('ARCHIVED', 'Archived'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PLANNING')
    
    # Completeness requirements
    required_files = models.JSONField(default=list, help_text='Required file types for this engagement')
    completeness_threshold = models.FloatField(default=95.0, help_text='Required completeness score threshold')
    
    # Engagement notes and documentation
    description = models.TextField(blank=True, help_text='Engagement description')
    notes = models.TextField(blank=True, help_text='Engagement notes')
    
    class Meta:
        db_table = 'audit_engagements'
        ordering = ['-fiscal_year', 'client__client_name']
        unique_together = [['client', 'fiscal_year']]  # One engagement per client per fiscal year
        indexes = [
            models.Index(fields=['engagement_id']),
            models.Index(fields=['client', 'fiscal_year']),
            models.Index(fields=['status', 'fiscal_year']),
            models.Index(fields=['fiscal_year', 'status']),
        ]
    
    def __str__(self):
        return f"{self.engagement_id} - {self.client.client_name} FY{self.fiscal_year}"
    
    def get_uploaded_file_types(self):
        """Get list of file types already uploaded for this engagement"""
        return list(self.data_files.values_list('file_type', flat=True).distinct())
    
    def is_complete_file_set(self):
        """Check if all required files have been uploaded"""
        uploaded_types = set(self.get_uploaded_file_types())
        required_types = set(self.required_files)
        return required_types.issubset(uploaded_types)
    
    def get_completeness_status(self):
        """Get overall completeness status for the engagement"""
        if not self.is_complete_file_set():
            return 'INCOMPLETE_FILES'
        
        latest_completeness = self.completeness_tests.order_by('-test_timestamp').first()
        if not latest_completeness:
            return 'NO_COMPLETENESS_TEST'
        
        if latest_completeness.completeness_score >= self.completeness_threshold:
            return 'COMPLETE'
        else:
            return 'INCOMPLETE_DATA'


# ============================================================================
# CORE DATA MODELS
# ============================================================================

class DataFile(BaseModel):
    """Model to track uploaded data files with proper engagement linking"""
    
    # File metadata
    file_name = models.CharField(max_length=255, help_text='Original file name')
    file_size = models.BigIntegerField(help_text='File size in bytes')
    file_hash = models.CharField(max_length=64, blank=True, help_text='SHA256 hash of file content for duplicate detection')
    
    # Engagement relationship
    engagement = models.ForeignKey(Engagement, on_delete=models.CASCADE, related_name='data_files', help_text='Engagement this file belongs to')
    
    # File type classification
    FILE_TYPE_CHOICES = [
        ('TB', 'Trial Balance'),
        ('GL', 'General Ledger Listing'),
        ('COA', 'Chart of Accounts'),
        ('OTHER', 'Other Supporting Document'),
    ]
    file_type = models.CharField(max_length=10, choices=FILE_TYPE_CHOICES, help_text='Type of audit file')
    
    # File validation
    is_validated = models.BooleanField(default=False, help_text='Whether file structure has been validated')
    validation_errors = models.JSONField(default=list, help_text='List of validation errors found')
    
    # Legacy fields for backward compatibility (will be populated from engagement)
    legacy_engagement_id = models.CharField(max_length=100, editable=False, help_text='Legacy engagement ID (auto-populated)')
    legacy_client_name = models.CharField(max_length=255, editable=False, help_text='Legacy client name (auto-populated)')
    legacy_company_name = models.CharField(max_length=255, editable=False, help_text='Legacy company name (auto-populated)')
    legacy_fiscal_year = models.IntegerField(editable=False, help_text='Legacy fiscal year (auto-populated)')
    legacy_audit_start_date = models.DateField(editable=False, help_text='Legacy audit start date (auto-populated)')
    legacy_audit_end_date = models.DateField(editable=False, help_text='Legacy audit end date (auto-populated)')
    
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
    min_amount = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True)
    max_amount = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True)
    
    class Meta:
        db_table = 'data_files'
        ordering = ['-uploaded_at']
        indexes = [
            models.Index(fields=['status', 'uploaded_at']),
            models.Index(fields=['engagement', 'file_type']),
            models.Index(fields=['file_type', 'status']),
            models.Index(fields=['legacy_engagement_id', 'legacy_fiscal_year'], name='datafile_legacy_idx'),
        ]
    
    def __str__(self):
        return f"{self.file_name} - {self.engagement.client.client_name} ({self.status})"
    
    def save(self, *args, **kwargs):
        """Auto-populate legacy fields from engagement"""
        if self.engagement:
            self.legacy_engagement_id = self.engagement.engagement_id
            self.legacy_client_name = self.engagement.client.client_name
            self.legacy_company_name = self.engagement.client.company_name
            self.legacy_fiscal_year = self.engagement.fiscal_year
            self.legacy_audit_start_date = self.engagement.audit_start_date
            self.legacy_audit_end_date = self.engagement.audit_end_date
        super().save(*args, **kwargs)

    def get_transaction_document_numbers(self):
        """Get all document numbers for transactions in this file"""
        return list(self.sapglposting_set.values_list('document_number', flat=True).distinct())
    
    def validate_file_structure(self):
        """Validate file structure based on file type"""
        errors = []
        
        if self.file_type == 'TB' and not self.trial_balances.exists():
            errors.append('Trial Balance file must contain trial balance data')
        elif self.file_type == 'GL' and not self.sapglposting_set.exists():
            errors.append('General Ledger file must contain GL posting data')
        elif self.file_type == 'COA' and not self.chart_of_accounts.exists():
            errors.append('Chart of Accounts file must contain COA data')
        
        self.validation_errors = errors
        self.is_validated = len(errors) == 0
        self.save()
        
        return self.is_validated


class GLAccount(BaseModel):
    """Master GL Account table - Links TB, Chart of Accounts, and GL Listing data"""
    
    # Primary account identifier - NO DECIMALS ALLOWED
    account_code = models.CharField(
        max_length=10, 
        db_index=True, 
        help_text='GL Account Code (no decimal or fractional values allowed)',
        validators=[],  # Custom validator will be added below
    )
    
    # Engagement context - accounts are unique per engagement
    engagement = models.ForeignKey(
        Engagement, 
        on_delete=models.CASCADE, 
        related_name='accounts',
        help_text='Engagement this account belongs to'
    )
    
    # Account details
    account_name = models.CharField(max_length=200, help_text='GL Account Name')
    account_description = models.TextField(blank=True, help_text='Detailed account description')
    
    # Company and organizational info
    company_code = models.CharField(max_length=10, blank=True, help_text='Company Code')
    profit_center = models.CharField(max_length=10, blank=True, help_text='Profit Center')
    cost_center = models.CharField(max_length=10, blank=True, help_text='Cost Center')
    
    # Account properties
    is_active = models.BooleanField(default=True, help_text='Is Account Active')
    currency = models.CharField(max_length=3, default='SAR', help_text='Account Currency')
    
    # Chart of Accounts hierarchical information (pulled from COA)
    account_type = models.CharField(max_length=50, blank=True, help_text='Account Type from COA hierarchy')
    sub_type = models.CharField(max_length=50, blank=True, help_text='Sub Type from COA hierarchy')
    sub_sub_type = models.CharField(max_length=50, blank=True, help_text='Sub Sub Type from COA hierarchy')
    
    # Financial statement classification
    financial_statement_category = models.CharField(max_length=50, blank=True, help_text='Financial Statement Category')
    balance_sheet_category = models.CharField(max_length=50, blank=True, help_text='Balance Sheet Category')
    income_statement_category = models.CharField(max_length=50, blank=True, help_text='Income Statement Category')
    
    # Trial Balance information
    opening_balance = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Opening Balance from TB')
    closing_balance = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Closing Balance from TB')
    tb_debit = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Debit Amount from TB (optional)')
    tb_credit = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Credit Amount from TB (optional)')
    
    # Legacy fields for backward compatibility
    legacy_engagement_id = models.CharField(max_length=100, editable=False, help_text='Legacy engagement ID (auto-populated)')
    legacy_client_name = models.CharField(max_length=255, editable=False, help_text='Legacy client name (auto-populated)')
    
    class Meta:
        db_table = 'gl_accounts'
        ordering = ['account_code']
        unique_together = [['engagement', 'account_code']]  # Unique account per engagement
        indexes = [
            models.Index(fields=['engagement', 'account_code'], name='gl_accounts_eng_acc_idx'),
            models.Index(fields=['account_code'], name='gl_accounts_code_idx'),
            models.Index(fields=['account_name'], name='gl_accounts_name_idx'),
            models.Index(fields=['company_code', 'account_code'], name='gl_accounts_comp_acc_idx'),
            models.Index(fields=['legacy_engagement_id', 'account_code'], name='gl_accounts_legacy_idx'),
            models.Index(fields=['financial_statement_category'], name='gl_accounts_fs_cat_idx'),
            models.Index(fields=['is_active'], name='gl_accounts_active_idx'),
            models.Index(fields=['account_type', 'sub_type', 'sub_sub_type'], name='gl_accounts_hierarchy_idx'),
        ]
    
    def clean(self):
        """Validate that account_code contains no decimal points"""
        from django.core.exceptions import ValidationError
        import re
        
        if self.account_code:
            # Convert to string first to handle integer/float values from Excel
            account_code_str = str(self.account_code).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in account_code_str:
                try:
                    # Check if it's a whole number with .0
                    float_val = float(account_code_str)
                    if float_val.is_integer():
                        # Convert to integer string (removes .0)
                        self.account_code = str(int(float_val))
                        account_code_str = self.account_code
                    else:
                        raise ValidationError({'account_code': 'Account code cannot contain fractional values'})
                except ValueError:
                    raise ValidationError({'account_code': 'Account code must be a valid number or alphanumeric string'})
            
            # Remove commas if present (thousands separators)
            if ',' in account_code_str:
                account_code_str = account_code_str.replace(',', '')
                self.account_code = account_code_str
            
            # Ensure account code is numeric or alphanumeric without decimals
            if not re.match(r'^[A-Za-z0-9]+$', account_code_str):
                raise ValidationError({'account_code': 'Account code must be alphanumeric without special characters'})
    
    def save(self, *args, **kwargs):
        """Auto-populate legacy fields from engagement"""
        self.clean()  # Validate before saving
        
        if self.engagement:
            self.legacy_engagement_id = self.engagement.engagement_id
            self.legacy_client_name = self.engagement.client.client_name
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.account_code} - {self.account_name}"
    
    @classmethod
    def get_or_create_account(cls, engagement, account_code, account_name=None, **kwargs):
        """
        Get or create a GL Account with the given code for a specific engagement
        Updates existing account if new information is provided
        """
        account, created = cls.objects.get_or_create(
            engagement=engagement,
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
    
    def get_unified_view(self):
        """Get unified view combining GL, TB, and COA data for this account"""
        return {
            'account_code': self.account_code,
            'account_name': self.account_name,
            'engagement': self.engagement.engagement_id,
            'client': self.engagement.client.client_name,
            
            # Trial Balance data
            'opening_balance': self.opening_balance,
            'closing_balance': self.closing_balance,
            'tb_debit': self.tb_debit,
            'tb_credit': self.tb_credit,
            
            # Chart of Accounts hierarchy
            'account_type': self.account_type,
            'sub_type': self.sub_type,
            'sub_sub_type': self.sub_sub_type,
            
            # GL Posting statistics
            'gl_transaction_count': self.gl_postings.count(),
            'gl_total_debits': sum(
                posting.amount_local_currency for posting in self.gl_postings.filter(amount_local_currency__gt=0)
            ),
            'gl_total_credits': abs(sum(
                posting.amount_local_currency for posting in self.gl_postings.filter(amount_local_currency__lt=0)
            )),
            
            # Financial statement classification
            'financial_statement_category': self.financial_statement_category,
            'balance_sheet_category': self.balance_sheet_category,
            'income_statement_category': self.income_statement_category,
        }


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
    """SAP GL Posting transaction data - Positive amounts=Debit, Negative amounts=Credit"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, help_text='Reference to the uploaded file')
    
    # Document information - NO DECIMAL DOCUMENT NUMBERS
    document_number = models.CharField(
        max_length=20, 
        db_index=True, 
        blank=True, 
        default='', 
        help_text='SAP Document Number (no decimal values allowed)'
    )
    document_type = models.CharField(max_length=10, db_index=True, blank=True, default='', help_text='Document Type')
    
    # Financial data - POSITIVE=DEBIT, NEGATIVE=CREDIT
    amount_local_currency = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        null=True,
        blank=True,
        db_index=True,
        help_text='Amount in local currency (Positive=Debit, Negative=Credit)'
    )
    local_currency = models.CharField(max_length=3, default='SAR', db_index=True, help_text='Local currency code')
    
    # Account information - NO DECIMAL ACCOUNT NUMBERS
    gl_account = models.CharField(
        max_length=10, 
        db_index=True, 
        help_text='G/L Account (legacy field, no decimal values allowed)'
    )
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
    
    # Additional transaction currency fields
    amount_transaction_currency = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        null=True,
        blank=True,
        help_text='Amount in transaction currency'
    )
    transaction_currency = models.CharField(max_length=3, blank=True, help_text='Transaction currency code')
    exchange_rate = models.DecimalField(
        max_digits=15, 
        decimal_places=6, 
        null=True,
        blank=True,
        help_text='Exchange rate'
    )
    
    # Additional posting fields
    posting_key = models.CharField(max_length=10, blank=True, help_text='Posting Key')
    reference_document = models.CharField(max_length=20, blank=True, help_text='Reference Document')
    document_header_text = models.CharField(max_length=200, blank=True, help_text='Document Header Text')
    company_code = models.CharField(max_length=10, blank=True, help_text='Company Code')
    fiscal_period = models.IntegerField(null=True, blank=True, help_text='Fiscal Period')
    
    # Cost center and organizational fields
    cost_center = models.CharField(max_length=10, blank=True, help_text='Cost Center')
    wbs_element = models.CharField(max_length=20, blank=True, help_text='WBS Element')
    order_number = models.CharField(max_length=20, blank=True, help_text='Order Number')
    asset_number = models.CharField(max_length=20, blank=True, help_text='Asset Number')
    sub_number = models.CharField(max_length=10, blank=True, help_text='Sub Number')
    business_area = models.CharField(max_length=10, blank=True, help_text='Business Area')
    partner_business_area = models.CharField(max_length=10, blank=True, help_text='Partner Business Area')
    
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
    
    def clean(self):
        """Validate that account and document numbers contain no decimal points"""
        from django.core.exceptions import ValidationError
        import re
        
        # Validate GL account number
        if self.gl_account:
            gl_account_str = str(self.gl_account).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in gl_account_str:
                try:
                    # Check if it's a whole number with .0
                    float_val = float(gl_account_str)
                    if float_val.is_integer():
                        # Convert to integer string (removes .0)
                        self.gl_account = str(int(float_val))
                        gl_account_str = self.gl_account
                    else:
                        raise ValidationError({'gl_account': 'GL Account cannot contain fractional values'})
                except ValueError:
                    raise ValidationError({'gl_account': 'GL Account must be a valid number or alphanumeric string'})
            
            # Remove commas if present (thousands separators)
            if ',' in gl_account_str:
                gl_account_str = gl_account_str.replace(',', '')
                self.gl_account = gl_account_str
            
            if not re.match(r'^[A-Za-z0-9]+$', gl_account_str):
                raise ValidationError({'gl_account': 'GL Account must be alphanumeric without special characters'})
        
        # Validate document number
        if self.document_number:
            document_number_str = str(self.document_number)
            if '.' in document_number_str or ',' in document_number_str:
                raise ValidationError({'document_number': 'Document number cannot contain decimal or fractional values'})
    
    def save(self, *args, **kwargs):
        """Validate before saving"""
        self.clean()
        super().save(*args, **kwargs)
    
    @property
    def transaction_type(self):
        """
        Determine transaction type based on amount
        Positive amounts are debits, negative amounts are credits
        """
        if self.amount_local_currency is None:
            return 'UNKNOWN'
        elif self.amount_local_currency > 0:
            return 'DEBIT'
        else:
            return 'CREDIT'
    
    @property
    def debit_amount(self):
        """Get debit amount (positive amounts only)"""
        return self.amount_local_currency if self.amount_local_currency and self.amount_local_currency > 0 else None
    
    @property
    def credit_amount(self):
        """Get credit amount (absolute value of negative amounts)"""
        return abs(self.amount_local_currency) if self.amount_local_currency and self.amount_local_currency < 0 else None
    
    def __str__(self):
        return f"{self.document_number} - {self.gl_account} - {self.amount_local_currency} ({self.transaction_type})"
    
    @classmethod
    def parse_and_create(cls, data_file, raw_data):
        """
        Parse raw data and create GL Posting record with proper amount parsing
        
        Args:
            data_file: DataFile instance
            raw_data: Dictionary with raw data fields
        
        Returns:
            SAPGLPosting instance or None if parsing fails
        """
        try:
            # Parse amount with error handling
            amount_str = raw_data.get('amount_local_currency', '')
            parsed_amount = AmountParser.parse_amount(amount_str)
            
            # Create the record
            posting = cls(
                data_file=data_file,
                document_number=str(raw_data.get('document_number', '')).strip(),
                document_type=str(raw_data.get('document_type', '')).strip(),
                amount_local_currency=parsed_amount,
                local_currency=str(raw_data.get('local_currency', 'SAR')).strip(),
                gl_account=str(raw_data.get('gl_account', '')).strip(),
                profit_center=str(raw_data.get('profit_center', '')).strip(),
                user_name=str(raw_data.get('user_name', '')).strip(),
                posting_date=raw_data.get('posting_date'),
                document_date=raw_data.get('document_date'),
                entry_date=raw_data.get('entry_date'),
                fiscal_year=raw_data.get('fiscal_year'),
                posting_period=raw_data.get('posting_period'),
                text=str(raw_data.get('text', '')).strip(),
                segment=str(raw_data.get('segment', '')).strip(),
                clearing_document=str(raw_data.get('clearing_document', '')).strip(),
                offsetting_account=str(raw_data.get('offsetting_account', '')).strip(),
                invoice_reference=str(raw_data.get('invoice_reference', '')).strip(),
                sales_document=str(raw_data.get('sales_document', '')).strip(),
                assignment=str(raw_data.get('assignment', '')).strip(),
                year_month=str(raw_data.get('year_month', '')).strip(),
            )
            
            return posting
            
        except Exception as e:
            # Log parsing error
            print(f"Error parsing GL posting data: {e}")
            return None
    
    def get_amount_display(self):
        """Get formatted amount for display"""
        return AmountParser.format_amount(self.amount_local_currency, self.local_currency)
    
    def get_parsing_info(self):
        """Get information about how the amount was parsed"""
        return {
            'amount': self.amount_local_currency,
            'transaction_type': self.transaction_type,
            'debit_amount': self.debit_amount,
            'credit_amount': self.credit_amount,
            'formatted_display': self.get_amount_display()
        }


class TrialBalance(BaseModel):
    """Trial Balance data - Each account MUST have Opening and Closing Balance"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='trial_balances', help_text='Reference to the uploaded TB file')
    
    # Account information - NO DECIMAL ACCOUNT NUMBERS
    company_code = models.CharField(max_length=10, help_text='Company Code')
    gl_account = models.CharField(
        max_length=10, 
        help_text='G/L Account (legacy field, no decimal values allowed)'
    )
    gl_account_ref = models.ForeignKey(
        GLAccount, 
        on_delete=models.PROTECT, 
        null=True, 
        blank=True, 
        related_name='trial_balances',
        help_text='Reference to GL Account master record'
    )
    short_text = models.CharField(max_length=100, help_text='Account Short Text/Description')
    currency = models.CharField(max_length=3, default='SAR', help_text='Currency')
    
    # Balance information - REQUIRED: Opening and Closing Balance
    opening_balance = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        help_text='Opening Balance (REQUIRED for each account)'
    )
    closing_balance = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        help_text='Closing Balance (REQUIRED for each account)'
    )
    
    # Optional: Debit and Credit entries
    debit = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        null=True, 
        blank=True, 
        help_text='Debit Amount (OPTIONAL)'
    )
    credit = models.DecimalField(
        max_digits=30, 
        decimal_places=10, 
        null=True, 
        blank=True, 
        help_text='Credit Amount (OPTIONAL)'
    )
    
    class Meta:
        db_table = 'trial_balance'
        ordering = ['gl_account']
        unique_together = [['data_file', 'gl_account']]  # One TB record per account per file
        indexes = [
            models.Index(fields=['data_file', 'gl_account']),
            models.Index(fields=['company_code', 'gl_account']),
            models.Index(fields=['gl_account_ref']),
        ]
    
    def clean(self):
        """Validate that account number contains no decimal points"""
        from django.core.exceptions import ValidationError
        import re
        
        if self.gl_account:
            # Convert to string first to handle integer/float values from Excel
            gl_account_str = str(self.gl_account).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in gl_account_str:
                try:
                    # Check if it's a whole number with .0
                    float_val = float(gl_account_str)
                    if float_val.is_integer():
                        # Convert to integer string (removes .0)
                        self.gl_account = str(int(float_val))
                        gl_account_str = self.gl_account
                    else:
                        raise ValidationError({'gl_account': 'GL Account cannot contain fractional values'})
                except ValueError:
                    raise ValidationError({'gl_account': 'GL Account must be a valid number or alphanumeric string'})
            
            # Remove commas if present (thousands separators)
            if ',' in gl_account_str:
                gl_account_str = gl_account_str.replace(',', '')
                self.gl_account = gl_account_str
            
            if not re.match(r'^[A-Za-z0-9]+$', gl_account_str):
                raise ValidationError({'gl_account': 'GL Account must be alphanumeric without special characters'})
    
    def save(self, *args, **kwargs):
        """Validate and auto-link to GL Account master record"""
        self.clean()
        
        # Auto-link to GL Account if engagement is available
        if self.data_file and self.data_file.engagement and self.gl_account:
            self.gl_account_ref = GLAccount.get_or_create_account(
                engagement=self.data_file.engagement,
                account_code=self.gl_account,
                account_name=self.short_text
            )
            
            # Update GL Account with TB information
            if self.gl_account_ref:
                self.gl_account_ref.opening_balance = self.opening_balance
                self.gl_account_ref.closing_balance = self.closing_balance
                self.gl_account_ref.tb_debit = self.debit
                self.gl_account_ref.tb_credit = self.credit
                self.gl_account_ref.save()
        
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.gl_account} - {self.short_text} - OB:{self.opening_balance} CB:{self.closing_balance}"
    
    def get_balance_movement(self):
        """Calculate balance movement (closing - opening)"""
        return self.closing_balance - self.opening_balance
    
    def validate_balance_equation(self):
        """Validate that Opening + Debit - Credit = Closing (if debit/credit provided)"""
        if self.debit is not None or self.credit is not None:
            calculated_closing = self.opening_balance + (self.debit or 0) - (self.credit or 0)
            return abs(calculated_closing - self.closing_balance) < 0.01  # Allow for small rounding differences
        return True
    
    @classmethod
    def parse_and_create(cls, data_file, raw_data):
        """
        Parse raw trial balance data and create TB record with proper amount parsing
        
        Args:
            data_file: DataFile instance
            raw_data: Dictionary with raw TB data
        
        Returns:
            TrialBalance instance or None if parsing fails
        """
        try:
            # Parse all amounts
            opening_balance = AmountParser.parse_amount(raw_data.get('opening_balance', ''), default_zero=True)
            closing_balance = AmountParser.parse_amount(raw_data.get('closing_balance', ''), default_zero=True)
            debit = AmountParser.parse_amount(raw_data.get('debit', ''))
            credit = AmountParser.parse_amount(raw_data.get('credit', ''))
            
            # Create the record
            tb = cls(
                data_file=data_file,
                company_code=str(raw_data.get('company_code', '')).strip(),
                gl_account=str(raw_data.get('gl_account', '')).strip(),
                short_text=str(raw_data.get('short_text', '')).strip(),
                currency=str(raw_data.get('currency', 'SAR')).strip(),
                opening_balance=opening_balance,
                closing_balance=closing_balance,
                debit=debit,
                credit=credit,
            )
            
            return tb
            
        except Exception as e:
            print(f"Error parsing Trial Balance data: {e}")
            return None
    
    def get_amounts_display(self):
        """Get formatted amounts for display"""
        return {
            'opening_balance': AmountParser.format_amount(self.opening_balance, self.currency),
            'closing_balance': AmountParser.format_amount(self.closing_balance, self.currency),
            'debit': AmountParser.format_amount(self.debit, self.currency) if self.debit else '',
            'credit': AmountParser.format_amount(self.credit, self.currency) if self.credit else '',
            'movement': AmountParser.format_amount(self.get_balance_movement(), self.currency)
        }
    
    def get_parsing_validation(self):
        """Validate parsed amounts and return validation results"""
        validation = {
            'balance_equation_valid': self.validate_balance_equation(),
            'has_required_balances': self.opening_balance is not None and self.closing_balance is not None,
            'has_optional_entries': self.debit is not None or self.credit is not None,
            'movement': self.get_balance_movement(),
        }
        
        if not validation['balance_equation_valid']:
            validation['balance_equation_error'] = 'Opening + Debit - Credit ≠ Closing Balance'
        
        return validation


class ChartOfAccount(BaseModel):
    """Chart of Accounts - Hierarchical structure: Account -> Type -> Sub-Type -> Sub-Sub-Type"""
    
    # File reference
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='chart_of_accounts', help_text='Reference to the uploaded COA file')
    
    # Account information - NO DECIMAL ACCOUNT NUMBERS (REQUIRED)
    account = models.CharField(
        max_length=10, 
        help_text='Account Code (no decimal values allowed)'
    )
    
    # HIERARCHICAL STRUCTURE (ALL REQUIRED)
    type = models.CharField(max_length=50, help_text='Type (REQUIRED - top level hierarchy)')
    sub_type = models.CharField(max_length=50, help_text='Sub Type (REQUIRED - second level hierarchy)')
    sub_sub_type = models.CharField(max_length=50, help_text='Sub Sub Type (REQUIRED - third level hierarchy)')
    
    # Account linking
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
    q1_amount = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Q1 Amount')
    adj_reclas = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Adjustments/Reclassifications')
    fin_q1_amount = models.DecimalField(max_digits=30, decimal_places=10, null=True, blank=True, help_text='Final Q1 Amount')
    
    class Meta:
        db_table = 'chart_of_accounts'
        ordering = ['type', 'sub_type', 'sub_sub_type', 'account']
        unique_together = [['data_file', 'account']]  # One COA record per account per file
        indexes = [
            models.Index(fields=['data_file', 'account']),
            models.Index(fields=['type', 'sub_type', 'sub_sub_type']),
            models.Index(fields=['account']),
            models.Index(fields=['gl_account_ref']),
            models.Index(fields=['type']),
            models.Index(fields=['sub_type']),
            models.Index(fields=['sub_sub_type']),
        ]
    
    def clean(self):
        """Validate hierarchical structure and account format"""
        from django.core.exceptions import ValidationError
        import re
        
        # Validate account number
        if self.account:
            account_str = str(self.account).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in account_str:
                try:
                    # Check if it's a whole number with .0
                    float_val = float(account_str)
                    if float_val.is_integer():
                        # Convert to integer string (removes .0)
                        self.account = str(int(float_val))
                        account_str = self.account
                    else:
                        raise ValidationError({'account': 'Account code cannot contain fractional values'})
                except ValueError:
                    raise ValidationError({'account': 'Account code must be a valid number or alphanumeric string'})
            
            # Remove commas if present (thousands separators)
            if ',' in account_str:
                account_str = account_str.replace(',', '')
                self.account = account_str
            
            if not re.match(r'^[A-Za-z0-9]+$', account_str):
                raise ValidationError({'account': 'Account code must be alphanumeric without special characters'})
        
        # Validate hierarchical structure
        if not self.type:
            raise ValidationError({'type': 'Type is required for hierarchical structure'})
        if not self.sub_type:
            raise ValidationError({'sub_type': 'Sub Type is required for hierarchical structure'})
        if not self.sub_sub_type:
            raise ValidationError({'sub_sub_type': 'Sub Sub Type is required for hierarchical structure'})
    
    def save(self, *args, **kwargs):
        """Validate and auto-link to GL Account master record"""
        self.clean()
        
        # Auto-link to GL Account if engagement is available
        if self.data_file and self.data_file.engagement and self.account:
            self.gl_account_ref = GLAccount.get_or_create_account(
                engagement=self.data_file.engagement,
                account_code=self.account,
                account_name=self.gl_account_long_text
            )
            
            # Update GL Account with COA hierarchy information
            if self.gl_account_ref:
                self.gl_account_ref.account_type = self.type
                self.gl_account_ref.sub_type = self.sub_type
                self.gl_account_ref.sub_sub_type = self.sub_sub_type
                self.gl_account_ref.financial_statement_category = self.financial_statement
                self.gl_account_ref.save()
        
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.account} - {self.type} → {self.sub_type} → {self.sub_sub_type}"
    
    def get_hierarchy_path(self):
        """Get full hierarchy path as string"""
        return f"{self.type} → {self.sub_type} → {self.sub_sub_type}"
    
    def get_hierarchy_dict(self):
        """Get hierarchy as dictionary"""
        return {
            'level_1_type': self.type,
            'level_2_sub_type': self.sub_type,
            'level_3_sub_sub_type': self.sub_sub_type,
            'account_code': self.account,
            'full_path': self.get_hierarchy_path()
        }
    
    @classmethod
    def get_hierarchy_tree(cls, engagement):
        """Get complete hierarchy tree for an engagement"""
        coa_records = cls.objects.filter(data_file__engagement=engagement)
        
        hierarchy = {}
        for record in coa_records:
            if record.type not in hierarchy:
                hierarchy[record.type] = {}
            if record.sub_type not in hierarchy[record.type]:
                hierarchy[record.type][record.sub_type] = {}
            if record.sub_sub_type not in hierarchy[record.type][record.sub_type]:
                hierarchy[record.type][record.sub_type][record.sub_sub_type] = []
            
            hierarchy[record.type][record.sub_type][record.sub_sub_type].append({
                'account': record.account,
                'description': record.gl_account_long_text,
                'id': record.id
            })
        
        return hierarchy
    
    @classmethod
    def parse_and_create(cls, data_file, raw_data):
        """
        Parse raw chart of accounts data and create COA record
        
        Args:
            data_file: DataFile instance
            raw_data: Dictionary with raw COA data
        
        Returns:
            ChartOfAccount instance or None if parsing fails
        """
        try:
            # Parse amount fields
            q1_amount = AmountParser.parse_amount(raw_data.get('q1_amount', ''))
            adj_reclas = AmountParser.parse_amount(raw_data.get('adj_reclas', ''))
            fin_q1_amount = AmountParser.parse_amount(raw_data.get('fin_q1_amount', ''))
            
            # Create the record
            coa = cls(
                data_file=data_file,
                account=str(raw_data.get('account', '')).strip(),
                type=str(raw_data.get('type', '')).strip(),
                sub_type=str(raw_data.get('sub_type', '')).strip(),
                sub_sub_type=str(raw_data.get('sub_sub_type', '')).strip(),
                gl_account=str(raw_data.get('gl_account', '')).strip(),
                company=str(raw_data.get('company', '')).strip(),
                branch=str(raw_data.get('branch', '')).strip(),
                cost_center=str(raw_data.get('cost_center', '')).strip(),
                code=str(raw_data.get('code', '')).strip(),
                gl_account_long_text=str(raw_data.get('gl_account_long_text', '')).strip(),
                ref_to_fs=str(raw_data.get('ref_to_fs', '')).strip(),
                financial_statement=str(raw_data.get('financial_statement', '')).strip(),
                ref_to_note=str(raw_data.get('ref_to_note', '')).strip(),
                fiscal_year=raw_data.get('fiscal_year'),
                q1_amount=q1_amount,
                adj_reclas=adj_reclas,
                fin_q1_amount=fin_q1_amount,
            )
            
            return coa
            
        except Exception as e:
            print(f"Error parsing Chart of Accounts data: {e}")
            return None
    
    def get_amounts_display(self):
        """Get formatted amounts for display"""
        return {
            'q1_amount': AmountParser.format_amount(self.q1_amount) if self.q1_amount else '',
            'adj_reclas': AmountParser.format_amount(self.adj_reclas) if self.adj_reclas else '',
            'fin_q1_amount': AmountParser.format_amount(self.fin_q1_amount) if self.fin_q1_amount else '',
        }
    
    def get_hierarchy_validation(self):
        """Validate hierarchical structure"""
        validation = {
            'has_complete_hierarchy': bool(self.type and self.sub_type and self.sub_sub_type),
            'hierarchy_levels': {
                'type': bool(self.type),
                'sub_type': bool(self.sub_type),
                'sub_sub_type': bool(self.sub_sub_type)
            },
            'account_format_valid': True,  # Will be set by clean() method
        }
        
        if not validation['has_complete_hierarchy']:
            missing_levels = [k for k, v in validation['hierarchy_levels'].items() if not v]
            validation['hierarchy_error'] = f"Missing hierarchy levels: {', '.join(missing_levels)}"
        
        return validation


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
    Engagement-level completeness test results
    
    Performs completeness validation across all files in an engagement:
    - GL-TB reconciliation analysis
    - Debit-credit balance verification  
    - Account coverage analysis across TB, GL, and COA
    - Transaction gap detection
    - Overall engagement completeness scoring
    """
    
    # Engagement reference (tests are performed at engagement level)
    engagement = models.ForeignKey(Engagement, on_delete=models.CASCADE, related_name='completeness_tests', help_text='Engagement being tested')
    
    # Test metadata
    test_timestamp = models.DateTimeField(auto_now_add=True, help_text='When the completeness test was performed')
    test_version = models.CharField(max_length=20, default='3.0.0', help_text='Version of completeness test algorithm')
    
    # Files included in the test
    gl_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='gl_completeness_tests', null=True, blank=True, help_text='GL file tested')
    tb_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='tb_completeness_tests', null=True, blank=True, help_text='TB file tested')
    coa_file = models.ForeignKey(DataFile, on_delete=models.CASCADE, related_name='coa_completeness_tests', null=True, blank=True, help_text='COA file tested')
    
    # Legacy field for backward compatibility
    legacy_engagement_id = models.CharField(max_length=100, editable=False, help_text='Legacy engagement ID (auto-populated)')
    
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
    
    # Enhanced test step results for engagement-level testing
    step1_file_completeness = models.JSONField(default=dict, help_text='File completeness check (TB, GL, COA present)')
    step2_gl_tb_reconciliation = models.JSONField(default=dict, help_text='GL-TB reconciliation test results')
    step3_debit_credit_balance = models.JSONField(default=dict, help_text='Debit-credit balance verification results')
    step4_account_coverage = models.JSONField(default=dict, help_text='Account coverage analysis across all files')
    step5_coa_hierarchy_validation = models.JSONField(default=dict, help_text='COA hierarchy validation results')
    step6_account_linking = models.JSONField(default=dict, help_text='Account linking validation across TB, GL, COA')
    step7_transaction_gaps = models.JSONField(default=dict, help_text='Transaction gap detection results')
    
    # Enhanced summary statistics
    total_gl_records = models.IntegerField(help_text='Total GL posting records analyzed')
    total_tb_records = models.IntegerField(null=True, blank=True, help_text='Total TB records analyzed')
    total_coa_records = models.IntegerField(null=True, blank=True, help_text='Total COA records analyzed')
    total_accounts_unified = models.IntegerField(help_text='Total accounts unified across all files')
    tests_passed = models.IntegerField(help_text='Number of tests passed')
    total_tests = models.IntegerField(default=7, help_text='Total number of tests performed')
    
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
            models.Index(fields=['engagement', 'test_timestamp'], name='completeness_eng_time_idx'),
            models.Index(fields=['legacy_engagement_id', 'overall_status'], name='completeness_legacy_idx'),
            models.Index(fields=['overall_status', 'completeness_score'], name='completeness_status_score_idx'),
            models.Index(fields=['test_timestamp'], name='completeness_time_idx'),
        ]
    
    def save(self, *args, **kwargs):
        """Auto-populate legacy fields from engagement"""
        if self.engagement:
            self.legacy_engagement_id = self.engagement.engagement_id
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"Completeness Test - {self.engagement.engagement_id} - {self.overall_status} ({self.completeness_score:.1f}%)"
    
    def get_summary(self):
        """Get a summary of the completeness test results"""
        return {
            'engagement_id': self.engagement.engagement_id,
            'client_name': self.engagement.client.client_name,
            'fiscal_year': self.engagement.fiscal_year,
            'status': self.overall_status,
            'score': self.completeness_score,
            'tests_passed': f"{self.tests_passed}/{self.total_tests}",
            'critical_issues': self.critical_issues_count,
            'explanation': self.overall_explanation,
            'files_tested': {
                'gl_file': self.gl_file.file_name if self.gl_file else None,
                'tb_file': self.tb_file.file_name if self.tb_file else None,
                'coa_file': self.coa_file.file_name if self.coa_file else None,
            }
        }
    
    def has_critical_issues(self):
        """Check if the test found critical issues"""
        return self.critical_issues_count > 0 or self.overall_status == 'FAIL'
    
    def get_failed_tests(self):
        """Get list of failed test steps"""
        failed_tests = []
        
        if not self.step1_file_completeness.get('passed', True):
            failed_tests.append('File Completeness')
        if not self.step2_gl_tb_reconciliation.get('passed', True):
            failed_tests.append('GL-TB Reconciliation')
        if not self.step3_debit_credit_balance.get('passed', True):
            failed_tests.append('Debit-Credit Balance')
        if not self.step4_account_coverage.get('passed', True):
            failed_tests.append('Account Coverage')
        if not self.step5_coa_hierarchy_validation.get('passed', True):
            failed_tests.append('COA Hierarchy Validation')
        if not self.step6_account_linking.get('passed', True):
            failed_tests.append('Account Linking')
        if not self.step7_transaction_gaps.get('passed', True):
            failed_tests.append('Transaction Gaps')
            
        return failed_tests
    
    def get_engagement_completeness_status(self):
        """Get engagement completeness status considering all files"""
        file_status = {
            'has_gl': self.gl_file is not None,
            'has_tb': self.tb_file is not None,
            'has_coa': self.coa_file is not None,
            'required_files_complete': False,
            'files_validated': False
        }
        
        file_status['required_files_complete'] = all([
            file_status['has_gl'],
            file_status['has_tb'], 
            file_status['has_coa']
        ])
        
        if file_status['required_files_complete']:
            file_status['files_validated'] = all([
                self.gl_file.is_validated if self.gl_file else False,
                self.tb_file.is_validated if self.tb_file else False,
                self.coa_file.is_validated if self.coa_file else False,
            ])
        
        return file_status
