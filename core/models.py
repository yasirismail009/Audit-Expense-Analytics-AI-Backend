from django.db import models
import json
from decimal import Decimal

# Create your models here.

class ExpenseSheet(models.Model):
    """Represents an expense sheet (CSV file) with date and name"""
    sheet_name = models.CharField(max_length=255, help_text='Name of the expense sheet')
    sheet_date = models.DateField(help_text='Date of the expense sheet')
    uploaded_at = models.DateTimeField(auto_now_add=True)
    total_expenses = models.IntegerField(default=0)
    total_amount = models.DecimalField(max_digits=15, decimal_places=2, default=Decimal('0.00'))
    
    class Meta:
        unique_together = ['sheet_name', 'sheet_date']
    
    def __str__(self):
        return f"{self.sheet_name} - {self.sheet_date}"
    
    @property
    def display_name(self):
        """Get formatted sheet name with date"""
        return f"{self.sheet_name}_{self.sheet_date}"

class Expense(models.Model):
    # Relationship to the expense sheet this expense came from
    expense_sheet = models.ForeignKey(
        ExpenseSheet, 
        on_delete=models.CASCADE, 
        related_name='expenses',
        help_text='The expense sheet this expense belongs to'
    )
    
    date = models.DateField()
    category = models.CharField(max_length=100)
    subcategory = models.CharField(max_length=100)
    description = models.CharField(max_length=255)
    employee = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    currency = models.CharField(max_length=10)
    payment_method = models.CharField(max_length=50)
    vendor_supplier = models.CharField(max_length=255)
    receipt_number = models.CharField(max_length=100)
    status = models.CharField(max_length=50)
    approved_by = models.CharField(max_length=100)
    notes = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.description} - {self.amount} ({getattr(self.expense_sheet, 'display_name', 'Unknown Sheet')})"
    
    @property
    def sheet_name(self):
        """Get the name of the expense sheet this expense came from"""
        return getattr(self.expense_sheet, 'display_name', 'Unknown Sheet')
    
    @property
    def sheet_date(self):
        """Get the date of the expense sheet"""
        return getattr(self.expense_sheet, 'sheet_date', None)

class SheetAnalysis(models.Model):
    """Analysis results for an entire expense sheet"""
    expense_sheet = models.OneToOneField(ExpenseSheet, on_delete=models.CASCADE, related_name='analysis')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Overall fraud score for the sheet (0-100)
    overall_fraud_score = models.FloatField()
    
    # Individual model scores for the sheet
    isolation_forest_score = models.FloatField()
    xgboost_score = models.FloatField()
    lof_score = models.FloatField()
    random_forest_score = models.FloatField()
    
    # Risk level for the entire sheet
    RISK_LEVELS = [
        ('LOW', 'Low Risk'),
        ('MEDIUM', 'Medium Risk'),
        ('HIGH', 'High Risk'),
        ('CRITICAL', 'Critical Risk'),
    ]
    risk_level = models.CharField(max_length=10, choices=RISK_LEVELS, default='LOW')
    
    # Sheet-level analysis results (JSON)
    analysis_details = models.JSONField(default=dict)
    
    # Sheet-level anomaly flags
    amount_anomalies_detected = models.IntegerField(default=0)
    timing_anomalies_detected = models.IntegerField(default=0)
    vendor_anomalies_detected = models.IntegerField(default=0)
    employee_anomalies_detected = models.IntegerField(default=0)
    duplicate_suspicions = models.IntegerField(default=0)
    
    # Summary statistics
    total_flagged_expenses = models.IntegerField(default=0)
    high_risk_expenses = models.IntegerField(default=0)
    critical_risk_expenses = models.IntegerField(default=0)
    
    def __str__(self):
        return f"Analysis for {getattr(self.expense_sheet, 'display_name', 'Unknown Sheet')} - Score: {self.overall_fraud_score}"
    
    @property
    def flag_rate(self):
        """Calculate the percentage of flagged expenses in the sheet"""
        expense_sheet = getattr(self, 'expense_sheet', None)
        if expense_sheet and getattr(expense_sheet, 'total_expenses', 0) > 0:
            return (self.total_flagged_expenses / expense_sheet.total_expenses) * 100
        return 0

class ExpenseAnalysis(models.Model):
    """Individual expense analysis within a sheet"""
    expense = models.OneToOneField(Expense, on_delete=models.CASCADE, related_name='analysis')
    sheet_analysis = models.ForeignKey(SheetAnalysis, on_delete=models.CASCADE, related_name='expense_analyses')
    created_at = models.DateTimeField(auto_now_add=True)
    
    # Individual expense fraud score
    fraud_score = models.FloatField()
    
    # Risk level for this specific expense
    RISK_LEVELS = [
        ('LOW', 'Low Risk'),
        ('MEDIUM', 'Medium Risk'),
        ('HIGH', 'High Risk'),
        ('CRITICAL', 'Critical Risk'),
    ]
    risk_level = models.CharField(max_length=10, choices=RISK_LEVELS, default='LOW')
    
    # Individual expense anomaly flags
    amount_anomaly = models.BooleanField(default=False)
    timing_anomaly = models.BooleanField(default=False)
    vendor_anomaly = models.BooleanField(default=False)
    employee_anomaly = models.BooleanField(default=False)
    duplicate_suspicion = models.BooleanField(default=False)
    
    # Detailed analysis for this expense (JSON)
    analysis_details = models.JSONField(default=dict)
    
    def __str__(self):
        return f"Analysis for {self.expense.description} - Score: {self.fraud_score}"

class AnalysisSession(models.Model):
    """Tracks each analysis session (can contain multiple sheets)"""
    session_id = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    session_name = models.CharField(max_length=255)
    total_sheets = models.IntegerField(default=0)
    total_expenses = models.IntegerField(default=0)
    flagged_sheets = models.IntegerField(default=0)
    analysis_status = models.CharField(max_length=20, default='PENDING')
    
    # Analysis configuration
    model_config = models.JSONField(default=dict)
    
    def __str__(self):
        return f"Session {self.session_id} - {self.session_name}"
    
    @property
    def flag_rate(self):
        """Calculate the percentage of flagged sheets"""
        if self.total_sheets > 0:
            return (self.flagged_sheets / self.total_sheets) * 100
        return 0
    
    def get_sheets_by_risk_level(self, risk_level):
        """Get all sheets with a specific risk level"""
        return self.expense_sheets.filter(analysis__risk_level=risk_level)
    
    def get_high_risk_sheets(self):
        """Get all high and critical risk sheets"""
        return self.expense_sheets.filter(analysis__risk_level__in=['HIGH', 'CRITICAL'])
