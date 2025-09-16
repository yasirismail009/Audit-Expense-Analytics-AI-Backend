"""
Serializers for listing APIs - Optimized for large datasets
"""

from rest_framework import serializers
from django.db.models import Count, Sum, Avg, Min, Max
from .models import SAPGLPosting, TrialBalance, ChartOfAccount, DataFile


class SAPGLPostingListSerializer(serializers.ModelSerializer):
    """Optimized serializer for GL posting listings"""
    
    data_file_name = serializers.CharField(source='data_file.file_name', read_only=True)
    data_file_id = serializers.UUIDField(source='data_file.id', read_only=True)
    
    class Meta:
        model = SAPGLPosting
        fields = [
            'id', 'document_number', 'document_type', 'posting_date', 
            'document_date', 'entry_date', 'gl_account', 'profit_center',
            'amount_local_currency', 'local_currency', 'user_name',
            'fiscal_year', 'posting_period', 'text', 'segment',
            'clearing_document', 'offsetting_account', 'invoice_reference',
            'sales_document', 'assignment', 'year_month',
            'data_file_name', 'data_file_id', 'created_at'
        ]


class SAPGLPostingSummarySerializer(serializers.Serializer):
    """Summary statistics for GL postings"""
    
    total_records = serializers.IntegerField()
    total_amount = serializers.DecimalField(max_digits=20, decimal_places=2)
    unique_accounts = serializers.IntegerField()
    unique_users = serializers.IntegerField()
    unique_documents = serializers.IntegerField()
    date_range = serializers.DictField()
    amount_range = serializers.DictField()
    top_accounts = serializers.ListField()
    top_users = serializers.ListField()


class TrialBalanceListSerializer(serializers.ModelSerializer):
    """Serializer for Trial Balance listings"""
    
    data_file_name = serializers.CharField(source='data_file.file_name', read_only=True)
    data_file_id = serializers.UUIDField(source='data_file.id', read_only=True)
    
    class Meta:
        model = TrialBalance
        fields = [
            'id', 'company_code', 'gl_account', 'short_text', 'currency',
            'opening_balance', 'debit', 'credit', 'closing_balance',
            'data_file_name', 'data_file_id', 'created_at'
        ]


class ChartOfAccountListSerializer(serializers.ModelSerializer):
    """Serializer for Chart of Accounts listings"""
    
    data_file_name = serializers.CharField(source='data_file.file_name', read_only=True)
    data_file_id = serializers.UUIDField(source='data_file.id', read_only=True)
    
    class Meta:
        model = ChartOfAccount
        fields = [
            'id', 'account', 'type', 'sub_type', 'sub_sub_type',
            'gl_account', 'company', 'branch', 'cost_center', 'code',
            'gl_account_long_text', 'ref_to_fs', 'financial_statement',
            'ref_to_note', 'fiscal_year', 'q1_amount', 'adj_reclas',
            'fin_q1_amount', 'data_file_name', 'data_file_id', 'created_at'
        ]


class DataFileListSerializer(serializers.ModelSerializer):
    """Serializer for DataFile listings with engagement information"""
    
    # Annotated counts
    gl_count = serializers.IntegerField(read_only=True)
    tb_count = serializers.IntegerField(read_only=True)
    chart_count = serializers.IntegerField(read_only=True)
    
    # Engagement information (from related engagement object)
    engagement_id = serializers.CharField(source='engagement.engagement_id', read_only=True)
    engagement_name = serializers.CharField(source='engagement.engagement_name', read_only=True)
    client_name = serializers.CharField(source='engagement.client.client_name', read_only=True)
    client_code = serializers.CharField(source='engagement.client.client_code', read_only=True)
    company_name = serializers.CharField(source='engagement.client.company_name', read_only=True)
    fiscal_year = serializers.IntegerField(source='engagement.fiscal_year', read_only=True)
    audit_start_date = serializers.DateField(source='engagement.audit_start_date', read_only=True)
    audit_end_date = serializers.DateField(source='engagement.audit_end_date', read_only=True)
    engagement_status = serializers.CharField(source='engagement.status', read_only=True)
    
    # Legacy fields as fallback (for backward compatibility)
    legacy_engagement_id = serializers.CharField(read_only=True)
    legacy_client_name = serializers.CharField(read_only=True)
    legacy_company_name = serializers.CharField(read_only=True)
    legacy_fiscal_year = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = DataFile
        fields = [
            'id', 'file_name', 'file_size', 'file_type', 'is_validated',
            'status', 'total_records', 'processed_records', 'failed_records',
            'uploaded_at', 'processed_at', 'error_message',
            'gl_count', 'tb_count', 'chart_count',
            # Engagement information
            'engagement_id', 'engagement_name', 'engagement_status',
            'client_name', 'client_code', 'company_name', 
            'fiscal_year', 'audit_start_date', 'audit_end_date',
            # Legacy fields
            'legacy_engagement_id', 'legacy_client_name', 'legacy_company_name', 'legacy_fiscal_year'
        ]
