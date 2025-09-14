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
    """Serializer for DataFile listings"""
    
    gl_count = serializers.IntegerField(read_only=True)
    tb_count = serializers.IntegerField(read_only=True)
    chart_count = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = DataFile
        fields = [
            'id', 'file_name', 'file_size', 'engagement_id', 'client_name',
            'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date',
            'status', 'total_records', 'processed_records', 'failed_records',
            'uploaded_at', 'processed_at', 'gl_count', 'tb_count', 'chart_count'
        ]
