"""
Listing API views - Optimized for large datasets with filtering and pagination
"""

from rest_framework import generics, status, filters
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from django.db.models import Q, Count, Sum, Avg, Min, Max
from django.utils import timezone
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from .models import SAPGLPosting, TrialBalance, ChartOfAccount, DataFile
from .listing_serializers import (
    SAPGLPostingListSerializer, SAPGLPostingSummarySerializer,
    TrialBalanceListSerializer, ChartOfAccountListSerializer, DataFileListSerializer
)

logger = logging.getLogger(__name__)


class OptimizedPagination(PageNumberPagination):
    """Optimized pagination for large datasets"""
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000


class SAPGLPostingListView(generics.ListAPIView):
    """
    List GL postings with advanced filtering and pagination
    Optimized for 400k+ records
    """
    
    serializer_class = SAPGLPostingListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'document_number': ['exact', 'icontains'],
        'document_type': ['exact', 'icontains'],
        'gl_account': ['exact', 'icontains'],
        'profit_center': ['exact', 'icontains'],
        'user_name': ['exact', 'icontains'],
        'local_currency': ['exact'],
        'fiscal_year': ['exact', 'gte', 'lte'],
        'posting_period': ['exact', 'gte', 'lte'],
        'segment': ['exact', 'icontains'],
        'posting_date': ['exact', 'gte', 'lte', 'range'],
        'amount_local_currency': ['exact', 'gte', 'lte', 'range'],
        'data_file': ['exact'],
        'data_file__client_name': ['exact', 'icontains'],
        'data_file__company_name': ['exact', 'icontains'],
        'data_file__engagement_id': ['exact', 'icontains'],
    }
    
    search_fields = [
        'document_number', 'gl_account', 'user_name', 'text', 
        'clearing_document', 'invoice_reference', 'sales_document'
    ]
    
    ordering_fields = [
        'posting_date', 'document_number', 'amount_local_currency', 
        'gl_account', 'user_name', 'fiscal_year', 'created_at'
    ]
    ordering = ['-posting_date', '-created_at']
    
    def get_queryset(self):
        """Optimized queryset with select_related for performance"""
        return SAPGLPosting.objects.select_related('data_file').only(
            'id', 'document_number', 'document_type', 'posting_date',
            'document_date', 'entry_date', 'gl_account', 'profit_center',
            'amount_local_currency', 'local_currency', 'user_name',
            'fiscal_year', 'posting_period', 'text', 'segment',
            'clearing_document', 'offsetting_account', 'invoice_reference',
            'sales_document', 'assignment', 'year_month', 'created_at',
            'data_file__file_name', 'data_file__id'
        )
    
    def list(self, request, *args, **kwargs):
        """Override list to include summary statistics"""
        queryset = self.filter_queryset(self.get_queryset())
        
        # Get paginated results
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            paginated_response = self.get_paginated_response(serializer.data)
            
            # Add summary statistics
            summary = self._get_summary_statistics(queryset)
            paginated_response.data['summary'] = SAPGLPostingSummarySerializer(summary).data
            
            return paginated_response
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    
    def _get_summary_statistics(self, queryset):
        """Get summary statistics for the filtered queryset"""
        try:
            # Basic counts
            total_records = queryset.count()
            
            # Amount statistics
            amount_stats = queryset.aggregate(
                total_amount=Sum('amount_local_currency'),
                avg_amount=Avg('amount_local_currency'),
                min_amount=Min('amount_local_currency'),
                max_amount=Max('amount_local_currency')
            )
            
            # Unique counts
            unique_accounts = queryset.values('gl_account').distinct().count()
            unique_users = queryset.values('user_name').distinct().count()
            unique_documents = queryset.values('document_number').distinct().count()
            
            # Date range
            date_range = queryset.aggregate(
                min_date=Min('posting_date'),
                max_date=Max('posting_date')
            )
            
            # Top accounts by amount
            top_accounts = queryset.values('gl_account').annotate(
                total_amount=Sum('amount_local_currency'),
                count=Count('id')
            ).order_by('-total_amount')[:10]
            
            # Top users by count
            top_users = queryset.values('user_name').annotate(
                count=Count('id'),
                total_amount=Sum('amount_local_currency')
            ).order_by('-count')[:10]
            
            return {
                'total_records': total_records,
                'total_amount': amount_stats['total_amount'] or Decimal('0'),
                'unique_accounts': unique_accounts,
                'unique_users': unique_users,
                'unique_documents': unique_documents,
                'date_range': {
                    'min_date': date_range['min_date'],
                    'max_date': date_range['max_date']
                },
                'amount_range': {
                    'min_amount': amount_stats['min_amount'],
                    'max_amount': amount_stats['max_amount'],
                    'avg_amount': amount_stats['avg_amount']
                },
                'top_accounts': list(top_accounts),
                'top_users': list(top_users)
            }
            
        except Exception as e:
            logger.error(f"Error calculating summary statistics: {e}")
            return {
                'total_records': 0,
                'total_amount': Decimal('0'),
                'unique_accounts': 0,
                'unique_users': 0,
                'unique_documents': 0,
                'date_range': {'min_date': None, 'max_date': None},
                'amount_range': {'min_amount': None, 'max_amount': None, 'avg_amount': None},
                'top_accounts': [],
                'top_users': []
            }


class TrialBalanceListView(generics.ListAPIView):
    """
    List Trial Balance records with filtering and pagination
    """
    
    serializer_class = TrialBalanceListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'company_code': ['exact', 'icontains'],
        'gl_account': ['exact', 'icontains'],
        'short_text': ['icontains'],
        'currency': ['exact'],
        'opening_balance': ['exact', 'gte', 'lte', 'range'],
        'debit': ['exact', 'gte', 'lte', 'range'],
        'credit': ['exact', 'gte', 'lte', 'range'],
        'closing_balance': ['exact', 'gte', 'lte', 'range'],
        'data_file': ['exact'],
    }
    
    search_fields = ['gl_account', 'short_text', 'company_code']
    ordering_fields = ['gl_account', 'opening_balance', 'closing_balance', 'created_at']
    ordering = ['gl_account']
    
    def get_queryset(self):
        return TrialBalance.objects.select_related('data_file').only(
            'id', 'company_code', 'gl_account', 'short_text', 'currency',
            'opening_balance', 'debit', 'credit', 'closing_balance',
            'created_at', 'data_file__file_name', 'data_file__id'
        )


class ChartOfAccountListView(generics.ListAPIView):
    """
    List Chart of Accounts records with filtering and pagination
    """
    
    serializer_class = ChartOfAccountListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'account': ['exact', 'icontains'],
        'type': ['exact', 'icontains'],
        'sub_type': ['exact', 'icontains'],
        'sub_sub_type': ['exact', 'icontains'],
        'company': ['exact', 'icontains'],
        'branch': ['exact', 'icontains'],
        'cost_center': ['exact', 'icontains'],
        'fiscal_year': ['exact', 'gte', 'lte'],
        'ref_to_fs': ['exact', 'icontains'],
        'data_file': ['exact'],
    }
    
    search_fields = [
        'account', 'type', 'sub_type', 'sub_sub_type', 
        'gl_account_long_text', 'company', 'branch'
    ]
    ordering_fields = ['account', 'type', 'sub_type', 'fiscal_year', 'created_at']
    ordering = ['account']
    
    def get_queryset(self):
        return ChartOfAccount.objects.select_related('data_file').only(
            'id', 'account', 'type', 'sub_type', 'sub_sub_type',
            'gl_account', 'company', 'branch', 'cost_center', 'code',
            'gl_account_long_text', 'ref_to_fs', 'financial_statement',
            'ref_to_note', 'fiscal_year', 'q1_amount', 'adj_reclas',
            'fin_q1_amount', 'created_at', 'data_file__file_name', 'data_file__id'
        )


class DataFileListView(generics.ListAPIView):
    """
    List uploaded data files grouped by engagement
    """
    
    serializer_class = DataFileListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'engagement_id': ['exact', 'icontains'],
        'client_name': ['exact', 'icontains'],
        'company_name': ['exact', 'icontains'],
        'fiscal_year': ['exact', 'gte', 'lte'],
        'status': ['exact'],
        'uploaded_at': ['exact', 'gte', 'lte', 'range'],
    }
    
    search_fields = ['file_name', 'engagement_id', 'client_name', 'company_name']
    ordering_fields = ['uploaded_at', 'file_name', 'fiscal_year', 'total_records']
    ordering = ['-uploaded_at']
    
    def get_queryset(self):
        return DataFile.objects.annotate(
            gl_count=Count('sapglposting', distinct=True),
            tb_count=Count('trialbalance', distinct=True),
            chart_count=Count('chartofaccount', distinct=True)
        ).only(
            'id', 'file_name', 'file_size', 'engagement_id', 'client_name',
            'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date',
            'status', 'total_records', 'processed_records', 'failed_records',
            'uploaded_at', 'processed_at'
        )
    
    def list(self, request, *args, **kwargs):
        """
        Override list to group data files by engagement
        """
        queryset = self.filter_queryset(self.get_queryset())
        
        # Group by engagement_id
        engagements = {}
        for data_file in queryset:
            engagement_id = data_file.engagement_id
            if engagement_id not in engagements:
                engagements[engagement_id] = {
                    'engagement_id': engagement_id,
                    'client_name': data_file.client_name,
                    'company_name': data_file.company_name,
                    'fiscal_year': data_file.fiscal_year,
                    'audit_start_date': data_file.audit_start_date,
                    'audit_end_date': data_file.audit_end_date,
                    'total_files': 0,
                    'total_records': 0,
                    'processed_records': 0,
                    'failed_records': 0,
                    'files': [],
                    'status_summary': {
                        'PENDING': 0,
                        'PROCESSING': 0,
                        'COMPLETED': 0,
                        'FAILED': 0,
                        'PARTIAL': 0
                    }
                }
            
            # Update engagement totals
            engagement = engagements[engagement_id]
            engagement['total_files'] += 1
            engagement['total_records'] += data_file.total_records or 0
            engagement['processed_records'] += data_file.processed_records or 0
            engagement['failed_records'] += data_file.failed_records or 0
            engagement['status_summary'][data_file.status] += 1
            
            # Add file details
            file_data = {
                'id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': data_file.status,
                'total_records': data_file.total_records,
                'processed_records': data_file.processed_records,
                'failed_records': data_file.failed_records,
                'uploaded_at': data_file.uploaded_at,
                'processed_at': data_file.processed_at,
                'gl_count': getattr(data_file, 'gl_count', 0),
                'tb_count': getattr(data_file, 'tb_count', 0),
                'chart_count': getattr(data_file, 'chart_count', 0)
            }
            engagement['files'].append(file_data)
        
        # Convert to list and sort by engagement_id
        result = list(engagements.values())
        result.sort(key=lambda x: x['engagement_id'])
        
        # Apply pagination if needed
        page = self.paginate_queryset(result)
        if page is not None:
            return self.get_paginated_response(page)
        
        return Response(result)


class GLPostingExportView(generics.ListAPIView):
    """
    Export GL postings to CSV/Excel format
    Optimized for large datasets
    """
    
    serializer_class = SAPGLPostingListSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Same filters as GLPostingListView
    filterset_fields = {
        'document_number': ['exact', 'icontains'],
        'document_type': ['exact', 'icontains'],
        'gl_account': ['exact', 'icontains'],
        'profit_center': ['exact', 'icontains'],
        'user_name': ['exact', 'icontains'],
        'local_currency': ['exact'],
        'fiscal_year': ['exact', 'gte', 'lte'],
        'posting_period': ['exact', 'gte', 'lte'],
        'segment': ['exact', 'icontains'],
        'posting_date': ['exact', 'gte', 'lte', 'range'],
        'amount_local_currency': ['exact', 'gte', 'lte', 'range'],
        'data_file': ['exact'],
    }
    
    search_fields = [
        'document_number', 'gl_account', 'user_name', 'text', 
        'clearing_document', 'invoice_reference', 'sales_document'
    ]
    
    ordering_fields = [
        'posting_date', 'document_number', 'amount_local_currency', 
        'gl_account', 'user_name', 'fiscal_year'
    ]
    ordering = ['-posting_date']
    
    def get_queryset(self):
        return SAPGLPosting.objects.select_related('data_file').only(
            'id', 'document_number', 'document_type', 'posting_date',
            'document_date', 'entry_date', 'gl_account', 'profit_center',
            'amount_local_currency', 'local_currency', 'user_name',
            'fiscal_year', 'posting_period', 'text', 'segment',
            'clearing_document', 'offsetting_account', 'invoice_reference',
            'sales_document', 'assignment', 'year_month', 'created_at',
            'data_file__file_name', 'data_file__id'
        )
    
    def list(self, request, *args, **kwargs):
        """Export data without pagination"""
        queryset = self.filter_queryset(self.get_queryset())
        
        # Limit export to prevent memory issues
        max_export_records = 100000  # 100k records max
        if queryset.count() > max_export_records:
            return Response(
                {'error': f'Export limited to {max_export_records} records. Please use filters to reduce the dataset.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'data': serializer.data,
            'total_records': len(serializer.data),
            'exported_at': timezone.now().isoformat()
        })
