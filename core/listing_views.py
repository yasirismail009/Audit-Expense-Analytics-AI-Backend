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

from .models import SAPGLPosting, TrialBalance, ChartOfAccount, DataFile, Client, Engagement
from .listing_serializers import (
    SAPGLPostingListSerializer, SAPGLPostingSummarySerializer,
    TrialBalanceListSerializer, ChartOfAccountListSerializer, DataFileListSerializer
)
from .serializers import ClientListSerializer, EngagementListSerializer

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
        # New engagement-based filters
        'engagement__engagement_id': ['exact', 'icontains'],
        'engagement__client__client_name': ['exact', 'icontains'],
        'engagement__client__company_name': ['exact', 'icontains'],
        'engagement__fiscal_year': ['exact', 'gte', 'lte'],
        'engagement__status': ['exact'],
        # Legacy filters for backward compatibility
        'legacy_engagement_id': ['exact', 'icontains'],
        'legacy_client_name': ['exact', 'icontains'],
        'legacy_company_name': ['exact', 'icontains'],
        'legacy_fiscal_year': ['exact', 'gte', 'lte'],
        # File-specific filters
        'file_type': ['exact'],
        'status': ['exact'],
        'is_validated': ['exact'],
        'uploaded_at': ['exact', 'gte', 'lte', 'range'],
    }
    
    search_fields = [
        'file_name', 'file_type',
        # Engagement fields
        'engagement__engagement_id', 'engagement__engagement_name',
        'engagement__client__client_name', 'engagement__client__company_name', 'engagement__client__client_code',
        # Legacy fields
        'legacy_engagement_id', 'legacy_client_name', 'legacy_company_name'
    ]
    ordering_fields = ['uploaded_at', 'file_name', 'file_size', 'total_records', 'engagement__fiscal_year', 'legacy_fiscal_year']
    ordering = ['-uploaded_at']
    
    def get_queryset(self):
        return DataFile.objects.select_related(
            'engagement',
            'engagement__client'
        ).annotate(
            gl_count=Count('sapglposting', distinct=True),
            tb_count=Count('trial_balances', distinct=True),
            chart_count=Count('chart_of_accounts', distinct=True)
        ).only(
            # DataFile fields
            'id', 'file_name', 'file_size', 'file_type', 'is_validated',
            'status', 'total_records', 'processed_records', 'failed_records',
            'uploaded_at', 'processed_at', 'error_message',
            # Legacy fields for backward compatibility
            'legacy_engagement_id', 'legacy_client_name', 'legacy_company_name', 'legacy_fiscal_year',
            'legacy_audit_start_date', 'legacy_audit_end_date',
            # Engagement fields
            'engagement__engagement_id', 'engagement__engagement_name', 'engagement__status',
            'engagement__fiscal_year', 'engagement__audit_start_date', 'engagement__audit_end_date',
            # Client fields
            'engagement__client__client_code', 'engagement__client__client_name', 'engagement__client__company_name'
        )
    
    def list(self, request, *args, **kwargs):
        """
        Override list to group data files by engagement
        """
        queryset = self.filter_queryset(self.get_queryset())
        
        # Group by engagement
        engagements = {}
        for data_file in queryset:
            # Get engagement info (prefer actual engagement object, fallback to legacy fields)
            if data_file.engagement:
                engagement_id = data_file.engagement.engagement_id
                engagement_name = data_file.engagement.engagement_name
                client_name = data_file.engagement.client.client_name if data_file.engagement.client else 'Unknown Client'
                client_code = data_file.engagement.client.client_code if data_file.engagement.client else 'N/A'
                company_name = data_file.engagement.client.company_name if data_file.engagement.client else 'Unknown Company'
                fiscal_year = data_file.engagement.fiscal_year
                audit_start_date = data_file.engagement.audit_start_date
                audit_end_date = data_file.engagement.audit_end_date
                engagement_status = data_file.engagement.status
            else:
                # Fallback to legacy fields
                engagement_id = data_file.legacy_engagement_id or 'legacy-' + str(data_file.id)
                engagement_name = f"Legacy Engagement - {data_file.legacy_client_name}"
                client_name = data_file.legacy_client_name or 'Unknown Client'
                client_code = 'LEGACY'
                company_name = data_file.legacy_company_name or 'Unknown Company'
                fiscal_year = data_file.legacy_fiscal_year
                audit_start_date = data_file.legacy_audit_start_date
                audit_end_date = data_file.legacy_audit_end_date
                engagement_status = 'LEGACY'
            
            if engagement_id not in engagements:
                engagements[engagement_id] = {
                    'engagement_id': engagement_id,
                    'engagement_name': engagement_name,
                    'engagement_status': engagement_status,
                    'client_name': client_name,
                    'client_code': client_code,
                    'company_name': company_name,
                    'fiscal_year': fiscal_year,
                    'audit_start_date': audit_start_date,
                    'audit_end_date': audit_end_date,
                    'total_files': 0,
                    'total_records': 0,
                    'processed_records': 0,
                    'failed_records': 0,
                    'files': [],
                    'file_types': set(),
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
            engagement['file_types'].add(data_file.file_type)
            
            # Add file details
            file_data = {
                'id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'file_type': data_file.file_type,
                'is_validated': data_file.is_validated,
                'status': data_file.status,
                'total_records': data_file.total_records,
                'processed_records': data_file.processed_records,
                'failed_records': data_file.failed_records,
                'uploaded_at': data_file.uploaded_at,
                'processed_at': data_file.processed_at,
                'error_message': data_file.error_message,
                'gl_count': getattr(data_file, 'gl_count', 0),
                'tb_count': getattr(data_file, 'tb_count', 0),
                'chart_count': getattr(data_file, 'chart_count', 0)
            }
            engagement['files'].append(file_data)
        
        # Convert file_types set to list for JSON serialization
        for engagement in engagements.values():
            engagement['file_types'] = list(engagement['file_types'])
        
        # Convert to list and sort by engagement_id
        result = list(engagements.values())
        result.sort(key=lambda x: (x['fiscal_year'], x['client_name'], x['engagement_id']))
        
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


class ClientListView(generics.ListAPIView):
    """
    List all clients with filtering and search capabilities
    """
    
    serializer_class = ClientListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'client_code': ['exact', 'icontains'],
        'client_name': ['exact', 'icontains'],
        'company_name': ['exact', 'icontains'],
        'industry': ['exact', 'icontains'],
        'country': ['exact', 'icontains'],
        'currency': ['exact'],
        'is_active': ['exact'],
        'created_at': ['exact', 'gte', 'lte', 'range'],
    }
    
    search_fields = [
        'client_code', 'client_name', 'company_name', 'industry',
        'primary_contact_name', 'primary_contact_email', 'tax_id', 'registration_number'
    ]
    
    ordering_fields = ['client_code', 'client_name', 'company_name', 'created_at', 'updated_at']
    ordering = ['client_name']
    
    def get_queryset(self):
        return Client.objects.annotate(
            total_engagements=Count('engagements', distinct=True),
            active_engagements=Count('engagements', filter=Q(engagements__status__in=['ACTIVE', 'IN_PROGRESS']), distinct=True),
            completed_engagements=Count('engagements', filter=Q(engagements__status='COMPLETED'), distinct=True)
        ).only(
            'id', 'client_code', 'client_name', 'company_name', 'industry',
            'country', 'currency', 'primary_contact_name', 'primary_contact_email',
            'is_active', 'created_at', 'updated_at'
        )


class EngagementListView(generics.ListAPIView):
    """
    List all engagements with client information and filtering
    """
    
    serializer_class = EngagementListSerializer
    pagination_class = OptimizedPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    filterset_fields = {
        'engagement_id': ['exact', 'icontains'],
        'engagement_name': ['exact', 'icontains'],
        'client__client_code': ['exact', 'icontains'],
        'client__client_name': ['exact', 'icontains'],
        'fiscal_year': ['exact', 'gte', 'lte'],
        'status': ['exact'],
        'engagement_type': ['exact'],
        'audit_opinion': ['exact'],
        'created_at': ['exact', 'gte', 'lte', 'range'],
        'audit_start_date': ['exact', 'gte', 'lte', 'range'],
        'audit_end_date': ['exact', 'gte', 'lte', 'range'],
    }
    
    search_fields = [
        'engagement_id', 'engagement_name', 'description',
        'client__client_code', 'client__client_name', 'client__company_name',
        'engagement_partner', 'engagement_manager'
    ]
    
    ordering_fields = [
        'engagement_id', 'engagement_name', 'fiscal_year', 'status',
        'audit_start_date', 'audit_end_date', 'created_at', 'updated_at'
    ]
    ordering = ['-fiscal_year', 'client__client_name', 'engagement_id']
    
    def get_queryset(self):
        return Engagement.objects.select_related('client').annotate(
            total_files=Count('data_files', distinct=True),
            completed_files=Count('data_files', filter=Q(data_files__status='COMPLETED'), distinct=True),
            pending_files=Count('data_files', filter=Q(data_files__status='PENDING'), distinct=True),
            total_records=Sum('data_files__total_records'),
            latest_completeness_score=Max('completeness_tests__completeness_score')
        ).only(
            'id', 'engagement_id', 'engagement_name', 'fiscal_year', 'status',
            'engagement_type', 'audit_opinion', 'audit_start_date', 'audit_end_date',
            'engagement_partner', 'engagement_manager', 'completeness_threshold',
            'created_at', 'updated_at',
            # Client fields
            'client__id', 'client__client_code', 'client__client_name', 'client__company_name'
        )
