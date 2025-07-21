from rest_framework import viewsets, status, generics
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
import pandas as pd
import csv
import io
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from django.db.models import Sum, Avg, Count, Min, Max
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.db.models.manager import Manager
    # Type hints for Django models
    SAPGLPostingManager = Manager
    DataFileManager = Manager
    AnalysisSessionManager = Manager
    TransactionAnalysisManager = Manager

from .models import SAPGLPosting, DataFile, AnalysisSession, TransactionAnalysis, SystemMetrics, GLAccount
from .serializers import (
    SAPGLPostingSerializer, SAPGLPostingListSerializer,
    DataFileSerializer, DataFileUploadSerializer,
    AnalysisSessionSerializer, AnalysisSessionCreateSerializer,
    TransactionAnalysisSerializer, TransactionAnalysisListSerializer,
    SystemMetricsSerializer, AnalysisRequestSerializer,
    AnalysisSummarySerializer, DataUploadResponseSerializer,
    DashboardStatsSerializer, GLAccountSerializer, GLAccountListSerializer,
    GLAccountAnalysisSerializer, TrialBalanceSerializer, GLAccountChartSerializer,
    GLAccountUploadSerializer, DuplicateAnomalySerializer, DuplicateAnomalyListSerializer,
    DuplicateChartDataSerializer, DuplicateAnalysisRequestSerializer, DuplicateTrainingDataSerializer
)
from .analytics import SAPGLAnalyzer

logger = logging.getLogger(__name__)

class SAPGLPostingViewSet(viewsets.ModelViewSet):
    """ViewSet for SAP GL Posting data"""
    
    queryset = SAPGLPosting.objects.all()
    serializer_class = SAPGLPostingSerializer
    
    def get_serializer_class(self):
        if self.action == 'list':
            return SAPGLPostingListSerializer
        return SAPGLPostingSerializer
    
    def get_queryset(self):
        # type: ignore
        queryset = SAPGLPosting.objects.all()
        
        # Apply filters
        document_number = self.request.query_params.get('document_number', None)
        if document_number:
            queryset = queryset.filter(document_number__icontains=document_number)
        
        document_type = self.request.query_params.get('document_type', None)
        if document_type:
            queryset = queryset.filter(document_type=document_type)
        
        gl_account = self.request.query_params.get('gl_account', None)
        if gl_account:
            queryset = queryset.filter(gl_account__icontains=gl_account)
        
        profit_center = self.request.query_params.get('profit_center', None)
        if profit_center:
            queryset = queryset.filter(profit_center__icontains=profit_center)
        
        user_name = self.request.query_params.get('user_name', None)
        if user_name:
            queryset = queryset.filter(user_name__icontains=user_name)
        
        date_from = self.request.query_params.get('date_from', None)
        if date_from:
            queryset = queryset.filter(posting_date__gte=date_from)
        
        date_to = self.request.query_params.get('date_to', None)
        if date_to:
            queryset = queryset.filter(posting_date__lte=date_to)
        
        min_amount = self.request.query_params.get('min_amount', None)
        if min_amount:
            queryset = queryset.filter(amount_local_currency__gte=min_amount)
        
        max_amount = self.request.query_params.get('max_amount', None)
        if max_amount:
            queryset = queryset.filter(amount_local_currency__lte=max_amount)
        
        fiscal_year = self.request.query_params.get('fiscal_year', None)
        if fiscal_year:
            queryset = queryset.filter(fiscal_year=fiscal_year)
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def statistics(self, request):
        """Get statistics for SAP GL postings"""
        queryset = self.get_queryset()
        
        stats = {
            'total_transactions': queryset.count(),
            'total_amount': float(queryset.aggregate(total=Sum('amount_local_currency'))['total'] or 0),
            'avg_amount': float(queryset.aggregate(avg=Avg('amount_local_currency'))['avg'] or 0),
            'high_value_transactions': queryset.filter(amount_local_currency__gt=1000000).count(),
            'unique_users': queryset.values('user_name').distinct().count(),
            'unique_accounts': queryset.values('gl_account').distinct().count(),
            'unique_profit_centers': queryset.values('profit_center').distinct().count(),
            'date_range': {
                'min_date': queryset.aggregate(min_date=Min('posting_date'))['min_date'],
                'max_date': queryset.aggregate(max_date=Max('posting_date'))['max_date']
            }
        }
        
        return Response(stats)
    
    @action(detail=False, methods=['get'])
    def top_users(self, request):
        """Get top users by transaction count and amount"""
        queryset = self.get_queryset()
        
        top_users = queryset.values('user_name').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency')
        ).order_by('-total_amount')[:10]
        
        return Response(list(top_users))
    
    @action(detail=False, methods=['get'])
    def top_accounts(self, request):
        """Get top G/L accounts by transaction count and amount"""
        queryset = self.get_queryset()
        
        top_accounts = queryset.values('gl_account').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency')
        ).order_by('-total_amount')[:10]
        
        return Response(list(top_accounts))

class DataFileViewSet(viewsets.ModelViewSet):
    """ViewSet for uploaded data files"""
    
    queryset = DataFile.objects.all()
    serializer_class = DataFileSerializer
    parser_classes = (MultiPartParser, FormParser)
    
    @action(detail=False, methods=['post'])
    def upload(self, request):
        """Upload and process CSV file"""
        try:
            file_obj = request.FILES.get('file')
            if not file_obj:
                return Response(
                    {'error': 'No file provided'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get additional fields from request data
            engagement_id = request.data.get('engagement_id', '')
            client_name = request.data.get('client_name', '')
            company_name = request.data.get('company_name', '')
            fiscal_year = request.data.get('fiscal_year', 2025)
            audit_start_date = request.data.get('audit_start_date')
            audit_end_date = request.data.get('audit_end_date')
            
            # Validate file type
            if not file_obj.name.endswith('.csv'):
                return Response(
                    {'error': 'Only CSV files are supported'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Create DataFile record with new fields
            data_file = DataFile.objects.create(
                file_name=file_obj.name,
                file_size=file_obj.size,
                engagement_id=engagement_id,
                client_name=client_name,
                company_name=company_name,
                fiscal_year=fiscal_year,
                audit_start_date=audit_start_date,
                audit_end_date=audit_end_date,
                status='PENDING'
            )
            
            # Process file in background (for now, process synchronously)
            result = self._process_csv_file(data_file, file_obj)
            
            if result['success']:
                return Response(
                    DataUploadResponseSerializer(data_file).data,
                    status=status.HTTP_201_CREATED
                )
            else:
                return Response(
                    {'error': result['error']}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
                
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _process_csv_file(self, data_file, file_obj):
        """Process uploaded CSV file"""
        try:
            # Update status
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Read CSV file
            content = file_obj.read().decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content))
            
            # Process rows
            processed_count = 0
            failed_count = 0
            min_date = None
            max_date = None
            min_amount = None
            max_amount = None
            
            for row in csv_reader:
                try:
                    # Map CSV columns to model fields
                    posting = self._create_posting_from_row(row)
                    if posting:
                        processed_count += 1
                        
                        # Update date and amount ranges
                        if posting.posting_date:
                            if min_date is None or posting.posting_date < min_date:
                                min_date = posting.posting_date
                            if max_date is None or posting.posting_date > max_date:
                                max_date = posting.posting_date
                        
                        if min_amount is None or posting.amount_local_currency < min_amount:
                            min_amount = posting.amount_local_currency
                        if max_amount is None or posting.amount_local_currency > max_amount:
                            max_amount = posting.amount_local_currency
                except Exception as e:
                    logger.error(f"Error processing row: {e}")
                    failed_count += 1
            
            # Update DataFile record
            data_file.total_records = processed_count + failed_count
            data_file.processed_records = processed_count
            data_file.failed_records = failed_count
            data_file.status = 'COMPLETED' if failed_count == 0 else 'PARTIAL'
            data_file.processed_at = timezone.now()
            data_file.min_date = min_date
            data_file.max_date = max_date
            data_file.min_amount = min_amount
            data_file.max_amount = max_amount
            data_file.save()
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {'success': False, 'error': str(e)}
    
    def _create_posting_from_row(self, row):
        """Create SAPGLPosting from CSV row"""
        try:
            # Map CSV columns to model fields
            # This mapping should be adjusted based on actual CSV structure
            posting = SAPGLPosting(
                document_number=row.get('Document', ''),
                document_type=row.get('Document type', ''),
                amount_local_currency=Decimal(row.get('Amount in Local Currency', '0').replace(',', '')),
                local_currency=row.get('Local Currency', 'SAR'),
                gl_account=row.get('G/L Account', ''),
                profit_center=row.get('Profit Center', ''),
                user_name=row.get('User Name', ''),
                fiscal_year=int(row.get('Fiscal Year', '2025')),
                posting_period=int(row.get('Posting period', '1')),
                text=row.get('Text', ''),
                segment=row.get('Segment', ''),
                clearing_document=row.get('Clearing Document', ''),
                offsetting_account=row.get('Offsetting', ''),
                invoice_reference=row.get('Invoice Reference', ''),
                sales_document=row.get('Sales Document', ''),
                assignment=row.get('Assignment', ''),
                year_month=row.get('Year/Month', '')
            )
            
            # Parse dates
            posting_date_str = row.get('Posting Date', '')
            if posting_date_str:
                try:
                    parsed_date = datetime.strptime(posting_date_str, '%m/%d/%Y').date()
                    posting.posting_date = parsed_date
                except:
                    pass
            
            document_date_str = row.get('Document Date', '')
            if document_date_str:
                try:
                    parsed_date = datetime.strptime(document_date_str, '%m/%d/%Y').date()
                    posting.document_date = parsed_date
                except:
                    pass
            
            entry_date_str = row.get('Entry Date', '')
            if entry_date_str:
                try:
                    parsed_date = datetime.strptime(entry_date_str, '%m/%d/%Y').date()
                    posting.entry_date = parsed_date
                except:
                    pass
            
            return posting
            
        except Exception as e:
            logger.error(f"Error creating posting from row: {e}")
            return None

class AnalysisSessionViewSet(viewsets.ModelViewSet):
    """ViewSet for analysis sessions"""
    
    queryset = AnalysisSession.objects.all()
    serializer_class = AnalysisSessionSerializer
    
    def get_serializer_class(self):
        if self.action == 'create':
            return AnalysisSessionCreateSerializer
        return AnalysisSessionSerializer
    
    @action(detail=True, methods=['post'])
    def run_analysis(self, request, pk=None):
        """Run analysis for a session"""
        try:
            session = self.get_object()
            
            # Check if session is already running or completed
            if session.status in ['RUNNING', 'COMPLETED']:
                return Response(
                    {'error': f'Analysis session is already {session.status.lower()}'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Run analysis
            analyzer = SAPGLAnalyzer()
            result = analyzer.analyze_transactions(session)
            
            if 'error' in result:
                return Response(
                    {'error': result['error']},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            return Response(result)
            
        except Exception as e:
            logger.error(f"Error running analysis: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=True, methods=['get'])
    def summary(self, request, pk=None):
        """Get analysis summary"""
        try:
            session = self.get_object()
            analyzer = SAPGLAnalyzer()
            summary = analyzer.get_analysis_summary(session)
            
            return Response(AnalysisSummarySerializer(summary).data)
            
        except Exception as e:
            logger.error(f"Error getting summary: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class TransactionAnalysisViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for transaction analysis results"""
    
    queryset = TransactionAnalysis.objects.all()
    serializer_class = TransactionAnalysisSerializer
    
    def get_serializer_class(self):
        if self.action == 'list':
            return TransactionAnalysisListSerializer
        return TransactionAnalysisSerializer
    
    def get_queryset(self):
        queryset = TransactionAnalysis.objects.all()
        
        # Apply filters
        session_id = self.request.query_params.get('session_id', None)
        if session_id:
            queryset = queryset.filter(session_id=session_id)
        
        risk_level = self.request.query_params.get('risk_level', None)
        if risk_level:
            queryset = queryset.filter(risk_level=risk_level)
        
        min_risk_score = self.request.query_params.get('min_risk_score', None)
        if min_risk_score:
            queryset = queryset.filter(risk_score__gte=min_risk_score)
        
        max_risk_score = self.request.query_params.get('max_risk_score', None)
        if max_risk_score:
            queryset = queryset.filter(risk_score__lte=max_risk_score)
        
        # Filter by anomaly types
        amount_anomaly = self.request.query_params.get('amount_anomaly', None)
        if amount_anomaly:
            queryset = queryset.filter(amount_anomaly=amount_anomaly.lower() == 'true')
        
        timing_anomaly = self.request.query_params.get('timing_anomaly', None)
        if timing_anomaly:
            queryset = queryset.filter(timing_anomaly=timing_anomaly.lower() == 'true')
        
        user_anomaly = self.request.query_params.get('user_anomaly', None)
        if user_anomaly:
            queryset = queryset.filter(user_anomaly=user_anomaly.lower() == 'true')
        
        account_anomaly = self.request.query_params.get('account_anomaly', None)
        if account_anomaly:
            queryset = queryset.filter(account_anomaly=account_anomaly.lower() == 'true')
        
        pattern_anomaly = self.request.query_params.get('pattern_anomaly', None)
        if pattern_anomaly:
            queryset = queryset.filter(pattern_anomaly=pattern_anomaly.lower() == 'true')
        
        return queryset.order_by('-risk_score', '-created_at')

class GLAccountViewSet(viewsets.ModelViewSet):
    """ViewSet for GL Account management and analysis"""
    
    queryset = GLAccount.objects.all()
    serializer_class = GLAccountSerializer
    
    def get_serializer_class(self):
        if self.action == 'list':
            return GLAccountListSerializer
        return GLAccountSerializer
    
    def get_queryset(self):
        queryset = GLAccount.objects.all()
        
        # Apply filters
        account_id = self.request.query_params.get('account_id', None)
        if account_id:
            queryset = queryset.filter(account_id__icontains=account_id)
        
        account_name = self.request.query_params.get('account_name', None)
        if account_name:
            queryset = queryset.filter(account_name__icontains=account_name)
        
        account_type = self.request.query_params.get('account_type', None)
        if account_type:
            queryset = queryset.filter(account_type=account_type)
        
        account_category = self.request.query_params.get('account_category', None)
        if account_category:
            queryset = queryset.filter(account_category=account_category)
        
        normal_balance = self.request.query_params.get('normal_balance', None)
        if normal_balance:
            queryset = queryset.filter(normal_balance=normal_balance)
        
        is_active = self.request.query_params.get('is_active', None)
        if is_active:
            queryset = queryset.filter(is_active=is_active.lower() == 'true')
        
        return queryset.order_by('account_id')
    
    @action(detail=False, methods=['get'])
    def analysis(self, request):
        """Get comprehensive analysis for all GL accounts"""
        try:
            accounts = self.get_queryset()
            analysis_results = []
            
            print(f"Debug: Found {accounts.count()} GL accounts")  # Debug log
            
            for account in accounts:
                try:
                    # Get account postings using the gl_account field (not the relationship)
                    postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                    
                    print(f"Debug: Account {account.account_id} has {postings.count()} postings")  # Debug log
                    
                    if not postings.exists():
                        print(f"Debug: Account {account.account_id} has no postings, skipping")  # Debug log
                        continue
                    
                    # Calculate statistics
                    total_debits = postings.filter(transaction_type='DEBIT').aggregate(
                        total=Sum('amount_local_currency')
                    )['total'] or Decimal('0.00')
                    
                    total_credits = postings.filter(transaction_type='CREDIT').aggregate(
                        total=Sum('amount_local_currency')
                    )['total'] or Decimal('0.00')
                    
                    debit_count = postings.filter(transaction_type='DEBIT').count()
                    credit_count = postings.filter(transaction_type='CREDIT').count()
                    
                    # Risk analysis
                    high_value_count = postings.filter(amount_local_currency__gt=1000000).count()
                    
                    # Get analysis records for this account
                    analysis_records = TransactionAnalysis.objects.filter(
                        transaction__gl_account=account.account_id
                    )
                    flagged_count = analysis_records.filter(risk_level__in=['HIGH', 'CRITICAL']).count()
                    
                    # Calculate average risk score
                    avg_risk_score = analysis_records.aggregate(
                        avg_score=Avg('risk_score')
                    )['avg_score'] or 0.0
                    
                    # Activity analysis
                    first_transaction = postings.order_by('posting_date').first()
                    last_transaction = postings.order_by('-posting_date').first()
                    
                    avg_amount = postings.aggregate(
                        avg_amount=Avg('amount_local_currency')
                    )['avg_amount'] or Decimal('0.00')
                    
                    max_amount = postings.aggregate(
                        max_amount=Max('amount_local_currency')
                    )['max_amount'] or Decimal('0.00')
                    
                    analysis_results.append({
                        'account_id': account.account_id,
                        'account_name': account.account_name,
                        'account_type': account.account_type,
                        'account_category': account.account_category,
                        'normal_balance': account.normal_balance,
                        'current_balance': float(account.current_balance),
                        'total_debits': float(total_debits),
                        'total_credits': float(total_credits),
                        'transaction_count': postings.count(),
                        'debit_count': debit_count,
                        'credit_count': credit_count,
                        'high_value_transactions': high_value_count,
                        'flagged_transactions': flagged_count,
                        'risk_score': round(avg_risk_score, 2),
                        'first_transaction_date': first_transaction.posting_date if first_transaction else None,
                        'last_transaction_date': last_transaction.posting_date if last_transaction else None,
                        'avg_transaction_amount': float(avg_amount),
                        'max_transaction_amount': float(max_amount)
                    })
                    
                    print(f"Debug: Added analysis for account {account.account_id}")  # Debug log
                    
                except Exception as account_error:
                    logger.error(f"Error processing account {account.account_id}: {account_error}")
                    print(f"Debug: Error processing account {account.account_id}: {account_error}")  # Debug log
                    continue
            
            print(f"Debug: Total analysis results: {len(analysis_results)}")  # Debug log
            
            # Sort by risk score (highest first)
            analysis_results.sort(key=lambda x: x['risk_score'], reverse=True)
            
            return Response(analysis_results)
            
        except Exception as e:
            logger.error(f"Error in GL account analysis: {e}")
            print(f"Debug: GL account analysis error: {e}")  # Debug log
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def trial_balance(self, request):
        """Generate Trial Balance report"""
        try:
            # Get date range from request
            date_from = request.query_params.get('date_from', None)
            date_to = request.query_params.get('date_to', None)
            
            accounts = self.get_queryset()
            trial_balance_data = []
            
            for account in accounts:
                # Get postings for the account using gl_account field
                postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                
                if date_from:
                    postings = postings.filter(posting_date__gte=date_from)
                if date_to:
                    postings = postings.filter(posting_date__lte=date_to)
                
                if not postings.exists():
                    continue
                
                # Calculate period movements
                period_debit = postings.filter(transaction_type='DEBIT').aggregate(
                    total=Sum('amount_local_currency')
                )['total'] or Decimal('0.00')
                
                period_credit = postings.filter(transaction_type='CREDIT').aggregate(
                    total=Sum('amount_local_currency')
                )['total'] or Decimal('0.00')
                
                # Calculate closing balances
                if account.normal_balance == 'DEBIT':
                    closing_debit = period_debit - period_credit
                    closing_credit = Decimal('0.00')
                    if closing_debit < 0:
                        closing_credit = abs(closing_debit)
                        closing_debit = Decimal('0.00')
                else:
                    closing_credit = period_credit - period_debit
                    closing_debit = Decimal('0.00')
                    if closing_credit < 0:
                        closing_debit = abs(closing_credit)
                        closing_credit = Decimal('0.00')
                
                # Net balance
                net_balance = closing_debit - closing_credit
                
                trial_balance_data.append({
                    'account_id': account.account_id,
                    'account_name': account.account_name,
                    'account_type': account.account_type,
                    'account_category': account.account_category,
                    'normal_balance': account.normal_balance,
                    'opening_debit': float(Decimal('0.00')),  # Assuming no opening balances for now
                    'opening_credit': float(Decimal('0.00')),
                    'period_debit': float(period_debit),
                    'period_credit': float(period_credit),
                    'closing_debit': float(closing_debit),
                    'closing_credit': float(closing_credit),
                    'net_balance': float(net_balance),
                    'transaction_count': postings.count()
                })
            
            # Sort by account type and account ID
            trial_balance_data.sort(key=lambda x: (x['account_type'], x['account_id']))
            
            return Response(trial_balance_data)
            
        except Exception as e:
            logger.error(f"Error generating trial balance: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def charts(self, request):
        """Get charts data for GL accounts"""
        try:
            accounts = self.get_queryset()
            
            # Account type distribution
            type_distribution = {}
            category_distribution = {}
            
            for account in accounts:
                # Get postings for this account
                postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                
                # Type distribution
                if account.account_type not in type_distribution:
                    type_distribution[account.account_type] = {
                        'count': 0,
                        'total_balance': Decimal('0.00'),
                        'total_transactions': 0
                    }
                type_distribution[account.account_type]['count'] += 1
                type_distribution[account.account_type]['total_balance'] += account.current_balance
                type_distribution[account.account_type]['total_transactions'] += postings.count()
                
                # Category distribution
                if account.account_category not in category_distribution:
                    category_distribution[account.account_category] = {
                        'count': 0,
                        'total_balance': Decimal('0.00')
                    }
                category_distribution[account.account_category]['count'] += 1
                category_distribution[account.account_category]['total_balance'] += account.current_balance
            
            # Top accounts by balance
            top_accounts_by_balance = []
            for account in accounts:
                postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                if postings.exists():
                    top_accounts_by_balance.append({
                        'account_id': account.account_id,
                        'account_name': account.account_name,
                        'balance': float(account.current_balance),
                        'account_type': account.account_type
                    })
            
            top_accounts_by_balance.sort(key=lambda x: abs(x['balance']), reverse=True)
            top_accounts_by_balance = top_accounts_by_balance[:10]
            
            # Top accounts by transaction count
            top_accounts_by_transactions = []
            for account in accounts:
                postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                transaction_count = postings.count()
                if transaction_count > 0:
                    top_accounts_by_transactions.append({
                        'account_id': account.account_id,
                        'account_name': account.account_name,
                        'transaction_count': transaction_count,
                        'account_type': account.account_type
                    })
            
            top_accounts_by_transactions.sort(key=lambda x: x['transaction_count'], reverse=True)
            top_accounts_by_transactions = top_accounts_by_transactions[:10]
            
            # Debit vs Credit analysis
            total_debits = Decimal('0.00')
            total_credits = Decimal('0.00')
            debit_count = 0
            credit_count = 0
            
            for account in accounts:
                postings = SAPGLPosting.objects.filter(gl_account=account.account_id)
                account_debits = postings.filter(transaction_type='DEBIT').aggregate(
                    total=Sum('amount_local_currency')
                )['total'] or Decimal('0.00')
                account_credits = postings.filter(transaction_type='CREDIT').aggregate(
                    total=Sum('amount_local_currency')
                )['total'] or Decimal('0.00')
                
                total_debits += account_debits
                total_credits += account_credits
                debit_count += postings.filter(transaction_type='DEBIT').count()
                credit_count += postings.filter(transaction_type='CREDIT').count()
            
            debit_credit_analysis = {
                'total_debits': float(total_debits),
                'total_credits': float(total_credits),
                'debit_count': debit_count,
                'credit_count': credit_count,
                'net_movement': float(total_debits - total_credits)
            }
            
            # Risk distribution by account type
            risk_distribution = []
            for account_type, data in type_distribution.items():
                accounts_of_type = accounts.filter(account_type=account_type)
                high_risk_count = 0
                total_risk_score = 0
                
                for account in accounts_of_type:
                    analysis_records = TransactionAnalysis.objects.filter(
                        transaction__gl_account=account.account_id
                    )
                    high_risk_count += analysis_records.filter(risk_level__in=['HIGH', 'CRITICAL']).count()
                    avg_score = analysis_records.aggregate(avg_score=Avg('risk_score'))['avg_score'] or 0
                    total_risk_score += avg_score
                
                avg_risk_score = total_risk_score / len(accounts_of_type) if accounts_of_type.exists() else 0
                
                risk_distribution.append({
                    'account_type': account_type,
                    'account_count': data['count'],
                    'high_risk_count': high_risk_count,
                    'avg_risk_score': round(avg_risk_score, 2)
                })
            
            charts_data = {
                'account_type_distribution': [
                    {
                        'account_type': account_type,
                        'count': data['count'],
                        'total_balance': float(data['total_balance']),
                        'total_transactions': data['total_transactions']
                    }
                    for account_type, data in type_distribution.items()
                ],
                'account_category_distribution': [
                    {
                        'category': category,
                        'count': data['count'],
                        'total_balance': float(data['total_balance'])
                    }
                    for category, data in category_distribution.items()
                ],
                'top_accounts_by_balance': top_accounts_by_balance,
                'top_accounts_by_transactions': top_accounts_by_transactions,
                'debit_credit_analysis': debit_credit_analysis,
                'risk_distribution': risk_distribution
            }
            
            return Response(charts_data)
            
        except Exception as e:
            logger.error(f"Error generating charts data: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def upload_master_data(self, request):
        """Upload GL Account master data from CSV"""
        try:
            serializer = GLAccountUploadSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            file_obj = serializer.validated_data['file']
            
            # Validate file type
            if not file_obj.name.endswith('.csv'):
                return Response(
                    {'error': 'Only CSV files are supported'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Process CSV file
            content = file_obj.read().decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content))
            
            created_count = 0
            updated_count = 0
            failed_count = 0
            
            for row in csv_reader:
                try:
                    account_id = row.get('Account ID', '').strip()
                    if not account_id:
                        failed_count += 1
                        continue
                    
                    account_data = {
                        'account_name': row.get('Account Name', '').strip(),
                        'account_type': row.get('Account Type', '').strip(),
                        'account_category': row.get('Account Category', '').strip(),
                        'account_subcategory': row.get('Account Subcategory', '').strip(),
                        'normal_balance': row.get('Normal Balance', 'DEBIT').strip().upper(),
                        'is_active': row.get('Is Active', 'TRUE').strip().upper() == 'TRUE'
                    }
                    
                    # Create or update account
                    account, created = GLAccount.objects.update_or_create(
                        account_id=account_id,
                        defaults=account_data
                    )
                    
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1
                        
                except Exception as e:
                    logger.error(f"Error processing GL account row: {e}")
                    failed_count += 1
            
            return Response({
                'message': 'GL Account master data uploaded successfully',
                'created_count': created_count,
                'updated_count': updated_count,
                'failed_count': failed_count
            })
            
        except Exception as e:
            logger.error(f"Error uploading GL account master data: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class DashboardView(generics.GenericAPIView):
    """Dashboard view for system overview"""
    
    def get(self, request):
        """Get dashboard statistics"""
        try:
            # Date range (last 30 days)
            end_date = timezone.now().date()
            start_date = end_date - timedelta(days=30)
            
            # Overall statistics
            total_transactions = SAPGLPosting.objects.count()
            total_amount = SAPGLPosting.objects.aggregate(
                total=Sum('amount_local_currency')
            )['total'] or Decimal('0')
            total_files = DataFile.objects.count()
            total_sessions = AnalysisSession.objects.count()
            
            # Recent activity
            recent_transactions = SAPGLPosting.objects.filter(
                created_at__date__gte=start_date
            ).count()
            recent_files = DataFile.objects.filter(
                uploaded_at__date__gte=start_date
            ).count()
            recent_sessions = AnalysisSession.objects.filter(
                created_at__date__gte=start_date
            ).count()
            
            # Risk statistics
            flagged_transactions = TransactionAnalysis.objects.filter(
                risk_level__in=['HIGH', 'CRITICAL']
            ).count()
            high_risk_transactions = TransactionAnalysis.objects.filter(
                risk_level='HIGH'
            ).count()
            critical_risk_transactions = TransactionAnalysis.objects.filter(
                risk_level='CRITICAL'
            ).count()
            
            # Top statistics
            top_users = SAPGLPosting.objects.values('user_name').annotate(
                count=Count('id'),
                total_amount=Sum('amount_local_currency')
            ).order_by('-total_amount')[:5]
            
            top_accounts = SAPGLPosting.objects.values('gl_account').annotate(
                count=Count('id'),
                total_amount=Sum('amount_local_currency')
            ).order_by('-total_amount')[:5]
            
            top_profit_centers = SAPGLPosting.objects.values('profit_center').annotate(
                count=Count('id'),
                total_amount=Sum('amount_local_currency')
            ).order_by('-total_amount')[:5]
            
            top_document_types = SAPGLPosting.objects.values('document_type').annotate(
                count=Count('id'),
                total_amount=Sum('amount_local_currency')
            ).order_by('-total_amount')[:5]
            
            stats = {
                'total_transactions': total_transactions,
                'total_amount': float(total_amount),
                'total_files': total_files,
                'total_sessions': total_sessions,
                'recent_transactions': recent_transactions,
                'recent_files': recent_files,
                'recent_sessions': recent_sessions,
                'flagged_transactions': flagged_transactions,
                'high_risk_transactions': high_risk_transactions,
                'critical_risk_transactions': critical_risk_transactions,
                'top_users': list(top_users),
                'top_accounts': list(top_accounts),
                'top_profit_centers': list(top_profit_centers),
                'top_document_types': list(top_document_types),
                'date_from': start_date,
                'date_to': end_date
            }
            
            return Response(DashboardStatsSerializer(stats).data)
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class FileListView(generics.ListAPIView):
    """View to get list of uploaded files"""
    
    queryset = DataFile.objects.all()
    serializer_class = DataFileSerializer
    
    def get_queryset(self):
        queryset = DataFile.objects.all()
        
        # Apply filters
        status_filter = self.request.query_params.get('status', None)
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        date_from = self.request.query_params.get('date_from', None)
        if date_from:
            queryset = queryset.filter(uploaded_at__date__gte=date_from)
        
        date_to = self.request.query_params.get('date_to', None)
        if date_to:
            queryset = queryset.filter(uploaded_at__date__lte=date_to)
        
        return queryset.order_by('-uploaded_at')
    
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        
        # Get file data with session information
        files_data = []
        serializer_data = serializer.data
        
        for i, file_obj in enumerate(queryset):
            file_data = serializer_data[i].copy()  # Make a copy to avoid modifying the original
            
            # Get related analysis sessions for this file
            related_sessions = AnalysisSession.objects.filter(
                created_at__gte=file_obj.uploaded_at
            ).order_by('-created_at')
            
            # Add session information to file data
            file_data['sessions'] = []
            for session in related_sessions:
                session_data = {
                    'id': str(session.id),
                    'session_name': session.session_name,
                    'status': session.status,
                    'created_at': session.created_at,
                    'started_at': session.started_at,
                    'completed_at': session.completed_at,
                    'total_transactions': session.total_transactions,
                    'flagged_transactions': session.flagged_transactions,
                    'flag_rate': round((session.flagged_transactions / session.total_transactions * 100), 2) if session.total_transactions > 0 else 0
                }
                file_data['sessions'].append(session_data)
            
            files_data.append(file_data)
        
        # Add summary statistics
        total_files = queryset.count()
        total_records = queryset.aggregate(total=Sum('total_records'))['total'] or 0
        total_processed = queryset.aggregate(total=Sum('processed_records'))['total'] or 0
        total_failed = queryset.aggregate(total=Sum('failed_records'))['total'] or 0
        
        # Calculate total sessions across all files
        total_sessions = sum(len(file_data['sessions']) for file_data in files_data)
        
        response_data = {
            'files': files_data,
            'summary': {
                'total_files': total_files,
                'total_records': total_records,
                'total_processed': total_processed,
                'total_failed': total_failed,
                'total_sessions': total_sessions,
                'success_rate': (total_processed / total_records * 100) if total_records > 0 else 0
            }
        }
        
        return Response(response_data)


class FileUploadAnalysisView(generics.CreateAPIView):
    """View to upload file and run analysis"""
    
    parser_classes = (MultiPartParser, FormParser)
    serializer_class = DataFileUploadSerializer
    
    def create(self, request, *args, **kwargs):
        try:
            # Validate the request using serializer
            serializer = self.get_serializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            # Get the validated data
            file_obj = serializer.validated_data['file']
            description = serializer.validated_data.get('description', '')
            engagement_id = serializer.validated_data['engagement_id']
            client_name = serializer.validated_data['client_name']
            company_name = serializer.validated_data['company_name']
            fiscal_year = serializer.validated_data['fiscal_year']
            audit_start_date = serializer.validated_data['audit_start_date']
            audit_end_date = serializer.validated_data['audit_end_date']
            
            # Validate file type
            if not file_obj.name.endswith('.csv'):
                return Response(
                    {'error': 'Only CSV files are supported'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Create DataFile record with new fields
            data_file = DataFile.objects.create(
                file_name=file_obj.name,
                file_size=file_obj.size,
                engagement_id=engagement_id,
                client_name=client_name,
                company_name=company_name,
                fiscal_year=fiscal_year,
                audit_start_date=audit_start_date,
                audit_end_date=audit_end_date,
                status='PENDING'
            )
            
            # Process file and run analysis
            result = self._process_and_analyze_file(data_file, file_obj)
            
            if result['success']:
                response_data = {
                        'file': DataFileSerializer(data_file).data,
                        'analysis': result.get('analysis', {}),
                    'processing_summary': result.get('processing_summary', {}),
                        'message': 'File uploaded and analysis completed successfully'
                }
                
                # Add validation errors if any
                if 'validation_errors' in result:
                    response_data['validation_errors'] = result['validation_errors']
                    response_data['validation_errors_summary'] = result.get('validation_errors_summary', '')
                
                return Response(response_data, status=status.HTTP_201_CREATED)
            else:
                return Response(
                    {'error': result['error']}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
                
        except Exception as e:
            logger.error(f"Error in file upload and analysis: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _process_and_analyze_file(self, data_file, file_obj):
        """Process uploaded CSV file and run analysis"""
        try:
            # Update status
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Delete all existing transactions before processing new file
            SAPGLPosting.objects.all().delete()  # type: ignore
            logger.info("Deleted all existing transactions before processing new file")
            
            # Read CSV file
            content = file_obj.read().decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content))
            
            # Process rows
            processed_count = 0
            failed_count = 0
            min_date = None
            max_date = None
            min_amount = None
            max_amount = None
            new_transactions = []
            validation_errors = []
            row_number = 0
            
            for row in csv_reader:
                row_number += 1
                try:
                    # Map CSV columns to model fields
                    posting = self._create_posting_from_row(row)
                    if posting:
                        processed_count += 1
                        new_transactions.append(posting)
                        
                        # Update date and amount ranges
                        if posting.posting_date:
                            if min_date is None or posting.posting_date < min_date:
                                min_date = posting.posting_date
                            if max_date is None or posting.posting_date > max_date:
                                max_date = posting.posting_date
                        
                        if min_amount is None or posting.amount_local_currency < min_amount:
                            min_amount = posting.amount_local_currency
                        if max_amount is None or posting.amount_local_currency > max_amount:
                            max_amount = posting.amount_local_currency
                except ValueError as e:
                    # This is a validation error - collect details
                    error_info = {
                        'row_number': row_number,
                        'error': str(e),
                        'complete_row_data': dict(row),  # Include all row data
                        'document_number': row.get('Document Number', 'N/A'),
                        'gl_account': row.get('G/L Account', 'N/A'),
                        'amount': row.get('Amount in Local Currency', 'N/A'),
                        'user_name': row.get('User Name', 'N/A')
                    }
                    validation_errors.append(error_info)
                    logger.error(f"Validation error in row {row_number}: {e}")
                    logger.error(f"Complete row data: {dict(row)}")
                    failed_count += 1
                except Exception as e:
                    # This is a general processing error
                    error_info = {
                        'row_number': row_number,
                        'error': f"Processing error: {str(e)}",
                        'complete_row_data': dict(row),  # Include all row data
                        'document_number': row.get('Document Number', 'N/A'),
                        'gl_account': row.get('G/L Account', 'N/A'),
                        'amount': row.get('Amount in Local Currency', 'N/A'),
                        'user_name': row.get('User Name', 'N/A')
                    }
                    validation_errors.append(error_info)
                    logger.error(f"Error processing row {row_number}: {e}")
                    logger.error(f"Complete row data: {dict(row)}")
                    failed_count += 1
            
            # Save all transactions in batch
            if new_transactions:
                SAPGLPosting.objects.bulk_create(new_transactions)  # type: ignore
                logger.info(f"Saved {len(new_transactions)} transactions in batch")
            
            # Update DataFile record
            data_file.total_records = processed_count + failed_count
            data_file.processed_records = processed_count
            data_file.failed_records = failed_count
            data_file.status = 'COMPLETED' if failed_count == 0 else 'PARTIAL'
            data_file.processed_at = timezone.now()
            data_file.min_date = min_date
            data_file.max_date = max_date
            data_file.min_amount = min_amount
            data_file.max_amount = max_amount
            data_file.save()
            
            # Initialize analyzer
            analyzer = SAPGLAnalyzer()
            
            # Run analysis if we have processed transactions
            analysis_result = {}
            if processed_count > 0:
                analysis_result = self._run_file_analysis(analyzer, data_file, new_transactions)
            
            # Prepare response
            response_data = {
                'success': True,
                'analysis': analysis_result,
                'processing_summary': {
                    'total_rows': processed_count + failed_count,
                    'processed_count': processed_count,
                    'failed_count': failed_count,
                    'success_rate': (processed_count / (processed_count + failed_count) * 100) if (processed_count + failed_count) > 0 else 0
                }
            }
            
            # Add validation errors if any
            if validation_errors:
                response_data['validation_errors'] = validation_errors[:10]  # Limit to first 10 errors
                if len(validation_errors) > 10:
                    response_data['validation_errors_summary'] = f"Showing first 10 of {len(validation_errors)} validation errors"
            
            return response_data
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {'success': False, 'error': str(e)}
    
    def _create_posting_from_row(self, row):
        """Create SAPGLPosting from CSV row"""
        try:
            # Define required fields and their display names
            required_fields = {
                'Document Number': row.get('Document Number', '').strip(),
                'G/L Account': row.get('G/L Account', '').strip(),
                'Amount in Local Currency': row.get('Amount in Local Currency', '').strip(),
                'Posting Date': row.get('Posting Date', '').strip(),
                'User Name': row.get('User Name', '').strip()
            }
            
            # Check for missing required fields
            missing_fields = []
            print(required_fields)
            for field_name, field_value in required_fields.items():
                if not field_value:
                    missing_fields.append(field_name)
            
            # If any required fields are missing, raise an error
            if missing_fields:
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(f"Row validation failed: {error_msg}")
                logger.error(f"Complete row data for debugging: {dict(row)}")
                raise ValueError(error_msg)
            
            # Parse dates with multiple format support
            posting_date = self._parse_date(row.get('Posting Date', ''))
            document_date = self._parse_date(row.get('Document Date', ''))
            entry_date = self._parse_date(row.get('Entry Date', ''))
            
            # Parse amount with proper error handling
            amount_str = row.get('Amount in Local Currency', '0')
            try:
                # Clean the amount string - remove commas, spaces, and currency symbols
                amount_str = str(amount_str).replace(',', '').replace(' ', '').replace('SAR', '').replace('$', '').strip()
                if not amount_str or amount_str == '':
                    raise ValueError("Amount cannot be empty")
                amount_local_currency = Decimal(amount_str)
                if amount_local_currency <= 0:
                    raise ValueError("Amount must be greater than zero")
            except (ValueError, TypeError, InvalidOperation) as e:
                error_msg = f"Invalid amount format: {amount_str}. Error: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Determine transaction type (DEBIT/CREDIT)
            # Check if there's a specific transaction type field
            transaction_type = row.get('Transaction Type', '').strip().upper()
            if not transaction_type:
                # Try to determine from amount (negative = credit, positive = debit)
                # Or use a default based on account type
                if amount_local_currency < 0:
                    transaction_type = 'CREDIT'
                    amount_local_currency = abs(amount_local_currency)  # Make amount positive
                else:
                    transaction_type = 'DEBIT'
            elif transaction_type not in ['DEBIT', 'CREDIT']:
                transaction_type = 'DEBIT'  # Default to debit
            
            # Parse fiscal year and posting period with error handling
            try:
                fiscal_year = int(row.get('Fiscal Year', '0'))
            except (ValueError, TypeError):
                fiscal_year = 2025  # Default value
                
            try:
                posting_period = int(row.get('Posting Period', '0'))
            except (ValueError, TypeError):
                posting_period = 1  # Default value
            
            # Map CSV columns to model fields
            posting = SAPGLPosting(
                document_number=row.get('Document Number', ''),
                posting_date=posting_date,
                gl_account=row.get('G/L Account', ''),
                amount_local_currency=amount_local_currency,
                transaction_type=transaction_type,
                local_currency=row.get('Local Currency', 'SAR'),
                text=row.get('Text', ''),
                document_date=document_date,
                offsetting_account=row.get('Offsetting Account', ''),
                user_name=row.get('User Name', ''),
                entry_date=entry_date,
                document_type=row.get('Document Type', ''),
                profit_center=row.get('Profit Center', ''),
                cost_center=row.get('Cost Center', ''),
                clearing_document=row.get('Clearing Document', ''),
                segment=row.get('Segment', ''),
                wbs_element=row.get('WBS Element', ''),
                plant=row.get('Plant', ''),
                material=row.get('Material', ''),
                invoice_reference=row.get('Invoice Reference', ''),
                billing_document=row.get('Billing Document', ''),
                sales_document=row.get('Sales Document', ''),
                purchasing_document=row.get('Purchasing Document', ''),
                order_number=row.get('Order Number', ''),
                asset_number=row.get('Asset Number', ''),
                network=row.get('Network', ''),
                assignment=row.get('Assignment', ''),
                tax_code=row.get('Tax Code', ''),
                account_assignment=row.get('Account Assignment', ''),
                fiscal_year=fiscal_year,
                posting_period=posting_period,
                year_month=row.get('Year/Month', '')
            )
            return posting
        except Exception as e:
            logger.error(f"Error creating posting from row: {e}")
            return None
    
    def _parse_date(self, date_str):
        """Parse date string with multiple format support"""
        if not date_str:
            return None
        
        date_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%m-%d-%Y',
            '%d-%m-%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    

    
    def _run_file_analysis(self, analyzer, data_file, transactions):
        """Run analysis on the uploaded file data"""
        try:
            # Create analysis session
            session = AnalysisSession.objects.create(
                session_name=f"Analysis for {data_file.file_name}",
                description=f"Automated analysis for uploaded file {data_file.file_name}",
                date_from=data_file.min_date,
                date_to=data_file.max_date,
                min_amount=data_file.min_amount,
                max_amount=data_file.max_amount,
                status='RUNNING'
            )
            
            # Run analysis using the session
            analysis_results = analyzer.analyze_transactions(session)
            
            if 'error' in analysis_results:
                return {
                    'error': analysis_results['error'],
                    'analysis_status': 'FAILED'
                }
            
            # Get analysis summary
            summary = analyzer.get_analysis_summary(session)
            
            return {
                'session_id': str(session.id),
                'total_transactions': summary.get('total_transactions', 0),
                'total_amount': summary.get('total_amount', 0.0),
                'flagged_transactions': summary.get('flagged_transactions', 0),
                'high_value_transactions': summary.get('high_value_transactions', 0),
                'analysis_status': session.status,
                'flag_rate': summary.get('flag_rate', 0.0),
                'anomaly_summary': summary.get('anomaly_summary', {}),
                'risk_distribution': summary.get('risk_distribution', [])
            }
            
        except Exception as e:
            logger.error(f"Error running file analysis: {e}")
            return {
                'error': str(e),
                'analysis_status': 'FAILED'
            }


class FileSummaryView(generics.RetrieveAPIView):
    """View to get summary by file ID"""
    
    queryset = DataFile.objects.all()
    serializer_class = DataFileSerializer
    lookup_field = 'id'
    
    def retrieve(self, request, *args, **kwargs):
        data_file = self.get_object()
        serializer = self.get_serializer(data_file)
        
        # Get related data - get all transactions first for statistics
        all_related_transactions = SAPGLPosting.objects.filter(
            created_at__gte=data_file.uploaded_at
        )
        
        # Get limited transactions for display
        related_transactions = all_related_transactions.order_by('-created_at')[:100]
        
        # Calculate additional statistics using the full dataset
        total_amount = all_related_transactions.aggregate(
            total=Sum('amount_local_currency')
        )['total'] or Decimal('0.00')
        
        # Get currency information from transactions
        currency_info = all_related_transactions.values('local_currency').distinct()
        primary_currency = currency_info.first()['local_currency'] if currency_info.exists() else 'SAR'
        
        unique_users = all_related_transactions.values('user_name').distinct().count()
        unique_accounts = all_related_transactions.values('gl_account').distinct().count()
        unique_profit_centers = all_related_transactions.values('profit_center').distinct().count()
        
        # Get analysis sessions for this file
        analysis_sessions = AnalysisSession.objects.filter(
            created_at__gte=data_file.uploaded_at
        ).order_by('-created_at')
        
        # Get transaction analysis records for this file's transactions
        transaction_analyses = TransactionAnalysis.objects.filter(
            transaction__in=all_related_transactions
        )
        
        # If no analysis records exist, run real-time anomaly detection
        if not transaction_analyses.exists():
            # Run real-time anomaly detection
            analyzer = SAPGLAnalyzer()
            transactions_list = list(all_related_transactions)
            
            # Run all anomaly tests
            duplicate_anomalies = analyzer.detect_duplicate_entries(transactions_list)
            user_anomalies = analyzer.detect_user_anomalies(transactions_list)
            backdated_anomalies = analyzer.detect_backdated_entries(transactions_list)
            closing_anomalies = analyzer.detect_closing_entries(transactions_list)
            unusual_day_anomalies = analyzer.detect_unusual_days(transactions_list)
            holiday_anomalies = analyzer.detect_holiday_entries(transactions_list)
            
            # Count anomalies by type - only the 6 specific types we're implementing
            duplicate_count = 0
            user_anomaly_count = 0
            backdated_count = 0
            closing_count = 0
            unusual_days_count = 0
            holiday_count = 0
            
            # 1) Count Duplicate Entries
            if duplicate_anomalies:
                duplicate_transaction_ids = set()
                for dup in duplicate_anomalies:
                    for transaction in dup.get('transactions', []):
                        duplicate_transaction_ids.add(transaction['id'])
                duplicate_count = len(duplicate_transaction_ids)
            
            # 2) Count User Analysis anomalies
            if user_anomalies:
                user_anomaly_users = set()
                for anomaly in user_anomalies:
                    user_anomaly_users.add(anomaly.get('user_name'))
                user_anomaly_count = len(user_anomaly_users)
            
            # 3) Count Backdated Entries
            if backdated_anomalies:
                backdated_transaction_ids = set()
                for anomaly in backdated_anomalies:
                    backdated_transaction_ids.add(anomaly.get('transaction_id'))
                backdated_count = len(backdated_transaction_ids)
            
            # 4) Count Closing Entries
            if closing_anomalies:
                closing_transaction_ids = set()
                for anomaly in closing_anomalies:
                    closing_transaction_ids.add(anomaly.get('transaction_id'))
                closing_count = len(closing_transaction_ids)
            
            # 5) Count Unusual Days
            if unusual_day_anomalies:
                unusual_days_transaction_ids = set()
                for anomaly in unusual_day_anomalies:
                    unusual_days_transaction_ids.add(anomaly.get('transaction_id'))
                unusual_days_count = len(unusual_days_transaction_ids)
            
            # 6) Count Holiday Entries
            if holiday_anomalies:
                holiday_transaction_ids = set()
                for anomaly in holiday_anomalies:
                    holiday_transaction_ids.add(anomaly.get('transaction_id'))
                holiday_count = len(holiday_transaction_ids)
            
            # Calculate total anomalies (sum of all 6 types)
            total_anomalies = duplicate_count + user_anomaly_count + backdated_count + closing_count + unusual_days_count + holiday_count
            
            # Calculate total anomalies
            total_anomalies = amount_anomaly_count + timing_anomaly_count + user_anomaly_count + account_anomaly_count + pattern_anomaly_count
            
            # Create risk distribution based on anomaly counts
            total_transactions = all_related_transactions.count()
            risk_distribution = [
                {
                    'risk_level': 'LOW',
                    'count': total_transactions - total_anomalies,
                    'percentage': round(((total_transactions - total_anomalies) / total_transactions * 100), 2) if total_transactions > 0 else 0
                },
                {
                    'risk_level': 'MEDIUM',
                    'count': 0,
                    'percentage': 0
                },
                {
                    'risk_level': 'HIGH',
                    'count': total_anomalies,
                    'percentage': round((total_anomalies / total_transactions * 100), 2) if total_transactions > 0 else 0
                },
                {
                    'risk_level': 'CRITICAL',
                    'count': 0,
                    'percentage': 0
                }
            ]
            
            anomaly_summary = {
                'duplicate_entries': duplicate_count,
                'user_anomalies': user_anomaly_count,
                'backdated_entries': backdated_count,
                'closing_entries': closing_count,
                'unusual_days': unusual_days_count,
                'holiday_entries': holiday_count,
                'total_anomalies': total_anomalies
            }
            
        else:
            # Use existing analysis records
            # Calculate risk distribution
            risk_distribution = [
                {
                    'risk_level': 'LOW',
                    'count': transaction_analyses.filter(risk_level='LOW').count(),
                    'percentage': round((transaction_analyses.filter(risk_level='LOW').count() / transaction_analyses.count() * 100), 2) if transaction_analyses.count() > 0 else 0
                },
                {
                    'risk_level': 'MEDIUM',
                    'count': transaction_analyses.filter(risk_level='MEDIUM').count(),
                    'percentage': round((transaction_analyses.filter(risk_level='MEDIUM').count() / transaction_analyses.count() * 100), 2) if transaction_analyses.count() > 0 else 0
                },
                {
                    'risk_level': 'HIGH',
                    'count': transaction_analyses.filter(risk_level='HIGH').count(),
                    'percentage': round((transaction_analyses.filter(risk_level='HIGH').count() / transaction_analyses.count() * 100), 2) if transaction_analyses.count() > 0 else 0
                },
                {
                    'risk_level': 'CRITICAL',
                    'count': transaction_analyses.filter(risk_level='CRITICAL').count(),
                    'percentage': round((transaction_analyses.filter(risk_level='CRITICAL').count() / transaction_analyses.count() * 100), 2) if transaction_analyses.count() > 0 else 0
                }
            ]
            
            # Calculate anomaly summary - use same structure as real-time detection
            # For existing analysis records, we need to run real-time detection to get accurate counts
            # since the boolean flags don't map directly to our 6 specific types
            
            # Run real-time anomaly detection to get accurate counts
            analyzer = SAPGLAnalyzer()
            transactions_list = list(all_related_transactions)
            
            # Run all anomaly tests
            duplicate_anomalies = analyzer.detect_duplicate_entries(transactions_list)
            user_anomalies = analyzer.detect_user_anomalies(transactions_list)
            backdated_anomalies = analyzer.detect_backdated_entries(transactions_list)
            closing_anomalies = analyzer.detect_closing_entries(transactions_list)
            unusual_day_anomalies = analyzer.detect_unusual_days(transactions_list)
            holiday_anomalies = analyzer.detect_holiday_entries(transactions_list)
            
            # Count anomalies by type - only the 6 specific types we're implementing
            duplicate_count = 0
            user_anomaly_count = 0
            backdated_count = 0
            closing_count = 0
            unusual_days_count = 0
            holiday_count = 0
            
            # 1) Count Duplicate Entries
            if duplicate_anomalies:
                duplicate_transaction_ids = set()
                for dup in duplicate_anomalies:
                    for transaction in dup.get('transactions', []):
                        duplicate_transaction_ids.add(transaction['id'])
                duplicate_count = len(duplicate_transaction_ids)
            
            # 2) Count User Analysis anomalies
            if user_anomalies:
                user_anomaly_users = set()
                for anomaly in user_anomalies:
                    user_anomaly_users.add(anomaly.get('user_name'))
                user_anomaly_count = len(user_anomaly_users)
            
            # 3) Count Backdated Entries
            if backdated_anomalies:
                backdated_transaction_ids = set()
                for anomaly in backdated_anomalies:
                    backdated_transaction_ids.add(anomaly.get('transaction_id'))
                backdated_count = len(backdated_transaction_ids)
            
            # 4) Count Closing Entries
            if closing_anomalies:
                closing_transaction_ids = set()
                for anomaly in closing_anomalies:
                    closing_transaction_ids.add(anomaly.get('transaction_id'))
                closing_count = len(closing_transaction_ids)
            
            # 5) Count Unusual Days
            if unusual_day_anomalies:
                unusual_days_transaction_ids = set()
                for anomaly in unusual_day_anomalies:
                    unusual_days_transaction_ids.add(anomaly.get('transaction_id'))
                unusual_days_count = len(unusual_days_transaction_ids)
            
            # 6) Count Holiday Entries
            if holiday_anomalies:
                holiday_transaction_ids = set()
                for anomaly in holiday_anomalies:
                    holiday_transaction_ids.add(anomaly.get('transaction_id'))
                holiday_count = len(holiday_transaction_ids)
            
            # Calculate total anomalies (sum of all 6 types)
            total_anomalies = duplicate_count + user_anomaly_count + backdated_count + closing_count + unusual_days_count + holiday_count
            
            anomaly_summary = {
                'duplicate_entries': duplicate_count,
                'user_anomalies': user_anomaly_count,
                'backdated_entries': backdated_count,
                'closing_entries': closing_count,
                'unusual_days': unusual_days_count,
                'holiday_entries': holiday_count,
                'total_anomalies': total_anomalies
            }
        
        # Get detailed anomaly data
        duplicate_entries = []
        user_anomalies = []
        backdated_entries = []
        closing_entries = []
        unusual_days = []
        holiday_entries = []
        
        # Use the anomaly detection results from above if available, otherwise run detection
        if not transaction_analyses.exists():
            # Use the results from real-time detection above
            duplicate_entries = duplicate_anomalies[:10]  # Limit to top 10
            
            # Convert user anomalies to the expected format
            for anomaly in user_anomalies:
                user_anomalies.append({
                    'user_name': anomaly.get('user_name'),
                    'anomaly_count': anomaly.get('statistics', {}).get('transaction_count', 0),
                    'avg_risk_score': anomaly.get('risk_score', 0)
                })
            
            # Convert timing anomalies to the expected format
            for anomaly in backdated_anomalies:
                backdated_entries.append({
                    'document_number': anomaly.get('document_number'),
                    'posting_date': anomaly.get('posting_date'),
                    'amount_local_currency': anomaly.get('amount'),
                    'currency': anomaly.get('currency', 'SAR'),
                    'user_name': anomaly.get('user_name')
                })
            
            for anomaly in closing_anomalies:
                closing_entries.append({
                    'document_number': anomaly.get('document_number'),
                    'posting_date': anomaly.get('posting_date'),
                    'amount_local_currency': anomaly.get('amount'),
                    'currency': anomaly.get('currency', 'SAR'),
                    'user_name': anomaly.get('user_name')
                })
            
            for anomaly in unusual_day_anomalies:
                unusual_days.append({
                    'document_number': anomaly.get('document_number'),
                    'posting_date': anomaly.get('posting_date'),
                    'amount_local_currency': anomaly.get('amount'),
                    'currency': anomaly.get('currency', 'SAR'),
                    'user_name': anomaly.get('user_name')
                })
            
            for anomaly in holiday_anomalies:
                holiday_entries.append({
                    'document_number': anomaly.get('document_number'),
                    'posting_date': anomaly.get('posting_date'),
                    'amount_local_currency': anomaly.get('amount'),
                    'currency': anomaly.get('currency', 'SAR'),
                    'user_name': anomaly.get('user_name')
                })
        else:
            # Use existing analysis records for detailed data
            if all_related_transactions.exists():
                analyzer = SAPGLAnalyzer()
                duplicates = analyzer.detect_duplicate_entries(list(all_related_transactions))
                duplicate_entries = duplicates[:10]  # Limit to top 10
            
            # Get user anomalies (transactions by users with unusual patterns)
            high_risk_users = transaction_analyses.filter(
                user_anomaly=True, risk_level__in=['HIGH', 'CRITICAL']
            ).values('transaction__user_name').annotate(
                count=Count('id'),
                avg_risk_score=Avg('risk_score')
            ).order_by('-avg_risk_score')[:10]
            
            for user_data in high_risk_users:
                user_anomalies.append({
                    'user_name': user_data['transaction__user_name'],
                    'anomaly_count': user_data['count'],
                    'avg_risk_score': round(user_data['avg_risk_score'], 2)
                })
            
            # Get backdated entries (transactions posted on weekends/holidays)
            backdated_entries = all_related_transactions.filter(
                posting_date__week_day__in=[1, 7]  # Monday=1, Sunday=7
            ).values('document_number', 'posting_date', 'amount_local_currency', 'local_currency', 'user_name')[:10]
            
            # Get closing entries (transactions at month-end)
            closing_entries = all_related_transactions.filter(
                posting_date__day__gte=25  # Last week of month
            ).values('document_number', 'posting_date', 'amount_local_currency', 'local_currency', 'user_name')[:10]
        
        # Generate charts data
        charts_data = {
            'monthly_transaction_volume': self._generate_monthly_transaction_volume(all_related_transactions),
            'risk_distribution_chart': {
                'labels': [item['risk_level'] for item in risk_distribution],
                'data': [item['count'] for item in risk_distribution],
                'percentages': [item['percentage'] for item in risk_distribution]
            },
            'top_users_by_amount': self._generate_top_users_by_amount(all_related_transactions),
            'top_accounts_by_transactions': self._generate_top_accounts_by_transactions(all_related_transactions),
            'anomaly_breakdown': {
                'labels': ['Duplicate Entries', 'User Anomalies', 'Backdated Entries', 'Closing Entries', 'Unusual Days', 'Holiday Entries'],
                'data': [
                    anomaly_summary['duplicate_entries'],
                    anomaly_summary['user_anomalies'],
                    anomaly_summary['backdated_entries'],
                    anomaly_summary['closing_entries'],
                    anomaly_summary['unusual_days'],
                    anomaly_summary['holiday_entries']
                ]
            }
        }
        
        # Calculate flag rate
        flagged_transactions = transaction_analyses.filter(risk_level__in=['HIGH', 'CRITICAL']).count()
        flag_rate = round((flagged_transactions / transaction_analyses.count() * 100), 2) if transaction_analyses.count() > 0 else 0
        
        # Prepare statistics-only response
        response_data = {
            'file_info': {
                'id': str(data_file.id),
                'file_name': data_file.file_name,
                'status': data_file.status,
                'uploaded_at': data_file.uploaded_at,
                'processed_at': data_file.processed_at,
                'total_records': data_file.total_records,
                'processed_records': data_file.processed_records,
                'failed_records': data_file.failed_records,
                'currency': primary_currency
            },
            'summary_statistics': {
                'total_transactions': all_related_transactions.count(),
                'total_amount': float(total_amount),
                'currency': primary_currency,
                'flagged_transactions': flagged_transactions,
                'high_value_transactions': all_related_transactions.filter(amount_local_currency__gt=1000000).count(),
                'flag_rate': flag_rate,
                'unique_users': unique_users,
                'unique_accounts': unique_accounts,
                'unique_profit_centers': unique_profit_centers,
                'avg_amount': float(total_amount / all_related_transactions.count()) if all_related_transactions.count() > 0 else 0,
                'min_amount': float(all_related_transactions.aggregate(Min('amount_local_currency'))['amount_local_currency__min'] or 0),
                'max_amount': float(all_related_transactions.aggregate(Max('amount_local_currency'))['amount_local_currency__max'] or 0),
                'date_range': {
                    'start_date': all_related_transactions.aggregate(Min('posting_date'))['posting_date__min'],
                    'end_date': all_related_transactions.aggregate(Max('posting_date'))['posting_date__max']
                }
            },
            'risk_distribution': risk_distribution,
            'anomaly_summary': anomaly_summary,
            'charts_data': {
                'risk_distribution_chart': charts_data['risk_distribution_chart'],
                'anomaly_breakdown': charts_data['anomaly_breakdown'],
                'top_users_by_amount': charts_data['top_users_by_amount'][:5],  # Limit to top 5
                'top_accounts_by_transactions': charts_data['top_accounts_by_transactions'][:5],  # Limit to top 5
                'monthly_transaction_volume': charts_data['monthly_transaction_volume']
            },
            'analysis_sessions_summary': {
                'total_sessions': analysis_sessions.count(),
                'latest_session': {
                    'id': str(analysis_sessions.first().id),
                    'session_name': analysis_sessions.first().session_name,
                    'status': analysis_sessions.first().status,
                    'created_at': analysis_sessions.first().created_at
                } if analysis_sessions.exists() else None
            },
            'gl_charts_data': self._generate_gl_charts_data(all_related_transactions),
            'gl_account_summary': self._generate_gl_account_summary(all_related_transactions)
        }
        
        return Response(response_data)
    
    def _generate_monthly_transaction_volume(self, transactions):
        """Generate monthly transaction volume data"""
        monthly_data = {}
        
        # Get the primary currency for the dataset
        primary_currency = transactions.values('local_currency').annotate(
            count=Count('id')
        ).order_by('-count').first()
        currency = primary_currency['local_currency'] if primary_currency else 'SAR'
        
        for transaction in transactions:
            if transaction.posting_date:
                month_key = transaction.posting_date.strftime('%Y-%m')
                if month_key not in monthly_data:
                    monthly_data[month_key] = {
                        'month': month_key,
                        'transaction_count': 0,
                        'total_amount': Decimal('0.00'),
                        'debit_amount': Decimal('0.00'),
                        'credit_amount': Decimal('0.00')
                    }
                
                monthly_data[month_key]['transaction_count'] += 1
                monthly_data[month_key]['total_amount'] += transaction.amount_local_currency
                
                if transaction.transaction_type == 'DEBIT':
                    monthly_data[month_key]['debit_amount'] += transaction.amount_local_currency
                else:
                    monthly_data[month_key]['credit_amount'] += transaction.amount_local_currency
        
        return [
            {
                'month': data['month'],
                'transaction_count': data['transaction_count'],
                'total_amount': float(data['total_amount']),
                'debit_amount': float(data['debit_amount']),
                'credit_amount': float(data['credit_amount']),
                'currency': currency
            }
            for data in sorted(monthly_data.values(), key=lambda x: x['month'])
        ]
    
    def _generate_top_users_by_amount(self, transactions):
        """Generate top users by transaction amount"""
        user_stats = transactions.values('user_name', 'local_currency').annotate(
            total_amount=Sum('amount_local_currency'),
            transaction_count=Count('id'),
            avg_amount=Avg('amount_local_currency')
        ).order_by('-total_amount')[:10]
        
        return [
            {
                'user_name': stat['user_name'],
                'total_amount': float(stat['total_amount']),
                'currency': stat['local_currency'],
                'transaction_count': stat['transaction_count'],
                'avg_amount': float(stat['avg_amount'])
            }
            for stat in user_stats
        ]
    
    def _generate_top_accounts_by_transactions(self, transactions):
        """Generate top accounts by transaction count"""
        account_stats = transactions.values('gl_account', 'local_currency').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            avg_amount=Avg('amount_local_currency')
        ).order_by('-transaction_count')[:10]
        
        return [
            {
                'gl_account': stat['gl_account'],
                'transaction_count': stat['transaction_count'],
                'total_amount': float(stat['total_amount']),
                'currency': stat['local_currency'],
                'avg_amount': float(stat['avg_amount'])
            }
            for stat in account_stats
        ]
    
    def _generate_gl_account_data(self, transactions):
        """Generate GL account data including Trial Balance and Trading Equity"""
        # Get unique GL accounts from transactions
        gl_accounts = transactions.values('gl_account').distinct()
        
        gl_account_data = []
        
        for account_data in gl_accounts:
            account_id = account_data['gl_account']
            
            # Get transactions for this account
            account_transactions = transactions.filter(gl_account=account_id)
            
            # Calculate Trial Balance (TB) - Debits minus Credits
            total_debits = account_transactions.filter(transaction_type='DEBIT').aggregate(
                total=Sum('amount_local_currency')
            )['total'] or Decimal('0.00')
            
            total_credits = account_transactions.filter(transaction_type='CREDIT').aggregate(
                total=Sum('amount_local_currency')
            )['total'] or Decimal('0.00')
            
            # Trial Balance = Debits - Credits
            trial_balance = total_debits - total_credits
            
            # Calculate Trading Equity (TE) - Net change in equity accounts
            # For equity accounts (typically 3xxxx series), TE = Credits - Debits
            # For other accounts, TE = Debits - Credits (same as TB)
            if account_id.startswith('3'):  # Equity accounts
                trading_equity = total_credits - total_debits
            else:
                trading_equity = trial_balance
            
            # Get account details from GLAccount model if available
            try:
                gl_account_obj = GLAccount.objects.get(account_id=account_id)
                account_name = gl_account_obj.account_name
                account_type = gl_account_obj.account_type
                account_category = gl_account_obj.account_category
                normal_balance = gl_account_obj.normal_balance
            except GLAccount.DoesNotExist:
                account_name = f"Account {account_id}"
                account_type = "Unknown"
                account_category = "Unknown"
                normal_balance = "DEBIT"
            
            # Calculate additional statistics
            transaction_count = account_transactions.count()
            debit_count = account_transactions.filter(transaction_type='DEBIT').count()
            credit_count = account_transactions.filter(transaction_type='CREDIT').count()
            avg_amount = account_transactions.aggregate(avg=Avg('amount_local_currency'))['avg'] or Decimal('0.00')
            
            # Get currency for this account (use the most common currency)
            account_currency = account_transactions.values('local_currency').annotate(
                count=Count('id')
            ).order_by('-count').first()
            currency = account_currency['local_currency'] if account_currency else 'SAR'
            
            gl_account_data.append({
                'account_id': account_id,
                'account_name': account_name,
                'account_type': account_type,
                'account_category': account_category,
                'normal_balance': normal_balance,
                'trial_balance': float(trial_balance),
                'trading_equity': float(trading_equity),
                'total_debits': float(total_debits),
                'total_credits': float(total_credits),
                'transaction_count': transaction_count,
                'debit_count': debit_count,
                'credit_count': credit_count,
                'avg_amount': float(avg_amount),
                'currency': currency,
                'balance_type': 'CREDIT' if trial_balance < 0 else 'DEBIT' if trial_balance > 0 else 'ZERO'
            })
        
        # Sort by absolute trial balance (highest first)
        gl_account_data.sort(key=lambda x: abs(x['trial_balance']), reverse=True)
        
        return gl_account_data
    
    def _generate_gl_charts_data(self, transactions):
        """Generate charts data for GL accounts"""
        # Get GL account statistics
        account_stats = transactions.values('gl_account', 'local_currency').annotate(
            total_amount=Sum('amount_local_currency'),
            transaction_count=Count('id'),
            total_debits=Sum('amount_local_currency', filter=Q(transaction_type='DEBIT')),
            total_credits=Sum('amount_local_currency', filter=Q(transaction_type='CREDIT'))
        ).order_by('-total_amount')[:20]  # Top 20 accounts by amount
        
        # Get primary currency for the dataset
        primary_currency = transactions.values('local_currency').annotate(
            count=Count('id')
        ).order_by('-count').first()
        currency = primary_currency['local_currency'] if primary_currency else 'SAR'
        
        # Prepare chart data
        chart_data = {
            'top_accounts_by_amount': [
                {
                    'account_id': stat['gl_account'],
                    'total_amount': float(stat['total_amount']),
                    'currency': stat['local_currency'],
                    'transaction_count': stat['transaction_count'],
                    'trial_balance': float((stat['total_debits'] or 0) - (stat['total_credits'] or 0))
                }
                for stat in account_stats
            ],
            'account_type_distribution': self._get_account_type_distribution(transactions),
            'balance_distribution': self._get_balance_distribution(transactions),
            'monthly_account_activity': self._get_monthly_account_activity(transactions),
            'currency': currency
        }
        
        return chart_data
    
    def _generate_gl_account_summary(self, transactions):
        """Generate comprehensive GL account summary with TB, TE, credits, and debits"""
        # Get all unique GL accounts from transactions
        gl_accounts = transactions.values('gl_account').distinct()
        
        # Get primary currency for the dataset
        primary_currency = transactions.values('local_currency').annotate(
            count=Count('id')
        ).order_by('-count').first()
        currency = primary_currency['local_currency'] if primary_currency else 'SAR'
        
        gl_account_summary = []
        
        for account_data in gl_accounts:
            account_id = account_data['gl_account']
            
            # Get transactions for this account
            account_transactions = transactions.filter(gl_account=account_id)
            
            # Calculate Trial Balance (TB) - Debits minus Credits
            total_debits = account_transactions.filter(transaction_type='DEBIT').aggregate(
                total=Sum('amount_local_currency')
            )['total'] or Decimal('0.00')
            
            total_credits = account_transactions.filter(transaction_type='CREDIT').aggregate(
                total=Sum('amount_local_currency')
            )['total'] or Decimal('0.00')
            
            # Trial Balance = Debits - Credits
            trial_balance = total_debits - total_credits
            
            # Calculate Trading Equity (TE) - Net change in equity accounts
            # For equity accounts (typically 3xxxx series), TE = Credits - Debits
            # For other accounts, TE = Debits - Credits (same as TB)
            if account_id.startswith('3'):  # Equity accounts
                trading_equity = total_credits - total_debits
            else:
                trading_equity = trial_balance
            
            # Get account details from GLAccount model if available
            try:
                gl_account_obj = GLAccount.objects.get(account_id=account_id)
                account_name = gl_account_obj.account_name
                account_type = gl_account_obj.account_type
                account_category = gl_account_obj.account_category
                normal_balance = gl_account_obj.normal_balance
            except GLAccount.DoesNotExist:
                account_name = f"Account {account_id}"
                account_type = "Unknown"
                account_category = "Unknown"
                normal_balance = "DEBIT"
            
            # Calculate additional statistics
            transaction_count = account_transactions.count()
            debit_count = account_transactions.filter(transaction_type='DEBIT').count()
            credit_count = account_transactions.filter(transaction_type='CREDIT').count()
            avg_amount = account_transactions.aggregate(avg=Avg('amount_local_currency'))['avg'] or Decimal('0.00')
            
            # Get currency for this account (use the most common currency)
            account_currency = account_transactions.values('local_currency').annotate(
                count=Count('id')
            ).order_by('-count').first()
            account_currency_code = account_currency['local_currency'] if account_currency else currency
            
            # Determine balance type
            if trial_balance > 0:
                balance_type = 'DEBIT'
            elif trial_balance < 0:
                balance_type = 'CREDIT'
            else:
                balance_type = 'ZERO'
            
            # Check if balance is normal (matches normal_balance)
            is_normal_balance = (
                (normal_balance == 'DEBIT' and trial_balance >= 0) or
                (normal_balance == 'CREDIT' and trial_balance <= 0) or
                trial_balance == 0
            )
            
            gl_account_summary.append({
                'account_id': account_id,
                'account_name': account_name,
                'account_type': account_type,
                'account_category': account_category,
                'normal_balance': normal_balance,
                'currency': account_currency_code,
                
                # Balance information
                'trial_balance': float(trial_balance),
                'trading_equity': float(trading_equity),
                'total_debits': float(total_debits),
                'total_credits': float(total_credits),
                'balance_type': balance_type,
                'is_normal_balance': is_normal_balance,
                
                # Transaction statistics
                'transaction_count': transaction_count,
                'debit_count': debit_count,
                'credit_count': credit_count,
                'avg_amount': float(avg_amount),
                
                # Additional metrics
                'debit_credit_ratio': float(total_debits / total_credits) if total_credits > 0 else None,
                'avg_debit_amount': float(total_debits / debit_count) if debit_count > 0 else 0,
                'avg_credit_amount': float(total_credits / credit_count) if credit_count > 0 else 0,
                'credit_debit_ratio': float(total_credits / total_debits) if total_debits > 0 else None
            })
        
        # Sort by absolute trial balance (highest first)
        gl_account_summary.sort(key=lambda x: abs(x['trial_balance']), reverse=True)
        
        # Calculate summary statistics
        total_accounts = len(gl_account_summary)
        total_trial_balance = sum(account['trial_balance'] for account in gl_account_summary)
        total_trading_equity = sum(account['trading_equity'] for account in gl_account_summary)
        total_debits = sum(account['total_debits'] for account in gl_account_summary)
        total_credits = sum(account['total_credits'] for account in gl_account_summary)
        
        # Count accounts by balance type
        debit_balance_accounts = sum(1 for account in gl_account_summary if account['balance_type'] == 'DEBIT')
        credit_balance_accounts = sum(1 for account in gl_account_summary if account['balance_type'] == 'CREDIT')
        zero_balance_accounts = sum(1 for account in gl_account_summary if account['balance_type'] == 'ZERO')
        
        # Count accounts with normal vs abnormal balances
        normal_balance_accounts = sum(1 for account in gl_account_summary if account['is_normal_balance'])
        abnormal_balance_accounts = total_accounts - normal_balance_accounts
        
        return {
            'summary_statistics': {
                'total_accounts': total_accounts,
                'total_trial_balance': float(total_trial_balance),
                'total_trading_equity': float(total_trading_equity),
                'total_debits': float(total_debits),
                'total_credits': float(total_credits),
                'currency': currency,
                'debit_balance_accounts': debit_balance_accounts,
                'credit_balance_accounts': credit_balance_accounts,
                'zero_balance_accounts': zero_balance_accounts,
                'normal_balance_accounts': normal_balance_accounts,
                'abnormal_balance_accounts': abnormal_balance_accounts
            },
            'accounts': gl_account_summary
        }
    
    def _get_account_type_distribution(self, transactions):
        """Get distribution of transactions by account type"""
        # Get account types from GLAccount model
        account_types = {}
        
        # Get primary currency for the dataset
        primary_currency = transactions.values('local_currency').annotate(
            count=Count('id')
        ).order_by('-count').first()
        currency = primary_currency['local_currency'] if primary_currency else 'SAR'
        
        for transaction in transactions:
            account_id = transaction.gl_account
            try:
                gl_account = GLAccount.objects.get(account_id=account_id)
                account_type = gl_account.account_type
            except GLAccount.DoesNotExist:
                account_type = "Unknown"
            
            if account_type not in account_types:
                account_types[account_type] = {
                    'type': account_type,
                    'count': 0,
                    'total_amount': Decimal('0.00')
                }
            
            account_types[account_type]['count'] += 1
            account_types[account_type]['total_amount'] += transaction.amount_local_currency
        
        return [
            {
                'type': data['type'],
                'count': data['count'],
                'total_amount': float(data['total_amount']),
                'currency': currency
            }
            for data in account_types.values()
        ]
    
    def _get_balance_distribution(self, transactions):
        """Get distribution of account balances"""
        balance_ranges = {
            'Negative (< 0)': 0,
            'Zero (0)': 0,
            'Low (0-10K)': 0,
            'Medium (10K-100K)': 0,
            'High (100K-1M)': 0,
            'Very High (> 1M)': 0
        }
        
        # Calculate trial balance for each account
        account_balances = {}
        for transaction in transactions:
            account_id = transaction.gl_account
            if account_id not in account_balances:
                account_balances[account_id] = {'debits': Decimal('0.00'), 'credits': Decimal('0.00')}
            
            if transaction.transaction_type == 'DEBIT':
                account_balances[account_id]['debits'] += transaction.amount_local_currency
            else:
                account_balances[account_id]['credits'] += transaction.amount_local_currency
        
        # Categorize balances
        for balance in account_balances.values():
            trial_balance = balance['debits'] - balance['credits']
            balance_amount = float(trial_balance)
            
            if balance_amount < 0:
                balance_ranges['Negative (< 0)'] += 1
            elif balance_amount == 0:
                balance_ranges['Zero (0)'] += 1
            elif balance_amount <= 10000:
                balance_ranges['Low (0-10K)'] += 1
            elif balance_amount <= 100000:
                balance_ranges['Medium (10K-100K)'] += 1
            elif balance_amount <= 1000000:
                balance_ranges['High (100K-1M)'] += 1
            else:
                balance_ranges['Very High (> 1M)'] += 1
        
        return [
            {
                'range': range_name,
                'count': count
            }
            for range_name, count in balance_ranges.items()
        ]
    
    def _get_monthly_account_activity(self, transactions):
        """Get monthly activity by account type"""
        monthly_data = {}
        
        for transaction in transactions:
            if transaction.posting_date:
                month_key = transaction.posting_date.strftime('%Y-%m')
                
                if month_key not in monthly_data:
                    monthly_data[month_key] = {
                        'month': month_key,
                        'total_transactions': 0,
                        'total_amount': Decimal('0.00'),
                        'account_types': {}
                    }
                
                monthly_data[month_key]['total_transactions'] += 1
                monthly_data[month_key]['total_amount'] += transaction.amount_local_currency
                
                # Get account type
                try:
                    gl_account = GLAccount.objects.get(account_id=transaction.gl_account)
                    account_type = gl_account.account_type
                except GLAccount.DoesNotExist:
                    account_type = "Unknown"
                
                if account_type not in monthly_data[month_key]['account_types']:
                    monthly_data[month_key]['account_types'][account_type] = {
                        'count': 0,
                        'amount': Decimal('0.00')
                    }
                
                monthly_data[month_key]['account_types'][account_type]['count'] += 1
                monthly_data[month_key]['account_types'][account_type]['amount'] += transaction.amount_local_currency
        
        return [
            {
                'month': data['month'],
                'total_transactions': data['total_transactions'],
                'total_amount': float(data['total_amount']),
                'account_types': [
                    {
                        'type': account_type,
                        'count': type_data['count'],
                        'amount': float(type_data['amount'])
                    }
                    for account_type, type_data in data['account_types'].items()
                ]
            }
            for data in sorted(monthly_data.values(), key=lambda x: x['month'])
        ]

class DuplicateAnomalyViewSet(viewsets.ViewSet):
    """ViewSet for duplicate anomaly detection and analysis"""
    
    def list(self, request):
        """Get comprehensive duplicate anomaly data for a specific sheet"""
        try:
            # Get sheet_id parameter (required)
            sheet_id = request.query_params.get('sheet_id', None)
            if not sheet_id:
                return Response(
                    {'error': 'sheet_id parameter is required'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get filter parameters
            date_from = request.query_params.get('date_from', None)
            date_to = request.query_params.get('date_to', None)
            min_amount = request.query_params.get('min_amount', None)
            max_amount = request.query_params.get('max_amount', None)
            gl_accounts = request.query_params.getlist('gl_accounts', [])
            users = request.query_params.getlist('users', [])
            document_types = request.query_params.getlist('document_types', [])
            duplicate_threshold = int(request.query_params.get('duplicate_threshold', 2))
            duplicate_types = request.query_params.getlist('duplicate_types', [])
            
            # Build query - filter by sheet_id first
            query = SAPGLPosting.objects.all()  # type: ignore
            
            # Filter by sheet_id (assuming sheet_id maps to a specific data file or session)
            # You can modify this logic based on how sheet_id relates to your data
            if sheet_id:
                # Option 1: If sheet_id is a file ID
                try:
                    data_file = DataFile.objects.get(id=sheet_id)  # type: ignore
                    # Filter transactions uploaded after this file
                    query = query.filter(created_at__gte=data_file.uploaded_at)
                except DataFile.DoesNotExist:
                    # Option 2: If sheet_id is a session ID
                    try:
                        session = AnalysisSession.objects.get(id=sheet_id)  # type: ignore
                        if session.date_from:
                            query = query.filter(posting_date__gte=session.date_from)
                        if session.date_to:
                            query = query.filter(posting_date__lte=session.date_to)
                    except AnalysisSession.DoesNotExist:
                        # Option 3: If sheet_id is a custom identifier
                        # You can implement custom logic here
                        pass
            
            # Apply additional filters
            if date_from:
                query = query.filter(posting_date__gte=date_from)
            if date_to:
                query = query.filter(posting_date__lte=date_to)
            if min_amount:
                query = query.filter(amount_local_currency__gte=min_amount)
            if max_amount:
                query = query.filter(amount_local_currency__lte=max_amount)
            if gl_accounts:
                query = query.filter(gl_account__in=gl_accounts)
            if users:
                query = query.filter(user_name__in=users)
            if document_types:
                query = query.filter(document_type__in=document_types)
            
            # Get transactions
            transactions = list(query)
            
            if not transactions:
                return Response({
                    'sheet_id': sheet_id,
                    'total_duplicates': 0,  # Number of duplicate groups
                    'unique_duplicate_transactions': 0,  # Number of unique transactions involved in duplicates
                    'total_transactions_involved': 0,
                    'total_amount_involved': 0.0,
                    'type_breakdown': {},
                    'duplicates': [],
                    'charts_data': {
                        'duplicate_flags_breakdown': {'labels': [], 'data': []},
                        'monthly_duplicate_data': [],
                        'user_breakdown': [],
                        'duplicate_type_breakdown': {},
                        'fs_line_breakdown': []
                    },
                    'training_data': {
                        'training_features': [],
                        'training_labels': [],
                        'total_samples': 0,
                        'duplicate_samples': 0,
                        'non_duplicate_samples': 0,
                        'feature_importance': {},
                        'model_metrics': {}
                    }
                })
            
            # Run duplicate detection
            analyzer = SAPGLAnalyzer()
            analyzer.analysis_config['duplicate_threshold'] = duplicate_threshold
            
            all_duplicates = analyzer.detect_duplicate_entries(transactions)
            
            # Filter by duplicate types if specified
            if duplicate_types:
                type_mapping = {
                    '1': 'Type 1 Duplicate',
                    '2': 'Type 2 Duplicate', 
                    '3': 'Type 3 Duplicate',
                    '4': 'Type 4 Duplicate',
                    '5': 'Type 5 Duplicate',
                    '6': 'Type 6 Duplicate'
                }
                filtered_duplicates = []
                for dup in all_duplicates:
                    for type_num in duplicate_types:
                        if dup['type'] == type_mapping.get(type_num):
                            filtered_duplicates.append(dup)
                            break
                all_duplicates = filtered_duplicates
            
            # Calculate summary statistics
            total_duplicates = len(all_duplicates)
            total_transactions_involved = sum(dup['count'] for dup in all_duplicates)
            total_amount_involved = sum(dup['amount'] * dup['count'] for dup in all_duplicates)
            
            # Count unique transactions involved in duplicates (consistent with file-summary)
            unique_duplicate_transactions = set()
            for dup in all_duplicates:
                for transaction in dup.get('transactions', []):
                    unique_duplicate_transactions.add(transaction['id'])
            unique_duplicate_count = len(unique_duplicate_transactions)
            
            # Type breakdown
            type_breakdown = {}
            for dup in all_duplicates:
                dup_type = dup['type']
                if dup_type not in type_breakdown:
                    type_breakdown[dup_type] = {
                        'count': 0,
                        'total_transactions': 0,
                        'total_amount': 0.0
                    }
                type_breakdown[dup_type]['count'] += 1
                type_breakdown[dup_type]['total_transactions'] += dup['count']
                type_breakdown[dup_type]['total_amount'] += dup['amount'] * dup['count']
            
            # Generate charts data
            charts_data = {
                'duplicate_flags_breakdown': {
                    'labels': ['Type 1', 'Type 2', 'Type 3', 'Type 4', 'Type 5', 'Type 6'],
                    'data': [
                        len([d for d in all_duplicates if d['type'] == 'Type 1 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 2 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 3 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 4 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 5 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 6 Duplicate'])
                    ]
                },
                'monthly_duplicate_data': self._generate_monthly_duplicate_data(transactions, all_duplicates),
                'user_breakdown': self._generate_user_breakdown(all_duplicates),
                'duplicate_type_breakdown': self._generate_duplicate_type_breakdown(all_duplicates),
                'fs_line_breakdown': self._generate_fs_line_breakdown(all_duplicates)
            }
            
            # Generate training data
            training_data = self._generate_training_data(transactions)
            
            # Comprehensive response
            response_data = {
                'sheet_id': sheet_id,
                'total_duplicates': total_duplicates,  # Number of duplicate groups
                'unique_duplicate_transactions': unique_duplicate_count,  # Number of unique transactions involved in duplicates (consistent with file-summary)
                'total_transactions_involved': total_transactions_involved,
                'total_amount_involved': float(total_amount_involved),
                'type_breakdown': type_breakdown,
                'duplicates': all_duplicates,
                'charts_data': charts_data,
                'training_data': training_data
            }
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error in duplicate anomaly analysis: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def analyze(self, request):
        """Run comprehensive duplicate analysis with custom parameters for a specific sheet"""
        try:
            # Get sheet_id from request data
            sheet_id = request.data.get('sheet_id', None)
            if not sheet_id:
                return Response(
                    {'error': 'sheet_id is required in request body'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate other parameters
            serializer = DuplicateAnalysisRequestSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            # Get validated data
            data = serializer.validated_data
            
            # Set default values if not provided
            if not data:
                data = {}
            
            # Build query - filter by sheet_id first
            query = SAPGLPosting.objects.all()  # type: ignore
            
            # Filter by sheet_id
            if sheet_id:
                try:
                    data_file = DataFile.objects.get(id=sheet_id)  # type: ignore
                    query = query.filter(created_at__gte=data_file.uploaded_at)
                except DataFile.DoesNotExist:
                    try:
                        session = AnalysisSession.objects.get(id=sheet_id)  # type: ignore
                        if session.date_from:
                            query = query.filter(posting_date__gte=session.date_from)
                        if session.date_to:
                            query = query.filter(posting_date__lte=session.date_to)
                    except AnalysisSession.DoesNotExist:
                        pass
            
            # Apply additional filters
            if data and data.get('date_from'):
                query = query.filter(posting_date__gte=data['date_from'])
            if data and data.get('date_to'):
                query = query.filter(posting_date__lte=data['date_to'])
            if data and data.get('min_amount'):
                query = query.filter(amount_local_currency__gte=data['min_amount'])
            if data and data.get('max_amount'):
                query = query.filter(amount_local_currency__lte=data['max_amount'])
            if data and data.get('gl_accounts'):
                query = query.filter(gl_account__in=data['gl_accounts'])
            if data and data.get('users'):
                query = query.filter(user_name__in=data['users'])
            if data and data.get('document_types'):
                query = query.filter(document_type__in=data['document_types'])
            
            # Get transactions
            transactions = list(query)
            
            if not transactions:
                return Response({
                    'sheet_id': sheet_id,
                    'message': 'No transactions found matching criteria',
                    'total_duplicates': 0,  # Number of duplicate groups
                    'unique_duplicate_transactions': 0,  # Number of unique transactions involved in duplicates
                    'total_transactions_involved': 0,
                    'total_amount_involved': 0.0,
                    'type_breakdown': {},
                    'duplicates': [],
                    'charts_data': {},
                    'training_data': {}
                })
            
            # Run duplicate detection
            analyzer = SAPGLAnalyzer()
            analyzer.analysis_config['duplicate_threshold'] = data.get('duplicate_threshold', 2) if data else 2
            
            all_duplicates = analyzer.detect_duplicate_entries(transactions)
            
            # Filter by duplicate types if specified
            if data and not data.get('include_all_types', True) and data.get('duplicate_types'):
                type_mapping = {
                    1: 'Type 1 Duplicate',
                    2: 'Type 2 Duplicate', 
                    3: 'Type 3 Duplicate',
                    4: 'Type 4 Duplicate',
                    5: 'Type 5 Duplicate',
                    6: 'Type 6 Duplicate'
                }
                filtered_duplicates = []
                for dup in all_duplicates:
                    for type_num in data['duplicate_types']:
                        if dup['type'] == type_mapping.get(type_num):
                            filtered_duplicates.append(dup)
                            break
                all_duplicates = filtered_duplicates
            
            # Calculate summary statistics
            total_duplicates = len(all_duplicates)
            total_transactions_involved = sum(dup['count'] for dup in all_duplicates)
            total_amount_involved = sum(dup['amount'] * dup['count'] for dup in all_duplicates)
            
            # Count unique transactions involved in duplicates (consistent with file-summary)
            unique_duplicate_transactions = set()
            for dup in all_duplicates:
                for transaction in dup.get('transactions', []):
                    unique_duplicate_transactions.add(transaction['id'])
            unique_duplicate_count = len(unique_duplicate_transactions)
            
            # Count unique transactions involved in duplicates (consistent with file-summary)
            unique_duplicate_transactions = set()
            for dup in all_duplicates:
                for transaction in dup.get('transactions', []):
                    unique_duplicate_transactions.add(transaction['id'])
            unique_duplicate_count = len(unique_duplicate_transactions)
            
            # Type breakdown
            type_breakdown = {}
            for dup in all_duplicates:
                dup_type = dup['type']
                if dup_type not in type_breakdown:
                    type_breakdown[dup_type] = {
                        'count': 0,
                        'total_transactions': 0,
                        'total_amount': 0.0
                    }
                type_breakdown[dup_type]['count'] += 1
                type_breakdown[dup_type]['total_transactions'] += dup['count']
                type_breakdown[dup_type]['total_amount'] += dup['amount'] * dup['count']
            
            # Generate charts data
            charts_data = {
                'duplicate_flags_breakdown': {
                    'labels': ['Type 1', 'Type 2', 'Type 3', 'Type 4', 'Type 5', 'Type 6'],
                    'data': [
                        len([d for d in all_duplicates if d['type'] == 'Type 1 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 2 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 3 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 4 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 5 Duplicate']),
                        len([d for d in all_duplicates if d['type'] == 'Type 6 Duplicate'])
                    ]
                },
                'monthly_duplicate_data': self._generate_monthly_duplicate_data(transactions, all_duplicates),
                'user_breakdown': self._generate_user_breakdown(all_duplicates),
                'duplicate_type_breakdown': self._generate_duplicate_type_breakdown(all_duplicates),
                'fs_line_breakdown': self._generate_fs_line_breakdown(all_duplicates)
            }
            
            # Generate training data
            training_data = self._generate_training_data(transactions)
            
            response_data = {
                'sheet_id': sheet_id,
                'message': 'Duplicate analysis completed successfully',
                'total_duplicates': total_duplicates,  # Number of duplicate groups
                'unique_duplicate_transactions': unique_duplicate_count,  # Number of unique transactions involved in duplicates (consistent with file-summary)
                'total_transactions_involved': total_transactions_involved,
                'total_amount_involved': float(total_amount_involved),
                'type_breakdown': type_breakdown,
                'duplicates': all_duplicates,
                'charts_data': charts_data,
                'training_data': training_data
            }
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error in duplicate analysis: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _generate_monthly_duplicate_data(self, transactions, duplicates):
        """Generate monthly duplicate data with amounts and counts"""
        monthly_data = {}
        
        # Group duplicates by month
        for dup in duplicates:
            for tx in dup['transactions']:
                if tx.get('posting_date'):
                    try:
                        month_key = tx['posting_date'][:7]  # YYYY-MM format
                        if month_key not in monthly_data:
                            monthly_data[month_key] = {
                                'month': month_key,
                                'debit_amount': 0.0,
                                'credit_amount': 0.0,
                                'journal_line_count': 0,
                                'duplicate_count': 0
                            }
                        
                        # Add amounts
                        if tx.get('transaction_type') == 'DEBIT':
                            monthly_data[month_key]['debit_amount'] += tx['amount']
                        else:
                            monthly_data[month_key]['credit_amount'] += tx['amount']
                        
                        monthly_data[month_key]['journal_line_count'] += 1
                        monthly_data[month_key]['duplicate_count'] += 1
                    except:
                        continue
        
        return list(monthly_data.values())
    
    def _generate_user_breakdown(self, duplicates):
        """Generate breakdown of duplicates per impacted user"""
        user_data = {}
        
        for dup in duplicates:
            for tx in dup['transactions']:
                user_name = tx.get('user_name', 'Unknown')
                if user_name not in user_data:
                    user_data[user_name] = {
                        'user_name': user_name,
                        'duplicate_count': 0,
                        'total_amount': 0.0,
                        'duplicate_types': set()
                    }
                
                user_data[user_name]['duplicate_count'] += 1
                user_data[user_name]['total_amount'] += tx['amount']
                user_data[user_name]['duplicate_types'].add(dup['type'])
        
        # Convert sets to lists for JSON serialization
        for user in user_data.values():
            user['duplicate_types'] = list(user['duplicate_types'])
        
        return list(user_data.values())
    
    def _generate_duplicate_type_breakdown(self, duplicates):
        """Generate detailed breakdown by duplicate type"""
        type_data = {
            'Type 3 Duplicate': {'count': 0, 'users': set(), 'amounts': []},
            'Type 4 Duplicate': {'count': 0, 'dates': set(), 'amounts': []},
            'Type 5 Duplicate': {'count': 0, 'dates': set(), 'amounts': []},
            'Type 6 Duplicate': {'count': 0, 'combinations': set(), 'amounts': []}
        }
        
        for dup in duplicates:
            if dup['type'] in type_data:
                type_data[dup['type']]['count'] += 1
                type_data[dup['type']]['amounts'].append(dup['amount'])
                
                if dup['type'] == 'Type 3 Duplicate':
                    type_data[dup['type']]['users'].add(dup.get('user_name', 'Unknown'))
                elif dup['type'] == 'Type 4 Duplicate':
                    type_data[dup['type']]['dates'].add(dup.get('posting_date', 'Unknown'))
                elif dup['type'] == 'Type 5 Duplicate':
                    type_data[dup['type']]['dates'].add(dup.get('document_date', 'Unknown'))
                elif dup['type'] == 'Type 6 Duplicate':
                    combination = f"{dup.get('user_name', 'Unknown')}-{dup.get('posting_date', 'Unknown')}-{dup.get('document_date', 'Unknown')}"
                    type_data[dup['type']]['combinations'].add(combination)
        
        # Convert sets to lists for JSON serialization
        for dup_type in type_data.values():
            for key, value in dup_type.items():
                if isinstance(value, set):
                    dup_type[key] = list(value)
        
        return type_data
    
    def _generate_fs_line_breakdown(self, duplicates):
        """Generate breakdown of duplicates per impacted FS line"""
        fs_line_data = {}
        
        for dup in duplicates:
            gl_account = dup.get('gl_account', 'Unknown')
            if gl_account not in fs_line_data:
                fs_line_data[gl_account] = {
                    'gl_account': gl_account,
                    'duplicate_count': 0,
                    'total_amount': 0.0,
                    'duplicate_types': set(),
                    'transaction_count': 0
                }
            
            fs_line_data[gl_account]['duplicate_count'] += 1
            fs_line_data[gl_account]['total_amount'] += dup['amount'] * dup['count']
            fs_line_data[gl_account]['duplicate_types'].add(dup['type'])
            fs_line_data[gl_account]['transaction_count'] += dup['count']
        
        # Convert sets to lists for JSON serialization
        for fs_line in fs_line_data.values():
            fs_line['duplicate_types'] = list(fs_line['duplicate_types'])
        
        return list(fs_line_data.values())
    
    def _generate_training_data(self, transactions):
        """Generate training data for machine learning"""
        import pandas as pd
        import numpy as np
        from datetime import datetime
        
        # Convert transactions to DataFrame with proper date handling
        transaction_data = []
        for t in transactions:
            # Convert dates to pandas datetime for proper handling
            posting_date = pd.to_datetime(t.posting_date) if t.posting_date else None
            document_date = pd.to_datetime(t.document_date) if t.document_date else None
            
            transaction_data.append({
                'id': str(t.id),
                'gl_account': t.gl_account or 'UNKNOWN',
                'amount': float(t.amount_local_currency),
                'user_name': t.user_name,
                'posting_date': posting_date,
                'document_date': document_date,
                'document_type': t.document_type,
                'text': t.text or '',
                'transaction_type': t.transaction_type
            })
        
        df = pd.DataFrame(transaction_data)
        
        # Create features
        features = []
        labels = []
        
        # Generate positive samples (duplicates)
        analyzer = SAPGLAnalyzer()
        duplicates = analyzer.detect_duplicate_entries(transactions)
        
        duplicate_ids = set()
        for dup in duplicates:
            for tx in dup['transactions']:
                duplicate_ids.add(tx['id'])
        
        # Create feature vectors
        for i, row in df.iterrows():
            # Basic features with proper date handling
            posting_date = row['posting_date']
            day_of_week = posting_date.dayofweek if pd.notna(posting_date) else 0
            day_of_month = posting_date.day if pd.notna(posting_date) else 0
            month = posting_date.month if pd.notna(posting_date) else 0
            
            feature_vector = [
                float(row['amount']),
                len(str(row['gl_account'])),
                len(str(row['user_name'])),
                day_of_week,
                day_of_month,
                month,
                1 if row['transaction_type'] == 'DEBIT' else 0,
                len(str(row['text']))
            ]
            
            features.append(feature_vector)
            labels.append(1 if row['id'] in duplicate_ids else 0)
        
        # Calculate feature importance (simple correlation-based)
        feature_names = ['amount', 'gl_account_len', 'user_name_len', 'day_of_week', 
                        'day_of_month', 'month', 'is_debit', 'text_len']
        feature_importance = {}
        
        if len(features) > 0:
            features_array = np.array(features)
            for i, name in enumerate(feature_names):
                if i < features_array.shape[1]:
                    correlation = np.corrcoef(features_array[:, i], labels)[0, 1]
                    feature_importance[name] = abs(correlation) if not np.isnan(correlation) else 0.0
        
        # Calculate model metrics
        total_samples = len(labels)
        duplicate_samples = sum(labels)
        non_duplicate_samples = total_samples - duplicate_samples
        
        model_metrics = {
            'total_samples': total_samples,
            'duplicate_samples': duplicate_samples,
            'non_duplicate_samples': non_duplicate_samples,
            'duplicate_ratio': duplicate_samples / total_samples if total_samples > 0 else 0
        }
        
        return {
            'training_features': features,
            'training_labels': labels,
            'total_samples': total_samples,
            'duplicate_samples': duplicate_samples,
            'non_duplicate_samples': non_duplicate_samples,
            'feature_importance': feature_importance,
            'model_metrics': model_metrics
        }
