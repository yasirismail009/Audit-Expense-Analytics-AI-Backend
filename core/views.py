"""
Clean views file with only the views used in the current URLs
"""

from rest_framework import viewsets, status, generics, serializers
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from django.db import transaction
from django.db.models import Sum, Count, Avg, Min, Max, Q
from django.utils import timezone
import pandas as pd
import csv
import io
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from rest_framework.pagination import PageNumberPagination
from django.db.models import Q

from .models import SAPGLPosting, DataFile, FileProcessingJob, MLModelTraining, OverallAnalysisResult, RiskScoringDocument, DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult
from .serializers import (
    DataFileSerializer, DataFileUploadSerializer, DataUploadResponseSerializer,
    FileProcessingJobSerializer, MLModelTrainingSerializer, TargetedAnomalyUploadSerializer,
    SAPGLPostingListSerializer
)
from .tasks import run_restructured_analysis

logger = logging.getLogger(__name__)

# ============================================================================
# UPLOAD AND FILE MANAGEMENT
# ============================================================================

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
            
            # Use bulk_create for better performance
            postings_to_create = []
            
            for row in csv_reader:
                try:
                    # Map CSV columns to model fields
                    posting = self._create_posting_from_row(row, data_file)
                    if posting:
                        postings_to_create.append(posting)
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
            
            # Bulk create all postings
            if postings_to_create:
                SAPGLPosting.objects.bulk_create(postings_to_create, batch_size=1000)
                logger.info(f"Successfully saved {len(postings_to_create)} postings to database")
            
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
            
            # Create processing job to trigger analysis
            self._create_processing_job(data_file)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {'success': False, 'error': str(e)}
    
    def _create_posting_from_row(self, row, data_file):
        """Create SAPGLPosting from CSV row"""
        try:
            # Map CSV columns to model fields
            posting = SAPGLPosting(
                data_file=data_file,  # Associate with the uploaded file
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
    
    def _create_processing_job(self, data_file):
        """Create a processing job to trigger analysis"""
        try:
            # Calculate file hash for duplicate detection
            import hashlib
            file_hash = hashlib.sha256(f"{data_file.file_name}{data_file.file_size}{data_file.uploaded_at}".encode()).hexdigest()
            
            # Create processing job
            processing_job = FileProcessingJob.objects.create(
                data_file=data_file,
                file_hash=file_hash,
                run_anomalies=True,  # Enable anomaly detection by default
                requested_anomalies=['duplicate', 'backdated', 'high_value', 'unusual_patterns'],
                status='PENDING'
            )
            
            # Trigger the analysis task
            from .tasks import run_restructured_analysis
            run_restructured_analysis.delay(str(processing_job.id))
            
            logger.info(f"Created processing job {processing_job.id} for file {data_file.file_name}")
            
        except Exception as e:
            logger.error(f"Error creating processing job: {e}")
            # Don't fail the upload if job creation fails
            pass

# ============================================================================
# PROCESSING JOBS
# ============================================================================

class FileProcessingJobViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for file processing jobs"""
    
    queryset = FileProcessingJob.objects.all()
    serializer_class = FileProcessingJobSerializer
    
    @action(detail=True, methods=['get'])
    def status(self, request, pk=None):
        """Get processing job status"""
        try:
            job = self.get_object()
            return Response({
                'job_id': str(job.id),
                'status': job.status,
                'started_at': job.started_at,
                'completed_at': job.completed_at,
                'processing_duration': job.processing_duration,
                'error_message': job.error_message,
                'analytics_results': job.analytics_results
            })
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ============================================================================
# ML MODEL TRAINING
# ============================================================================

class MLModelTrainingViewSet(viewsets.ModelViewSet):
    """ViewSet for ML model training"""
    
    queryset = MLModelTraining.objects.all()
    serializer_class = MLModelTrainingSerializer
    
    @action(detail=False, methods=['post'])
    def train_models(self, request):
        """Train ML models"""
        try:
            # Get the latest processing job or create a dummy one
            latest_job = FileProcessingJob.objects.filter(status='COMPLETED').order_by('-created_at').first()
            
            if not latest_job:
                return Response(
                    {'error': 'No completed processing jobs found. Please upload and process a file first.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Trigger ML model training
            from .tasks import train_ml_models
            result = train_ml_models.delay(str(latest_job.id))
            
            return Response({
                'message': 'ML model training started',
                'status': 'PENDING',
                'task_id': str(result.id),
                'job_id': str(latest_job.id)
            })
        except Exception as e:
            logger.error(f"Error training models: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def retrain_models(self, request):
        """Retrain ML models"""
        try:
            # Get the latest processing job or create a dummy one
            latest_job = FileProcessingJob.objects.filter(status='COMPLETED').order_by('-created_at').first()
            
            if not latest_job:
                return Response(
                    {'error': 'No completed processing jobs found. Please upload and process a file first.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Trigger ML model retraining
            from .tasks import retrain_ml_models
            result = retrain_ml_models.delay(str(latest_job.id))
            
            return Response({
                'message': 'ML model retraining started',
                'status': 'PENDING',
                'task_id': str(result.id),
                'job_id': str(latest_job.id)
            })
        except Exception as e:
            logger.error(f"Error retraining models: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def model_info(self, request):
        """Get model information"""
        try:
            # Implementation for getting model info
            return Response({
                'models': [],
                'latest_version': '1.0.0'
            })
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def predict_anomalies(self, request):
        """Predict anomalies"""
        try:
            # Implementation for predicting anomalies
            return Response({
                'predictions': [],
                'status': 'completed'
            })
        except Exception as e:
            logger.error(f"Error predicting anomalies: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=True, methods=['get'])
    def status(self, request, pk=None):
        """Get training status"""
        try:
            training = self.get_object()
            return Response({
                'training_id': str(training.id),
                'status': training.status,
                'started_at': training.started_at,
                'completed_at': training.completed_at,
                'training_duration': training.training_duration,
                'error_message': training.error_message
            })
        except Exception as e:
            logger.error(f"Error getting training status: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ============================================================================
# DEBUG AND MONITORING
# ============================================================================

class CeleryDebugViewSet(viewsets.ViewSet):
    """ViewSet for Celery debugging and monitoring"""
    
    @action(detail=False, methods=['get'])
    def health_check(self, request):
        """Health check for Celery workers"""
        try:
            return Response({
                'status': 'healthy',
                'timestamp': timezone.now()
            })
        except Exception as e:
            logger.error(f"Error in health check: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ============================================================================
# UPLOAD AND PROCESSING VIEWS
# ============================================================================

class TargetedAnomalyUploadView(generics.CreateAPIView):
    """View for targeted anomaly upload"""
    
    serializer_class = TargetedAnomalyUploadSerializer
    parser_classes = (MultiPartParser, FormParser)
    
    def create(self, request, *args, **kwargs):
        try:
            # Validate serializer
            serializer = self.get_serializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            # Get file and metadata
            file_obj = request.FILES.get('file')
            if not file_obj:
                return Response(
                    {'error': 'No file provided'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get additional fields from request data
            engagement_id = serializer.validated_data.get('engagement_id', '')
            client_name = serializer.validated_data.get('client_name', '')
            company_name = serializer.validated_data.get('company_name', '')
            fiscal_year = serializer.validated_data.get('fiscal_year', 2025)
            audit_start_date = serializer.validated_data.get('audit_start_date')
            audit_end_date = serializer.validated_data.get('audit_end_date')
            description = serializer.validated_data.get('description', '')
            
            # Get anomaly detection configuration
            run_anomalies = serializer.validated_data.get('run_anomalies', False)
            anomalies = serializer.validated_data.get('anomalies', [])
            
            # Validate file type
            if not file_obj.name.endswith('.csv'):
                return Response(
                    {'error': 'Only CSV files are supported'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Create DataFile record
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
            
            # Process CSV file
            result = self._process_csv_file(data_file, file_obj)
            
            if result['success']:
                # Create processing job with anomaly detection configuration
                processing_job = self._create_processing_job(data_file, run_anomalies, anomalies)
                
                # Return response
                response_data = {
                    'message': 'Targeted anomaly upload completed successfully',
                    'status': 'success',
                    'file_id': str(data_file.id),
                    'job_id': str(processing_job.id) if processing_job else None,
                    'processed_records': data_file.processed_records,
                    'total_records': data_file.total_records,
                    'run_anomalies': run_anomalies,
                    'requested_anomalies': anomalies
                }
                
                return Response(response_data, status=status.HTTP_201_CREATED)
            else:
                return Response(
                    {'error': result['error']}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
                
        except Exception as e:
            logger.error(f"Error in targeted anomaly upload: {e}")
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
            
            # Use bulk_create for better performance
            postings_to_create = []
            
            for row in csv_reader:
                try:
                    # Map CSV columns to model fields
                    posting = self._create_posting_from_row(row, data_file)
                    if posting:
                        postings_to_create.append(posting)
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
            
            # Bulk create all postings
            if postings_to_create:
                SAPGLPosting.objects.bulk_create(postings_to_create, batch_size=1000)
                logger.info(f"Successfully saved {len(postings_to_create)} postings to database")
            
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
    
    def _create_posting_from_row(self, row, data_file):
        """Create SAPGLPosting from CSV row"""
        try:
            # Map CSV columns to model fields
            posting = SAPGLPosting(
                data_file=data_file,  # Associate with the uploaded file
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
    
    def _create_processing_job(self, data_file, run_anomalies=False, anomalies=None):
        """Create a processing job with anomaly detection configuration"""
        try:
            # Calculate file hash for duplicate detection
            import hashlib
            file_hash = hashlib.sha256(f"{data_file.file_name}{data_file.file_size}{data_file.uploaded_at}".encode()).hexdigest()
            
            # Create processing job
            processing_job = FileProcessingJob.objects.create(
                data_file=data_file,
                file_hash=file_hash,
                run_anomalies=run_anomalies,
                requested_anomalies=anomalies or [],
                status='PENDING'
            )
            
            # Trigger the analysis task
            from .tasks import run_restructured_analysis
            run_restructured_analysis.delay(str(processing_job.id))
            
            logger.info(f"Created processing job {processing_job.id} for file {data_file.file_name}")
            return processing_job
            
        except Exception as e:
            logger.error(f"Error creating processing job: {e}")
            # Don't fail the upload if job creation fails
            return None



class FileListingAPIView(generics.GenericAPIView):
    """View for listing all files with processing status"""
    
    def get(self, request):
        try:
            # Get query parameters for filtering
            status_filter = request.query_params.get('status', None)
            engagement_id = request.query_params.get('engagement_id', None)
            client_name = request.query_params.get('client_name', None)
            company_name = request.query_params.get('company_name', None)
            fiscal_year = request.query_params.get('fiscal_year', None)
            date_from = request.query_params.get('date_from', None)
            date_to = request.query_params.get('date_to', None)
            
            # Start with all files
            files = DataFile.objects.all().order_by('-uploaded_at')
            
            # Apply filters
            if status_filter:
                files = files.filter(status=status_filter)
            
            if engagement_id:
                files = files.filter(engagement_id__icontains=engagement_id)
            
            if client_name:
                files = files.filter(client_name__icontains=client_name)
            
            if company_name:
                files = files.filter(company_name__icontains=company_name)
            
            if fiscal_year:
                files = files.filter(fiscal_year=fiscal_year)
            
            if date_from:
                files = files.filter(uploaded_at__date__gte=date_from)
            
            if date_to:
                files = files.filter(uploaded_at__date__lte=date_to)
            
            # Prepare response data
            file_list = []
            for file_obj in files:
                # Get processing job for this file
                processing_job = FileProcessingJob.objects.filter(data_file=file_obj).first()
                
                file_data = {
                    'id': str(file_obj.id),
                    'file_name': file_obj.file_name,
                    'file_size': file_obj.file_size,
                    'engagement_id': file_obj.engagement_id,
                    'client_name': file_obj.client_name,
                    'company_name': file_obj.company_name,
                    'fiscal_year': file_obj.fiscal_year,
                    'audit_start_date': file_obj.audit_start_date,
                    'audit_end_date': file_obj.audit_end_date,
                    'uploaded_at': file_obj.uploaded_at,
                    'processed_at': file_obj.processed_at,
                    'file_status': file_obj.status,
                    'total_records': file_obj.total_records,
                    'processed_records': file_obj.processed_records,
                    'failed_records': file_obj.failed_records,
                    'min_date': file_obj.min_date,
                    'max_date': file_obj.max_date,
                    'min_amount': file_obj.min_amount,
                    'max_amount': file_obj.max_amount,
                    'error_message': file_obj.error_message,
                    'processing_status': 'NOT_STARTED'
                }
                
                # Add processing job information if exists
                if processing_job:
                    file_data.update({
                        'processing_status': processing_job.status,
                        'processing_started_at': processing_job.started_at,
                        'processing_completed_at': processing_job.completed_at,
                        'processing_duration': processing_job.processing_duration,
                        'processing_error_message': processing_job.error_message,
                        'job_id': str(processing_job.id)
                    })
                
                file_list.append(file_data)
            
            # Calculate summary statistics
            total_files = len(file_list)
            processed_files = len([f for f in file_list if f['file_status'] == 'COMPLETED'])
            pending_files = len([f for f in file_list if f['file_status'] == 'PENDING'])
            failed_files = len([f for f in file_list if f['file_status'] == 'FAILED'])
            processing_files = len([f for f in file_list if f['processing_status'] in ['PROCESSING', 'QUEUED']])
            
            return Response({
                'files': file_list,
                'summary': {
                    'total_files': total_files,
                    'processed_files': processed_files,
                    'pending_files': pending_files,
                    'failed_files': failed_files,
                    'processing_files': processing_files
                },
                'filters_applied': {
                    'status': status_filter,
                    'engagement_id': engagement_id,
                    'client_name': client_name,
                    'company_name': company_name,
                    'fiscal_year': fiscal_year,
                    'date_from': date_from,
                    'date_to': date_to
                }
            })
            
        except Exception as e:
            logger.error(f"Error getting file listing: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ============================================================================
# ANALYSIS STATISTICS VIEWS
# ============================================================================

class FileAnalysisStatisticsView(generics.GenericAPIView):
    """Critical view for retrieving comprehensive file analysis statistics and chart data"""
    
    def get(self, request, file_id):
        """
        Get comprehensive overall and risk analysis statistics and chart data for a specific file
        
        Args:
            file_id (str): UUID of the DataFile to get analysis for
            
        Returns:
            JSON response with comprehensive statistics and chart data
        """
        try:
            # Validate file exists with enhanced error handling
            try:
                data_file = DataFile.objects.select_related().get(id=file_id)
            except DataFile.DoesNotExist:
                return Response(
                    {
                        'error': f'File with ID {file_id} not found',
                        'error_code': 'FILE_NOT_FOUND',
                        'suggestions': ['Verify the file ID is correct', 'Check if the file has been uploaded']
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            except Exception as e:
                logger.error(f"Database error retrieving file {file_id}: {e}")
                return Response(
                    {
                        'error': 'Database error occurred while retrieving file',
                        'error_code': 'DATABASE_ERROR',
                        'details': str(e)
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Get the latest overall analysis result for this file with enhanced validation
            overall_analysis = OverallAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not overall_analysis:
                logger.warning(f"No completed overall analysis found for file {file_id}")
            
            # Get the latest risk scoring document for this file with enhanced validation
            risk_document = RiskScoringDocument.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-document_date').first()
            
            if not risk_document:
                logger.warning(f"No completed risk analysis found for file {file_id}")
            
            # Get the latest user analysis result for this file with enhanced validation
            user_analysis = UserAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not user_analysis:
                logger.warning(f"No completed user analysis found for file {file_id}")
            
            # Validate file processing status
            if data_file.status != 'COMPLETED':
                logger.warning(f"File {file_id} is not in COMPLETED status. Current status: {data_file.status}")
            
            # Prepare response data with enhanced structure and validation
            response_data = {
                'file_info': {
                    'file_id': str(data_file.id),
                    'file_name': data_file.file_name,
                    'engagement_id': data_file.engagement_id,
                    'client_name': data_file.client_name,
                    'company_name': data_file.company_name,
                    'fiscal_year': data_file.fiscal_year,
                    'uploaded_at': data_file.uploaded_at,
                    'processed_at': data_file.processed_at,
                    'total_records': data_file.total_records,
                    'processed_records': data_file.processed_records,
                    'failed_records': data_file.failed_records,
                    'file_status': data_file.status,
                    'processing_success_rate': (data_file.processed_records / data_file.total_records * 100) if data_file.total_records > 0 else 0
                },
                'overall_analysis': {},
                'risk_analysis': {},
                'user_analysis': {},
                'chart_data': {},
                'analysis_metadata': {
                    'has_overall_analysis': bool(overall_analysis),
                    'has_risk_analysis': bool(risk_document),
                    'has_user_analysis': bool(user_analysis),
                    'analysis_timestamp': timezone.now().isoformat(),
                    'analysis_version': '2.0.0'
                }
            }
            
            # Overall Analysis Statistics with enhanced error handling
            if overall_analysis:
                try:
                    # Get actual transaction data for enhanced statistics with optimization
                    transactions = SAPGLPosting.objects.filter(data_file=data_file).select_related()
                
                    # Enhanced transaction summary with real data and validation
                    enhanced_transaction_summary = self._enhance_transaction_summary(
                        overall_analysis.transaction_summary, transactions
                    )
                
                    # Enhanced flag summary with real data and validation
                    enhanced_flag_summary = self._enhance_flag_summary(
                        overall_analysis.flag_summary, transactions
                    )
                    
                    # Add debugging information for overall analysis
                    enhanced_flag_summary['debug_info'] = {
                        'backdated_anomalies_count': enhanced_flag_summary.get('backdated_anomalies', 0),
                        'duplicate_anomalies_count': enhanced_flag_summary.get('duplicate_anomalies', 0),
                        'high_value_anomalies_count': enhanced_flag_summary.get('high_value_anomalies', 0),
                        'total_anomalies': enhanced_flag_summary.get('total_anomalies', 0),
                        'debug_message': f'Overall analysis: {enhanced_flag_summary.get("backdated_anomalies", 0)} backdated, {enhanced_flag_summary.get("duplicate_anomalies", 0)} duplicates, {enhanced_flag_summary.get("high_value_anomalies", 0)} high value'
                    }
                
                    # Enhanced risk assessment with real data and validation
                    enhanced_risk_assessment = self._enhance_risk_assessment(
                        overall_analysis.risk_assessment, transactions
                    )
                
                    # Chart data removed from overall analysis to prevent duplication
                    enhanced_chart_data = {}
                    
                    # Add critical alerts and warnings
                    critical_alerts = self._generate_critical_alerts(enhanced_transaction_summary, enhanced_flag_summary, enhanced_risk_assessment)
                    
                except Exception as e:
                    logger.error(f"Error enhancing overall analysis for file {file_id}: {e}")
                    enhanced_transaction_summary = overall_analysis.transaction_summary or {}
                    enhanced_flag_summary = overall_analysis.flag_summary or {}
                    enhanced_risk_assessment = overall_analysis.risk_assessment or {}
                    enhanced_chart_data = overall_analysis.chart_data or {}
                    critical_alerts = [{'type': 'ERROR', 'message': f'Error enhancing analysis: {str(e)}'}]
                
                response_data['overall_analysis'] = {
                    'analysis_id': str(overall_analysis.id),
                    'analysis_date': overall_analysis.analysis_date,
                    'processing_duration': overall_analysis.processing_duration,
                    'status': overall_analysis.status,
                    
                    # Enhanced Transaction Summary Statistics
                    'transaction_summary': enhanced_transaction_summary,
                    
                    # Enhanced Flag Summary Statistics
                    'flag_summary': enhanced_flag_summary,
                    
                    # Expense Analysis Statistics
                    'expense_analysis': overall_analysis.expense_analysis,
                    
                    # Enhanced Risk Assessment Statistics
                    'risk_assessment': enhanced_risk_assessment,
                    
                    # Enhanced Chart Data (only for overall analysis)
                    'chart_data': enhanced_chart_data,
                    
                    # Critical Alerts and Warnings
                    'critical_alerts': critical_alerts
                }
            else:
                response_data['overall_analysis'] = {
                    'error': 'No overall analysis results found for this file',
                    'status': 'NOT_AVAILABLE'
                }
            
            # Risk Analysis Statistics with enhanced error handling
            if risk_document:
                try:
                    # Get actual transaction data for enhanced risk analysis with optimization
                    transactions = SAPGLPosting.objects.filter(data_file=data_file).select_related()
                
                    # Enhanced risk analysis with real data and validation
                    enhanced_risk_analysis = self._enhance_risk_analysis(risk_document, transactions)
                    
                    # Add debugging information for transaction-based anomalies
                    backdated_count = transactions.filter(is_backdated=True).count()
                    duplicate_count = transactions.filter(is_duplicate=True).count()
                    high_value_count = len([t for t in transactions if t.is_high_value])
                    
                    debug_info = {
                        'backdated_anomalies_count': backdated_count,
                        'duplicate_anomalies_count': duplicate_count,
                        'high_value_anomalies_count': high_value_count,
                        'total_transactions': transactions.count(),
                        'backdated_percentage': (backdated_count / transactions.count() * 100) if transactions.count() > 0 else 0,
                        'debug_message': f'Found {backdated_count} backdated anomalies out of {transactions.count()} total transactions',
                        'clarification': 'Risk analysis is a comprehensive analysis that includes transaction-based and user-based anomalies'
                    }
                    
                    # Add user anomalies information if available
                    if user_analysis:
                        user_anomalies_count = len(user_analysis.user_anomalies or [])
                        high_risk_users_count = user_analysis.get_high_risk_users_count()
                        total_users_count = user_analysis.get_total_users()
                        
                        debug_info.update({
                            'user_anomalies_count': user_anomalies_count,
                            'high_risk_users_count': high_risk_users_count,
                            'total_users_count': total_users_count,
                            'user_anomaly_percentage': (user_anomalies_count / total_users_count * 100) if total_users_count > 0 else 0,
                            'debug_message': f'{debug_info["debug_message"]}, {user_anomalies_count} user anomalies out of {total_users_count} users',
                            'clarification': 'Risk analysis includes both transaction-based anomalies (duplicates, backdated, high-value) and user-based anomalies (behavior patterns, risk factors)'
                        })
                        
                        # Add user anomalies to risk analysis
                        enhanced_risk_analysis['user_anomalies'] = {
                            'total_user_anomalies': user_anomalies_count,
                            'high_risk_users_count': high_risk_users_count,
                            'total_users_count': total_users_count,
                            'user_anomaly_percentage': (user_anomalies_count / total_users_count * 100) if total_users_count > 0 else 0,
                            'user_anomaly_types_count': len(self._get_user_anomaly_type_summary(user_analysis.user_anomalies or [])),
                            'user_anomaly_severity_distribution': self._get_user_anomaly_severity_distribution(user_analysis.user_anomalies or []),
                            'user_risk_distribution': self._get_user_risk_distribution(user_analysis.user_risk_assessment)
                        }
                        
                        # Update user behavior risk factor in risk_factors
                        if 'risk_factors' in enhanced_risk_analysis:
                            enhanced_risk_analysis['risk_factors']['user_behavior_risk'] = {
                                'count': user_anomalies_count,
                                'percentage': (user_anomalies_count / total_users_count * 100) if total_users_count > 0 else 0,
                                'description': 'Risk associated with user behavior patterns and anomalies',
                                'high_risk_users_count': high_risk_users_count,
                                'high_risk_users_percentage': (high_risk_users_count / total_users_count * 100) if total_users_count > 0 else 0
                            }
                    
                    enhanced_risk_analysis['debug_info'] = debug_info
                
                    response_data['risk_analysis'] = enhanced_risk_analysis
                except Exception as e:
                    logger.error(f"Error enhancing risk analysis for file {file_id}: {e}")
                    response_data['risk_analysis'] = {
                        'error': f'Error enhancing risk analysis: {str(e)}',
                        'status': 'ERROR',
                        'original_data': {
                            'document_id': str(risk_document.id),
                            'document_date': risk_document.document_date,
                            'status': risk_document.status
                        }
                    }
            else:
                response_data['risk_analysis'] = {
                    'error': 'No risk analysis document found for this file',
                    'status': 'NOT_AVAILABLE',
                    'suggestions': ['Run risk analysis for this file', 'Check if risk analysis processing completed successfully']
                }
            
            # User Analysis Statistics with enhanced error handling
            if user_analysis:
                try:
                    # Get actual transaction data for enhanced user analysis with optimization
                    transactions = SAPGLPosting.objects.filter(data_file=data_file).select_related()
                    
                    # Enhanced user analysis with real data and validation
                    enhanced_user_analysis = self._enhance_user_analysis(user_analysis, transactions)
                    
                    # Add debugging information for user anomalies
                    user_anomalies_count = len(user_analysis.user_anomalies or [])
                    high_risk_users_count = user_analysis.get_high_risk_users_count()
                    total_users_count = user_analysis.get_total_users()
                    
                    enhanced_user_analysis['debug_info'] = {
                        'user_anomalies_count': user_anomalies_count,
                        'high_risk_users_count': high_risk_users_count,
                        'total_users_count': total_users_count,
                        'anomaly_percentage': (user_anomalies_count / total_users_count * 100) if total_users_count > 0 else 0,
                        'high_risk_percentage': (high_risk_users_count / total_users_count * 100) if total_users_count > 0 else 0,
                        'debug_message': f'Found {user_anomalies_count} user anomalies out of {total_users_count} total users',
                        'clarification': 'User analysis focuses on user behavior patterns and anomalies, complementing transaction-based anomaly detection'
                    }
                    
                    response_data['user_analysis'] = enhanced_user_analysis
                except Exception as e:
                    logger.error(f"Error enhancing user analysis for file {file_id}: {e}")
                    response_data['user_analysis'] = {
                        'error': f'Error enhancing user analysis: {str(e)}',
                        'status': 'ERROR',
                        'original_data': {
                            'analysis_id': str(user_analysis.id),
                            'analysis_date': user_analysis.analysis_date,
                            'status': user_analysis.status
                        }
                    }
            else:
                response_data['user_analysis'] = {
                    'error': 'No user analysis results found for this file',
                    'status': 'NOT_AVAILABLE',
                    'suggestions': ['Run user analysis for this file', 'Check if user analysis processing completed successfully']
                }
            
            # Combined Chart Data (only for overall and risk analysis)
            combined_chart_data = {}
            
            if overall_analysis and overall_analysis.chart_data:
                combined_chart_data.update(overall_analysis.chart_data)
            
            # Add risk-specific chart data if available
            if risk_document:
                # Create ANOMALY-BASED risk distribution chart data (not transaction-based)
                # Get transactions for this file to calculate anomaly distribution
                transactions = SAPGLPosting.objects.filter(data_file=data_file)
                
                # Calculate anomaly counts
                duplicate_anomalies = transactions.filter(is_duplicate=True).count()
                backdated_anomalies = transactions.filter(is_backdated=True).count()
                high_value_anomalies = len([t for t in transactions if t.is_high_value])
                
                # Add debugging information for chart data
                chart_debug_info = {
                    'duplicate_anomalies': duplicate_anomalies,
                    'backdated_anomalies': backdated_anomalies,
                    'high_value_anomalies': high_value_anomalies,
                    'total_transactions': transactions.count(),
                    'debug_message': f'Chart data: {duplicate_anomalies} duplicates, {backdated_anomalies} backdated, {high_value_anomalies} high value'
                }
                
                # Add user anomalies information if available
                if user_analysis:
                    user_anomalies_count = len(user_analysis.user_anomalies or [])
                    high_risk_users_count = user_analysis.get_high_risk_users_count()
                    chart_debug_info.update({
                        'user_anomalies': user_anomalies_count,
                        'high_risk_users': high_risk_users_count,
                        'debug_message': f'{chart_debug_info["debug_message"]}, {user_anomalies_count} user anomalies, {high_risk_users_count} high-risk users'
                    })
                
                # Calculate anomaly distribution based on actual anomaly counts
                # This is the correct approach - show individual anomalies, not merged transactions
                
                # Initialize anomaly counts by risk level
                low_risk_anomalies = 0
                medium_risk_anomalies = 0
                high_risk_anomalies = 0
                critical_risk_anomalies = 0
                
                # Distribute anomalies based on their risk levels
                # Duplicate anomalies are typically high/critical risk
                if duplicate_anomalies > 0:
                    critical_risk_anomalies += duplicate_anomalies
                
                # Backdated anomalies are typically high risk
                if backdated_anomalies > 0:
                    high_risk_anomalies += backdated_anomalies
                
                # High value anomalies are typically medium risk
                if high_value_anomalies > 0:
                    medium_risk_anomalies += high_value_anomalies
                
                combined_chart_data['risk_distribution_chart'] = {
                    'labels': ['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk'],
                    'data': [
                        low_risk_anomalies,
                        medium_risk_anomalies,
                        high_risk_anomalies,
                        critical_risk_anomalies
                    ],
                    'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384'],
                    'description': 'Distribution of anomalies across risk levels (not transactions)',
                    'total_anomalies': low_risk_anomalies + medium_risk_anomalies + high_risk_anomalies + critical_risk_anomalies,
                    'debug_info': chart_debug_info
                }
                
                # Add overall risk score gauge chart with comprehensive definitions
                combined_chart_data['overall_risk_gauge'] = {
                    'value': risk_document.overall_risk_score,
                    'max_value': 100,
                    'risk_level': self._get_risk_level(risk_document.overall_risk_score),
                    'color': self._get_risk_color(risk_document.overall_risk_score),
                    'definition': {
                        'title': 'Overall Risk Score Gauge',
                        'description': 'Comprehensive risk assessment indicator that evaluates the average risk level across all transactions in the dataset',
                        'calculation_method': 'Weighted average of individual transaction risk scores',
                        'scale': '0-100 (0 = No Risk, 100 = Maximum Risk)',
                        'update_frequency': 'Real-time with each analysis run'
                    },
                    'risk_levels': {
                        'CRITICAL': {
                            'range': '80-100',
                            'description': 'Extremely high risk requiring immediate attention',
                            'color': '#FF6384',
                            'actions': [
                                'Immediate audit review required',
                                'Fraud investigation recommended',
                                'Regulatory compliance review needed',
                                'Internal control assessment mandatory'
                            ]
                        },
                        'HIGH': {
                            'range': '60-79',
                            'description': 'High risk requiring prompt investigation',
                            'color': '#FF9F40',
                            'actions': [
                                'Detailed transaction review',
                                'Risk factor analysis',
                                'Control effectiveness assessment',
                                'Management oversight required'
                            ]
                        },
                        'MEDIUM': {
                            'range': '30-59',
                            'description': 'Moderate risk with some concerns',
                            'color': '#FFCE56',
                            'actions': [
                                'Selective transaction review',
                                'Pattern analysis recommended',
                                'Monitoring enhancement',
                                'Periodic reassessment'
                            ]
                        },
                        'LOW': {
                            'range': '0-29',
                            'description': 'Low risk with normal transaction patterns',
                            'color': '#4BC0C0',
                            'actions': [
                                'Routine monitoring',
                                'Standard audit procedures',
                                'Regular risk assessment',
                                'Baseline establishment'
                            ]
                        }
                    },
                    'calculation_details': {
                        'formula': 'Overall Risk Score = Σ(Individual Transaction Risk Scores) / Total Transactions',
                        'components': {
                            'duplicate_analysis': {
                                'weight': '25-30 points',
                                'description': 'Risk from duplicate transaction detection'
                            },
                            'backdated_analysis': {
                                'weight': '25-30 points', 
                                'description': 'Risk from backdated entry detection'
                            },
                            'high_value_analysis': {
                                'weight': '10-15 points',
                                'description': 'Risk from transactions exceeding 1M SAR threshold'
                            },
                            'general_anomaly': {
                                'weight': '10-20 points',
                                'description': 'Risk from unusual patterns and ML-based anomaly detection'
                            }
                        },
                        'scaling_factors': {
                            'amount_multiplier': 'Risk increases with transaction amount',
                            'frequency_multiplier': 'Risk increases with anomaly frequency',
                            'combination_effect': 'Multiple anomalies per transaction have multiplicative effect'
                        }
                    },
                    'interpretation_guide': {
                        'score_0_20': 'Excellent - Minimal risk indicators detected',
                        'score_21_40': 'Good - Some risk factors present but manageable',
                        'score_41_60': 'Fair - Moderate risk requiring attention',
                        'score_61_80': 'Poor - High risk requiring immediate action',
                        'score_81_100': 'Critical - Maximum risk requiring urgent intervention'
                    },
                    'business_impact': {
                        'financial_risk': 'Potential for financial loss or misstatement',
                        'compliance_risk': 'Risk of regulatory violations or audit findings',
                        'operational_risk': 'Risk of process failures or control weaknesses',
                        'reputational_risk': 'Risk of damage to organizational reputation'
                    }
                }
            
            response_data['chart_data'] = combined_chart_data
            
            # Add all GL accounts data (not just top ones)
            try:
                transactions = SAPGLPosting.objects.filter(data_file=data_file)
                all_gl_accounts = self._generate_all_gl_accounts_data(transactions)
                response_data['all_gl_accounts'] = {
                    'total_accounts': len(all_gl_accounts),
                    'accounts': all_gl_accounts,
                    'summary': {
                        'total_transactions': sum(acc['transaction_count'] for acc in all_gl_accounts),
                        'total_amount': sum(acc['total_amount'] for acc in all_gl_accounts),
                        'avg_risk_score': sum(acc['avg_risk_score'] for acc in all_gl_accounts) / len(all_gl_accounts) if all_gl_accounts else 0,
                        'accounts_with_anomalies': len([acc for acc in all_gl_accounts if acc['anomaly_counts']['total_anomalies'] > 0]),
                        'high_risk_accounts': len([acc for acc in all_gl_accounts if acc['risk_level'] in ['HIGH', 'CRITICAL']])
                    }
                }
            except Exception as e:
                logger.warning(f"Error generating all GL accounts data: {e}")
                response_data['all_gl_accounts'] = {
                    'error': f'Error generating GL accounts data: {str(e)}',
                    'total_accounts': 0,
                    'accounts': []
                }
            
            # Add final validation and summary
            response_data['summary'] = {
                'total_alerts': len(response_data.get('overall_analysis', {}).get('critical_alerts', [])),
                'critical_issues': len([a for a in response_data.get('overall_analysis', {}).get('critical_alerts', []) if a.get('type') == 'CRITICAL']),
                'warnings': len([a for a in response_data.get('overall_analysis', {}).get('critical_alerts', []) if a.get('type') == 'WARNING']),
                'analysis_quality': 'HIGH' if response_data['analysis_metadata']['has_overall_analysis'] and response_data['analysis_metadata']['has_risk_analysis'] else 'PARTIAL',
                'user_anomalies_summary': {
                    'total_user_anomalies': user_analysis.get_anomalies_count() if user_analysis else 0,
                    'high_risk_users': user_analysis.get_high_risk_users_count() if user_analysis else 0,
                    'total_users': user_analysis.get_total_users() if user_analysis else 0,
                    'anomaly_percentage': (user_analysis.get_anomalies_count() / user_analysis.get_total_users() * 100) if user_analysis and user_analysis.get_total_users() > 0 else 0
                } if user_analysis else {}
            }
            
            # Log successful response for monitoring
            logger.info(f"Successfully retrieved analysis statistics for file {file_id} with {response_data['summary']['total_alerts']} alerts")
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Critical error getting file analysis statistics for file {file_id}: {e}")
            return Response(
                {
                    'error': 'Critical error occurred while retrieving file analysis statistics',
                    'error_code': 'ANALYSIS_ERROR',
                    'details': str(e),
                    'file_id': file_id,
                    'timestamp': timezone.now().isoformat(),
                    'suggestions': [
                        'Check if the file has been properly processed',
                        'Verify database connectivity',
                        'Review system logs for additional details'
                    ]
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_risk_level(self, risk_score):
        """
        Get risk level classification based on risk score
        
        Risk Level Classification System:
        - CRITICAL (80-100): Extremely high risk requiring immediate attention
        - HIGH (60-79): High risk requiring prompt investigation  
        - MEDIUM (30-59): Moderate risk with some concerns
        - LOW (0-29): Low risk with normal transaction patterns
        
        Args:
            risk_score (float): Risk score between 0-100
            
        Returns:
            str: Risk level classification
        """
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_risk_color(self, risk_score):
        """
        Get color code for risk score visualization
        
        Color Coding System:
        - Red (#FF6384): Critical risk (80-100)
        - Orange (#FF9F40): High risk (60-79)
        - Yellow (#FFCE56): Medium risk (30-59)
        - Green (#4BC0C0): Low risk (0-29)
        
        Args:
            risk_score (float): Risk score between 0-100
            
        Returns:
            str: Hexadecimal color code for visualization
        """
        if risk_score >= 80:
            return '#FF6384'  # Red for critical
        elif risk_score >= 60:
            return '#FF9F40'  # Orange for high
        elif risk_score >= 30:
            return '#FFCE56'  # Yellow for medium
        else:
            return '#4BC0C0'  # Green for low
    
    def _calculate_percentile(self, queryset, field_name, percentile):
        """Calculate percentile for a given field"""
        try:
            values = list(queryset.values_list(field_name, flat=True))
            if not values:
                return 0.0
            
            values.sort()
            index = (percentile / 100) * (len(values) - 1)
            
            if index.is_integer():
                return float(values[int(index)])
            else:
                lower_index = int(index)
                upper_index = lower_index + 1
                lower_value = values[lower_index]
                upper_value = values[upper_index] if upper_index < len(values) else lower_value
                
                # Linear interpolation
                weight = index - lower_index
                return float(lower_value + weight * (upper_value - lower_value))
        except Exception:
            return 0.0
    
    def _enhance_transaction_summary(self, original_summary, transactions):
        """Enhance transaction summary with real data"""
        if not transactions.exists():
            return original_summary
        
        # Calculate real statistics
        total_transactions = transactions.count()
        total_amount = transactions.aggregate(total=Sum('amount_local_currency'))['total'] or Decimal('0.00')
        unique_users = transactions.values('user_name').distinct().count()
        unique_accounts = transactions.values('gl_account').distinct().count()
        
        # Amount statistics
        amounts = [float(t.amount_local_currency) for t in transactions]
        amount_stats = {
            'mean': float(sum(amounts) / len(amounts)) if amounts else 0,
            'median': float(sorted(amounts)[len(amounts)//2]) if amounts else 0,
            'min': float(min(amounts)) if amounts else 0,
            'max': float(max(amounts)) if amounts else 0,
            'std': float((sum((x - sum(amounts)/len(amounts))**2 for x in amounts) / len(amounts))**0.5) if amounts else 0
        }
        
        # Date range
        date_range = transactions.aggregate(
            min_date=Min('posting_date'),
            max_date=Max('posting_date')
        )
        
        # Currency
        currency = transactions.first().local_currency if transactions.exists() else 'SAR'
        
        enhanced_summary = {
            'total_transactions': total_transactions,
            'total_amount': float(total_amount),
            'unique_users': unique_users,
            'unique_accounts': unique_accounts,
            'average_transaction_amount': float(total_amount / total_transactions) if total_transactions > 0 else 0,
            'amount_statistics': amount_stats,
            'date_range': {
                'min_date': date_range['min_date'].isoformat() if date_range['min_date'] else None,
                'max_date': date_range['max_date'].isoformat() if date_range['max_date'] else None
            },
            'currency': currency
        }
        
        # Merge with original summary if it has additional fields
        if isinstance(original_summary, dict):
            enhanced_summary.update(original_summary)
        
        return enhanced_summary
    
    def _enhance_flag_summary(self, original_summary, transactions):
        """Enhance flag summary with real data"""
        if not transactions.exists():
            return original_summary
        
        # Calculate real anomaly statistics
        duplicate_count = transactions.filter(is_duplicate=True).count()
        backdated_count = transactions.filter(is_backdated=True).count()
        high_value_count = len([t for t in transactions if t.is_high_value])
        
        # Calculate unique transactions with any anomaly using Q objects (more reliable than union)
        from django.db.models import Q
        unique_anomaly_transactions = transactions.filter(
            Q(is_duplicate=True) | Q(is_backdated=True) | Q(amount_local_currency__gt=1000000)
        )
        
        total_flagged = unique_anomaly_transactions.count()
        
        # Risk level distribution based on overall_risk_score
        low_risk = transactions.filter(overall_risk_score__lt=30).count()
        medium_risk = transactions.filter(overall_risk_score__gte=30, overall_risk_score__lt=60).count()
        high_risk = transactions.filter(overall_risk_score__gte=60, overall_risk_score__lt=80).count()
        critical_risk = transactions.filter(overall_risk_score__gte=80).count()
        
        enhanced_summary = {
            'total_anomalies': duplicate_count + backdated_count + high_value_count,
            'duplicate_anomalies': duplicate_count,
            'backdated_anomalies': backdated_count,
            'high_value_anomalies': high_value_count,
            'low_risk_anomalies': low_risk,
            'medium_risk_anomalies': medium_risk,
            'high_risk_anomalies': high_risk,
            'critical_risk_anomalies': critical_risk,
            'total_flagged': total_flagged,  # Use the correct count of unique transactions with anomalies
            'anomaly_rate': ((duplicate_count + backdated_count + high_value_count) / transactions.count() * 100) if transactions.count() > 0 else 0
        }
        
        # Merge with original summary if it has additional fields, but ensure our calculated values take precedence
        if isinstance(original_summary, dict):
            # Start with original summary, then override with our calculated values
            final_summary = original_summary.copy()
            final_summary.update(enhanced_summary)
            return final_summary
        
        return enhanced_summary
    
    def _enhance_risk_assessment(self, original_assessment, transactions):
        """Enhance risk assessment with real data"""
        if not transactions.exists():
            return original_assessment
        
        # Calculate real risk statistics
        total_transactions = transactions.count()
        avg_risk_score = transactions.aggregate(avg=Avg('overall_risk_score'))['avg'] or 0
        max_risk_score = transactions.aggregate(max=Max('overall_risk_score'))['max'] or 0
        min_risk_score = transactions.aggregate(min=Min('overall_risk_score'))['min'] or 0
        
        # Risk distribution
        risk_distribution = {
            'low_risk': transactions.filter(overall_risk_score__lt=30).count(),
            'medium_risk': transactions.filter(overall_risk_score__gte=30, overall_risk_score__lt=60).count(),
            'high_risk': transactions.filter(overall_risk_score__gte=60, overall_risk_score__lt=80).count(),
            'critical_risk': transactions.filter(overall_risk_score__gte=80).count()
        }
        
        # High-risk transactions
        high_risk_transactions = transactions.filter(overall_risk_score__gte=60).values(
            'id', 'document_number', 'gl_account', 'amount_local_currency', 
            'user_name', 'posting_date', 'overall_risk_score'
        )[:10]  # Top 10 high-risk transactions
        
        enhanced_assessment = {
            'overall_risk_score': float(avg_risk_score),
            'max_risk_score': float(max_risk_score),
            'min_risk_score': float(min_risk_score),
            'risk_level': self._get_risk_level(avg_risk_score),
            'risk_distribution': risk_distribution,
            'high_risk_transactions': list(high_risk_transactions),
            'total_transactions': total_transactions,
            'risk_percentiles': {
                'p25': self._calculate_percentile(transactions, 'overall_risk_score', 25),
                'p50': self._calculate_percentile(transactions, 'overall_risk_score', 50),
                'p75': self._calculate_percentile(transactions, 'overall_risk_score', 75),
                'p90': self._calculate_percentile(transactions, 'overall_risk_score', 90)
            }
        }
        
        # Merge with original assessment if it has additional fields
        if isinstance(original_assessment, dict):
            enhanced_assessment.update(original_assessment)
        
        return enhanced_assessment
    

    
    def _enhance_risk_analysis(self, risk_document, transactions):
        """Enhance risk analysis with real data"""
        if not transactions.exists():
            return {
                'document_id': str(risk_document.id),
                'document_date': risk_document.document_date,
                'document_version': risk_document.document_version,
                'processing_duration': risk_document.processing_duration,
                'status': risk_document.status,
                'error': 'No transaction data available for enhancement'
            }
        
        # Calculate real risk statistics
        total_transactions = transactions.count()
        avg_risk_score = transactions.aggregate(avg=Avg('overall_risk_score'))['avg'] or 0
        max_risk_score = transactions.aggregate(max=Max('overall_risk_score'))['max'] or 0
        min_risk_score = transactions.aggregate(min=Min('overall_risk_score'))['min'] or 0
        
        # Real risk distribution based on overall_risk_score
        real_risk_distribution = {
            'low_risk': transactions.filter(overall_risk_score__lt=30).count(),
            'medium_risk': transactions.filter(overall_risk_score__gte=30, overall_risk_score__lt=60).count(),
            'high_risk': transactions.filter(overall_risk_score__gte=60, overall_risk_score__lt=80).count(),
            'critical_risk': transactions.filter(overall_risk_score__gte=80).count()
        }
        
        # Enhanced methodology overview
        enhanced_methodology = {
            'description': 'Comprehensive risk scoring methodology based on actual transaction analysis',
            'version': risk_document.document_version,
            'analysis_date': risk_document.document_date.isoformat(),
            'total_transactions_analyzed': total_transactions,
            'risk_score_range': {
                'min': float(min_risk_score),
                'max': float(max_risk_score),
                'average': float(avg_risk_score)
            },
            'risk_levels': {
                'low_risk': {'min': 0, 'max': 29, 'description': 'Normal transactions'},
                'medium_risk': {'min': 30, 'max': 59, 'description': 'Some concerns'},
                'high_risk': {'min': 60, 'max': 79, 'description': 'Significant risk'},
                'critical_risk': {'min': 80, 'max': 100, 'description': 'High risk'}
            }
        }
        
        # Enhanced risk factors based on actual data
        enhanced_risk_factors = {
            'duplicate_risk': {
                'count': transactions.filter(is_duplicate=True).count(),
                'percentage': (transactions.filter(is_duplicate=True).count() / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with duplicate transactions'
            },
            'backdated_risk': {
                'count': transactions.filter(is_backdated=True).count(),
                'percentage': (transactions.filter(is_backdated=True).count() / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with backdated entries'
            },
            'high_value_risk': {
                'count': len([t for t in transactions if t.is_high_value]),
                'percentage': (len([t for t in transactions if t.is_high_value]) / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with high-value transactions'
            },
            'unusual_pattern_risk': {
                'count': transactions.filter(overall_risk_score__gte=60).count(),
                'percentage': (transactions.filter(overall_risk_score__gte=60).count() / total_transactions * 100) if total_transactions > 0 else 0,
                'description': 'Risk associated with unusual transaction patterns'
            }
        }
        
        # Add user-based risk factors if user analysis data is available
        # Note: This will be populated in the main method when user_analysis is available
        enhanced_risk_factors['user_behavior_risk'] = {
            'count': 0,  # Will be updated if user analysis is available
            'percentage': 0,  # Will be updated if user analysis is available
            'description': 'Risk associated with user behavior patterns and anomalies'
        }
        
        # Enhanced recommendations based on actual data
        enhanced_recommendations = []
        
        if real_risk_distribution['critical_risk'] > 0:
            enhanced_recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Immediate review of critical risk transactions',
                'description': f'{real_risk_distribution["critical_risk"]} critical risk transactions require immediate attention',
                'count': real_risk_distribution['critical_risk']
            })
        
        if real_risk_distribution['high_risk'] > 0:
            enhanced_recommendations.append({
                'priority': 'HIGH',
                'action': 'Review high-risk transactions',
                'description': f'{real_risk_distribution["high_risk"]} high-risk transactions need investigation',
                'count': real_risk_distribution['high_risk']
            })
        
        if enhanced_risk_factors['duplicate_risk']['count'] > 0:
            enhanced_recommendations.append({
                'priority': 'HIGH',
                'action': 'Investigate duplicate transactions',
                'description': f'{enhanced_risk_factors["duplicate_risk"]["count"]} duplicate transactions detected',
                'count': enhanced_risk_factors['duplicate_risk']['count']
            })
        
        if enhanced_risk_factors['backdated_risk']['count'] > 0:
            enhanced_recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Review backdated entries',
                'description': f'{enhanced_risk_factors["backdated_risk"]["count"]} backdated transactions found',
                'count': enhanced_risk_factors['backdated_risk']['count']
            })
        
        if enhanced_risk_factors['high_value_risk']['count'] > 0:
            enhanced_recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Review high-value transactions',
                'description': f'{enhanced_risk_factors["high_value_risk"]["count"]} high-value transactions identified',
                'count': enhanced_risk_factors['high_value_risk']['count']
            })
        
        # Enhanced audit implications
        enhanced_audit_implications = {
            'immediate_actions': [
                'Review all critical and high-risk transactions',
                'Investigate duplicate transactions',
                'Verify backdated entries',
                'Analyze high-value transaction patterns'
            ],
            'follow_up_actions': [
                'Implement controls to prevent future issues',
                'Regular monitoring of flagged patterns',
                'Staff training on proper posting procedures',
                'Establish risk-based audit procedures'
            ],
            'compliance_considerations': [
                'Ensure proper documentation for all flagged transactions',
                'Verify compliance with accounting standards',
                'Review internal control effectiveness',
                'Assess fraud risk indicators'
            ]
        }
        
        return {
            'document_id': str(risk_document.id),
            'document_date': risk_document.document_date,
            'document_version': risk_document.document_version,
            'processing_duration': risk_document.processing_duration,
            'status': risk_document.status,
            
            # Enhanced Summary Statistics
            'total_transactions': total_transactions,
            'high_risk_transactions': real_risk_distribution['high_risk'],
            'medium_risk_transactions': real_risk_distribution['medium_risk'],
            'low_risk_transactions': real_risk_distribution['low_risk'],
            'critical_risk_transactions': real_risk_distribution['critical_risk'],
            'overall_risk_score': float(avg_risk_score),
            
            # Enhanced Methodology and Factors
            'methodology_overview': enhanced_methodology,
            'risk_factors': enhanced_risk_factors,
            'scoring_criteria': risk_document.scoring_criteria,
            
            # Enhanced Risk Calculations and Distributions
            'risk_calculations': {
                'total_flagged': real_risk_distribution['high_risk'] + real_risk_distribution['critical_risk'],
                'overall_risk_score': float(avg_risk_score),
                'risk_level': self._get_risk_level(avg_risk_score),
                'risk_score_range': {
                    'min': float(min_risk_score),
                    'max': float(max_risk_score),
                    'average': float(avg_risk_score)
                }
            },
            'risk_distributions': real_risk_distribution,
            
            # Enhanced Recommendations and Audit Implications
            'recommendations': enhanced_recommendations,
            'audit_implications': enhanced_audit_implications
        } 
    
    def _generate_critical_alerts(self, transaction_summary, flag_summary, risk_assessment):
        """Generate critical alerts and warnings based on analysis results"""
        alerts = []
        
        try:
            # High value transaction alerts
            if transaction_summary.get('total_amount', 0) > 100000000:  # 100M SAR
                alerts.append({
                    'type': 'CRITICAL',
                    'category': 'HIGH_VALUE',
                    'message': f'Total transaction amount ({transaction_summary.get("total_amount", 0):,.2f} SAR) exceeds 100M SAR threshold',
                    'severity': 'HIGH',
                    'action_required': 'Immediate review of high-value transactions'
                })
            
            # High anomaly rate alerts
            anomaly_rate = flag_summary.get('anomaly_rate', 0)
            if anomaly_rate > 50:  # More than 50% anomalies
                alerts.append({
                    'type': 'WARNING',
                    'category': 'HIGH_ANOMALY_RATE',
                    'message': f'Anomaly rate ({anomaly_rate:.2f}%) is significantly high',
                    'severity': 'MEDIUM',
                    'action_required': 'Investigate anomaly patterns and root causes'
                })
            
            # Critical risk alerts
            critical_risk_count = flag_summary.get('critical_risk_anomalies', 0)
            if critical_risk_count > 0:
                alerts.append({
                    'type': 'CRITICAL',
                    'category': 'CRITICAL_RISK',
                    'message': f'{critical_risk_count} critical risk transactions detected',
                    'severity': 'CRITICAL',
                    'action_required': 'Immediate investigation of critical risk transactions'
                })
            
            # High risk score alerts
            overall_risk_score = risk_assessment.get('overall_risk_score', 0)
            if overall_risk_score > 70:
                alerts.append({
                    'type': 'WARNING',
                    'category': 'HIGH_RISK_SCORE',
                    'message': f'Overall risk score ({overall_risk_score:.2f}) indicates high risk',
                    'severity': 'HIGH',
                    'action_required': 'Review risk assessment methodology and investigate high-risk factors'
                })
            
            # Duplicate transaction alerts
            duplicate_count = flag_summary.get('duplicate_anomalies', 0)
            if duplicate_count > 10:
                alerts.append({
                    'type': 'WARNING',
                    'category': 'HIGH_DUPLICATE_COUNT',
                    'message': f'{duplicate_count} duplicate transactions detected',
                    'severity': 'MEDIUM',
                    'action_required': 'Investigate duplicate transaction patterns'
                })
            
            # Backdated transaction alerts
            backdated_count = flag_summary.get('backdated_anomalies', 0)
            if backdated_count > 5:
                alerts.append({
                    'type': 'WARNING',
                    'category': 'HIGH_BACKDATED_COUNT',
                    'message': f'{backdated_count} backdated transactions detected',
                    'severity': 'MEDIUM',
                    'action_required': 'Review backdated transaction policies and controls'
                })
            
            # No alerts if everything is normal
            if not alerts:
                alerts.append({
                    'type': 'INFO',
                    'category': 'NORMAL',
                    'message': 'No critical issues detected in analysis',
                    'severity': 'LOW',
                    'action_required': 'Continue monitoring'
                })
                
        except Exception as e:
            logger.error(f"Error generating critical alerts: {e}")
            alerts.append({
                'type': 'ERROR',
                'category': 'ALERT_GENERATION_ERROR',
                'message': f'Error generating alerts: {str(e)}',
                'severity': 'HIGH',
                'action_required': 'Review system logs and contact support'
            })
        
        return alerts 

    def _generate_all_gl_accounts_data(self, transactions):
        """Generate data for all GL accounts, not just top ones"""
        if not transactions.exists():
            return []
        
        # Get all GL accounts with their statistics
        gl_accounts_data = transactions.values('gl_account').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            debit_amount=Sum('amount_local_currency', filter=Q(transaction_type='DEBIT')),
            credit_amount=Sum('amount_local_currency', filter=Q(transaction_type='CREDIT')),
            avg_risk_score=Avg('overall_risk_score'),
            duplicate_count=Count('id', filter=Q(is_duplicate=True)),
            backdated_count=Count('id', filter=Q(is_backdated=True)),
            high_value_count=Count('id', filter=Q(amount_local_currency__gt=1000000))
        ).order_by('-total_amount')  # Order by total amount descending
        
        # Convert to list of dictionaries with enhanced information
        all_gl_accounts = []
        for account_data in gl_accounts_data:
            account_info = {
                'gl_account': account_data['gl_account'],
                'transaction_count': account_data['transaction_count'],
                'total_amount': float(account_data['total_amount'] or 0),
                'debit_amount': float(account_data['debit_amount'] or 0),
                'credit_amount': float(account_data['credit_amount'] or 0),
                'avg_risk_score': float(account_data['avg_risk_score'] or 0),
                'anomaly_counts': {
                    'duplicate': account_data['duplicate_count'],
                    'backdated': account_data['backdated_count'],
                    'high_value': account_data['high_value_count'],
                    'total_anomalies': account_data['duplicate_count'] + account_data['backdated_count'] + account_data['high_value_count']
                },
                'risk_level': self._get_risk_level(account_data['avg_risk_score'] or 0),
                'risk_color': self._get_risk_color(account_data['avg_risk_score'] or 0)
            }
            all_gl_accounts.append(account_info)
        
        return all_gl_accounts
    
    def _enhance_user_analysis(self, user_analysis, transactions):
        """Enhanced user analysis with key statistics only"""
        try:
            enhanced_analysis = {
                'analysis_id': str(user_analysis.id),
                'analysis_date': user_analysis.analysis_date,
                'processing_duration': user_analysis.processing_duration,
                'status': user_analysis.status,
                'analysis_version': user_analysis.analysis_version or '1.0.0',
                
                # User Summary Statistics
                'user_summary': {
                    'total_users': user_analysis.get_total_users(),
                    'total_transactions': user_analysis.get_total_transactions(),
                    'avg_transactions_per_user': user_analysis.get_total_transactions() / user_analysis.get_total_users() if user_analysis.get_total_users() > 0 else 0,
                    'unique_accounts_accessed': len(set(t.gl_account for t in transactions)),
                    'date_range': {
                        'earliest_date': min(t.posting_date for t in transactions).isoformat() if transactions else None,
                        'latest_date': max(t.posting_date for t in transactions).isoformat() if transactions else None
                    }
                },
                
                # User Anomalies Statistics
                'user_anomalies': {
                    'total_anomalies': user_analysis.get_anomalies_count(),
                    'anomaly_types_count': len(self._get_user_anomaly_type_summary(user_analysis.user_anomalies or [])),
                    'anomaly_severity_distribution': self._get_user_anomaly_severity_distribution(user_analysis.user_anomalies or []),
                    'top_anomalous_users_count': len(self._get_top_anomalous_users(user_analysis.user_anomalies or [], limit=5)),
                    'anomaly_trends_summary': {
                        'total_days': self._get_user_anomaly_trends(user_analysis.user_anomalies or []).get('total_days', 0),
                        'avg_anomalies_per_day': self._get_user_anomaly_trends(user_analysis.user_anomalies or []).get('avg_anomalies_per_day', 0)
                    }
                },
                
                # User Risk Assessment Statistics
                'user_risk_assessment': {
                    'high_risk_users_count': user_analysis.get_high_risk_users_count(),
                    'risk_distribution': self._get_user_risk_distribution(user_analysis.user_risk_assessment),
                    'risk_factors_count': len(self._extract_user_risk_factors(user_analysis.user_risk_assessment)),
                    'top_risk_users_count': len(self._get_top_risk_users(user_analysis.user_risk_assessment, limit=5))
                },
                
                # User Activity Pattern Statistics
                'user_patterns': {
                    'high_activity_users_count': len(self._analyze_user_transaction_patterns(user_analysis.user_transaction_summary or []).get('high_activity_users', [])),
                    'low_activity_users_count': len(self._analyze_user_transaction_patterns(user_analysis.user_transaction_summary or []).get('low_activity_users', [])),
                    'high_value_users_count': len(self._analyze_user_transaction_patterns(user_analysis.user_transaction_summary or []).get('high_value_users', [])),
                    'multi_account_users_count': len(self._analyze_user_transaction_patterns(user_analysis.user_transaction_summary or []).get('multi_account_users', [])),
                    'most_accessed_accounts_count': len(self._analyze_account_access_patterns(user_analysis.user_account_distribution or []).get('most_accessed_accounts', [])),
                    'exclusive_accounts_count': len(self._analyze_account_access_patterns(user_analysis.user_account_distribution or []).get('exclusive_accounts', [])),
                    'shared_accounts_count': len(self._analyze_account_access_patterns(user_analysis.user_account_distribution or []).get('shared_accounts', [])),
                    'weekend_activity_percentage': self._analyze_temporal_patterns(transactions).get('weekend_activity', {}).get('weekend_percentage', 0)
                }
            }
            
            return enhanced_analysis
            
        except Exception as e:
            logger.error(f"Error enhancing user analysis: {e}")
            return {
                'error': f'Error enhancing user analysis: {str(e)}',
                'status': 'ERROR'
            }
    
    def _get_user_anomaly_type_summary(self, user_anomalies):
        """Get summary of user anomaly types"""
        if not user_anomalies:
            return {}
        
        anomaly_types = {}
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                anomaly_type = anomaly.get('anomaly_type', 'unknown')
                if anomaly_type not in anomaly_types:
                    anomaly_types[anomaly_type] = {
                        'count': 0,
                        'users': set(),
                        'severity_levels': set()
                    }
                
                anomaly_types[anomaly_type]['count'] += 1
                anomaly_types[anomaly_type]['users'].add(anomaly.get('user_name', 'unknown'))
                anomaly_types[anomaly_type]['severity_levels'].add(anomaly.get('severity', 'medium'))
        
        # Convert sets to lists for JSON serialization
        for anomaly_type in anomaly_types:
            anomaly_types[anomaly_type]['users'] = list(anomaly_types[anomaly_type]['users'])
            anomaly_types[anomaly_type]['severity_levels'] = list(anomaly_types[anomaly_type]['severity_levels'])
        
        return anomaly_types
    
    def _get_user_anomaly_severity_distribution(self, user_anomalies):
        """Get distribution of user anomalies by severity"""
        if not user_anomalies:
            return {}
        
        severity_distribution = {
            'low': 0,
            'medium': 0,
            'high': 0,
            'critical': 0
        }
        
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                severity = anomaly.get('severity', 'medium').lower()
                if severity in severity_distribution:
                    severity_distribution[severity] += 1
        
        return severity_distribution
    
    def _get_top_anomalous_users(self, user_anomalies, limit=10):
        """Get top users with most anomalies"""
        if not user_anomalies:
            return []
        
        user_anomaly_counts = {}
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                user_name = anomaly.get('user_name', 'unknown')
                if user_name not in user_anomaly_counts:
                    user_anomaly_counts[user_name] = {
                        'user_name': user_name,
                        'anomaly_count': 0,
                        'anomaly_types': set(),
                        'severity_levels': set(),
                        'total_risk_score': 0
                    }
                
                user_anomaly_counts[user_name]['anomaly_count'] += 1
                user_anomaly_counts[user_name]['anomaly_types'].add(anomaly.get('anomaly_type', 'unknown'))
                user_anomaly_counts[user_name]['severity_levels'].add(anomaly.get('severity', 'medium'))
                user_anomaly_counts[user_name]['total_risk_score'] += anomaly.get('risk_score', 0)
        
        # Convert sets to lists and calculate average risk score
        for user_data in user_anomaly_counts.values():
            user_data['anomaly_types'] = list(user_data['anomaly_types'])
            user_data['severity_levels'] = list(user_data['severity_levels'])
            user_data['avg_risk_score'] = user_data['total_risk_score'] / user_data['anomaly_count'] if user_data['anomaly_count'] > 0 else 0
        
        # Sort by anomaly count and return top users
        top_users = sorted(user_anomaly_counts.values(), key=lambda x: x['anomaly_count'], reverse=True)
        return top_users[:limit]
    
    def _get_user_anomaly_trends(self, user_anomalies):
        """Get trends in user anomalies over time"""
        if not user_anomalies:
            return {}
        
        # Group anomalies by date
        date_anomalies = {}
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                date_str = anomaly.get('date', 'unknown')
                if date_str not in date_anomalies:
                    date_anomalies[date_str] = {
                        'date': date_str,
                        'anomaly_count': 0,
                        'unique_users': set(),
                        'anomaly_types': set()
                    }
                
                date_anomalies[date_str]['anomaly_count'] += 1
                date_anomalies[date_str]['unique_users'].add(anomaly.get('user_name', 'unknown'))
                date_anomalies[date_str]['anomaly_types'].add(anomaly.get('anomaly_type', 'unknown'))
        
        # Convert sets to lists
        for date_data in date_anomalies.values():
            date_data['unique_users'] = list(date_data['unique_users'])
            date_data['anomaly_types'] = list(date_data['anomaly_types'])
        
        # Sort by date
        sorted_dates = sorted(date_anomalies.values(), key=lambda x: x['date'])
        
        return {
            'daily_trends': sorted_dates,
            'total_days': len(sorted_dates),
            'avg_anomalies_per_day': sum(d['anomaly_count'] for d in sorted_dates) / len(sorted_dates) if sorted_dates else 0
        }
    
    def _get_user_risk_distribution(self, user_risk_assessment):
        """Get distribution of user risk levels"""
        if not user_risk_assessment:
            return {}
        
        risk_distribution = {
            'low': 0,
            'medium': 0,
            'high': 0,
            'critical': 0
        }
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user in user_risk_assessment:
                if isinstance(user, dict):
                    risk_level = user.get('risk_level', 'low').lower()
                    if risk_level in risk_distribution:
                        risk_distribution[risk_level] += 1
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            for user in user_risk_scores:
                if isinstance(user, dict):
                    risk_level = user.get('risk_level', 'low').lower()
                    if risk_level in risk_distribution:
                        risk_distribution[risk_level] += 1
        
        return risk_distribution
    
    def _extract_user_risk_factors(self, user_risk_assessment):
        """Extract key risk factors from user risk assessment"""
        if not user_risk_assessment:
            return []
        
        risk_factors = []
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user in user_risk_assessment:
                if isinstance(user, dict):
                    factors = user.get('risk_factors', [])
                    if isinstance(factors, list):
                        risk_factors.extend(factors)
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            for user in user_risk_scores:
                if isinstance(user, dict):
                    factors = user.get('risk_factors', [])
                    if isinstance(factors, list):
                        risk_factors.extend(factors)
        
        # Count frequency of each risk factor
        factor_counts = {}
        for factor in risk_factors:
            if isinstance(factor, dict):
                factor_name = factor.get('factor', 'unknown')
            else:
                factor_name = str(factor)
            
            if factor_name not in factor_counts:
                factor_counts[factor_name] = 0
            factor_counts[factor_name] += 1
        
        # Return top risk factors
        sorted_factors = sorted(factor_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'factor': factor, 'count': count} for factor, count in sorted_factors[:10]]
    
    def _get_top_risk_users(self, user_risk_assessment, limit=10):
        """Get top users with highest risk scores"""
        if not user_risk_assessment:
            return []
        
        user_risk_data = []
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user in user_risk_assessment:
                if isinstance(user, dict):
                    user_risk_data.append({
                        'user_name': user.get('user_name', 'unknown'),
                        'risk_score': user.get('risk_score', 0),
                        'risk_level': user.get('risk_level', 'low'),
                        'risk_factors': user.get('risk_factors', [])
                    })
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            for user in user_risk_scores:
                if isinstance(user, dict):
                    user_risk_data.append({
                        'user_name': user.get('user_name', 'unknown'),
                        'risk_score': user.get('risk_score', 0),
                        'risk_level': user.get('risk_level', 'low'),
                        'risk_factors': user.get('risk_factors', [])
                    })
        
        # Sort by risk score and return top users
        sorted_users = sorted(user_risk_data, key=lambda x: x['risk_score'], reverse=True)
        return sorted_users[:limit]
    
    def _analyze_user_transaction_patterns(self, user_transaction_summary):
        """Analyze patterns in user transaction behavior"""
        if not user_transaction_summary:
            return {}
        
        patterns = {
            'high_activity_users': [],
            'low_activity_users': [],
            'high_value_users': [],
            'multi_account_users': []
        }
        
        for user in user_transaction_summary:
            if isinstance(user, dict):
                user_name = user.get('user_name', 'unknown')
                transaction_count = user.get('transaction_count', 0)
                total_amount = user.get('total_amount', 0)
                account_count = user.get('account_count', 0)
                
                # High activity users (>10 transactions)
                if transaction_count > 10:
                    patterns['high_activity_users'].append({
                        'user_name': user_name,
                        'transaction_count': transaction_count,
                        'total_amount': total_amount
                    })
                
                # Low activity users (≤5 transactions)
                if transaction_count <= 5:
                    patterns['low_activity_users'].append({
                        'user_name': user_name,
                        'transaction_count': transaction_count,
                        'total_amount': total_amount
                    })
                
                # High value users (>1M total amount)
                if total_amount > 1000000:
                    patterns['high_value_users'].append({
                        'user_name': user_name,
                        'transaction_count': transaction_count,
                        'total_amount': total_amount
                    })
                
                # Multi-account users (>5 accounts)
                if account_count > 5:
                    patterns['multi_account_users'].append({
                        'user_name': user_name,
                        'account_count': account_count,
                        'transaction_count': transaction_count
                    })
        
        return patterns
    
    def _analyze_account_access_patterns(self, user_account_distribution):
        """Analyze patterns in user account access"""
        if not user_account_distribution:
            return {}
        
        patterns = {
            'most_accessed_accounts': [],
            'exclusive_accounts': [],
            'shared_accounts': []
        }
        
        for account_data in user_account_distribution:
            if isinstance(account_data, dict):
                account = account_data.get('account', 'unknown')
                user_count = account_data.get('user_count', 0)
                transaction_count = account_data.get('transaction_count', 0)
                
                account_info = {
                    'account': account,
                    'user_count': user_count,
                    'transaction_count': transaction_count
                }
                
                # Most accessed accounts (high transaction count)
                if transaction_count > 50:
                    patterns['most_accessed_accounts'].append(account_info)
                
                # Exclusive accounts (only 1 user)
                if user_count == 1:
                    patterns['exclusive_accounts'].append(account_info)
                
                # Shared accounts (multiple users)
                if user_count > 3:
                    patterns['shared_accounts'].append(account_info)
        
        # Sort by relevant metrics
        patterns['most_accessed_accounts'].sort(key=lambda x: x['transaction_count'], reverse=True)
        patterns['exclusive_accounts'].sort(key=lambda x: x['transaction_count'], reverse=True)
        patterns['shared_accounts'].sort(key=lambda x: x['user_count'], reverse=True)
        
        return patterns
    
    def _analyze_temporal_patterns(self, transactions):
        """Analyze temporal patterns in user activity"""
        if not transactions:
            return {}
        
        # Group transactions by user and date
        user_daily_activity = {}
        for transaction in transactions:
            user_name = transaction.user_name
            date_str = transaction.posting_date.isoformat()
            
            if user_name not in user_daily_activity:
                user_daily_activity[user_name] = {}
            
            if date_str not in user_daily_activity[user_name]:
                user_daily_activity[user_name][date_str] = {
                    'transaction_count': 0,
                    'total_amount': 0,
                    'accounts_accessed': set()
                }
            
            user_daily_activity[user_name][date_str]['transaction_count'] += 1
            user_daily_activity[user_name][date_str]['total_amount'] += float(transaction.amount_local_currency)
            user_daily_activity[user_name][date_str]['accounts_accessed'].add(transaction.gl_account)
        
        # Convert sets to lists
        for user_data in user_daily_activity.values():
            for date_data in user_data.values():
                date_data['accounts_accessed'] = list(date_data['accounts_accessed'])
        
        patterns = {
            'user_daily_activity': user_daily_activity,
            'peak_activity_days': self._find_peak_activity_days(transactions),
            'weekend_activity': self._analyze_weekend_activity(transactions),
            'monthly_trends': self._analyze_monthly_trends(transactions)
        }
        
        return patterns
    
    def _find_peak_activity_days(self, transactions):
        """Find days with peak transaction activity"""
        daily_counts = {}
        for transaction in transactions:
            date_str = transaction.posting_date.isoformat()
            daily_counts[date_str] = daily_counts.get(date_str, 0) + 1
        
        # Find top 5 busiest days
        sorted_days = sorted(daily_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'date': date, 'transaction_count': count} for date, count in sorted_days[:5]]
    
    def _analyze_weekend_activity(self, transactions):
        """Analyze weekend vs weekday activity"""
        weekday_count = 0
        weekend_count = 0
        
        for transaction in transactions:
            if transaction.posting_date.weekday() < 5:  # Monday = 0, Friday = 4
                weekday_count += 1
            else:
                weekend_count += 1
        
        return {
            'weekday_transactions': weekday_count,
            'weekend_transactions': weekend_count,
            'weekend_percentage': (weekend_count / (weekday_count + weekend_count) * 100) if (weekday_count + weekend_count) > 0 else 0
        }
    
    def _analyze_monthly_trends(self, transactions):
        """Analyze monthly transaction trends"""
        monthly_counts = {}
        for transaction in transactions:
            month_key = f"{transaction.posting_date.year}-{transaction.posting_date.month:02d}"
            monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1
        
        # Sort by month
        sorted_months = sorted(monthly_counts.items())
        return [{'month': month, 'transaction_count': count} for month, count in sorted_months]
    
    def _prepare_user_anomaly_export(self, user_anomalies):
        """Prepare user anomalies data for export"""
        if not user_anomalies:
            return []
        
        export_data = []
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                export_data.append({
                    'user_name': anomaly.get('user_name', ''),
                    'anomaly_type': anomaly.get('anomaly_type', ''),
                    'severity': anomaly.get('severity', ''),
                    'risk_score': anomaly.get('risk_score', 0),
                    'description': anomaly.get('description', ''),
                    'date': anomaly.get('date', ''),
                    'transaction_count': anomaly.get('transaction_count', 0),
                    'total_amount': anomaly.get('total_amount', 0)
                })
        
        return export_data
    
    def _prepare_user_risk_export(self, user_risk_assessment):
        """Prepare user risk assessment data for export"""
        if not user_risk_assessment:
            return []
        
        export_data = []
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user in user_risk_assessment:
                if isinstance(user, dict):
                    export_data.append({
                        'user_name': user.get('user_name', ''),
                        'risk_score': user.get('risk_score', 0),
                        'risk_level': user.get('risk_level', ''),
                        'risk_factors': user.get('risk_factors', []),
                        'transaction_count': user.get('transaction_count', 0),
                        'total_amount': user.get('total_amount', 0)
                    })
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            for user in user_risk_scores:
                if isinstance(user, dict):
                    export_data.append({
                        'user_name': user.get('user_name', ''),
                        'risk_score': user.get('risk_score', 0),
                        'risk_level': user.get('risk_level', ''),
                        'risk_factors': user.get('risk_factors', []),
                        'transaction_count': user.get('transaction_count', 0),
                        'total_amount': user.get('total_amount', 0)
                    })
        
        return export_data
    
    def _prepare_user_summary_export(self, user_transaction_summary):
        """Prepare user transaction summary data for export"""
        if not user_transaction_summary:
            return []
        
        export_data = []
        for user in user_transaction_summary:
            if isinstance(user, dict):
                export_data.append({
                    'user_name': user.get('user_name', ''),
                    'transaction_count': user.get('transaction_count', 0),
                    'total_amount': user.get('total_amount', 0),
                    'account_count': user.get('account_count', 0),
                    'avg_amount': user.get('avg_amount', 0),
                    'min_amount': user.get('min_amount', 0),
                    'max_amount': user.get('max_amount', 0),
                    'first_transaction_date': user.get('first_transaction_date', ''),
                    'last_transaction_date': user.get('last_transaction_date', '')
                })
        
        return export_data

class SAPGLPostingPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 500

class SAPGLPostingListView(generics.ListAPIView):
    """API view for listing SAPGLPosting records by file ID with advanced filters and pagination.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - date_from, date_to: Posting date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - gl_account: Comma-separated list (e.g., gl_account=1001,1002)
    
    User Filters:
    - user_name: Comma-separated list or fuzzy search (e.g., user_name=ali,ahmed or user_name=ali)
    - user_activity: high_activity (users with >10 transactions) or low_activity (users with ≤5 transactions)
    - user_risk: high_risk (users with avg risk >50) or low_risk (users with avg risk ≤20)
    
    Search Filters:
    - document_number: Fuzzy search
    - text_search: Fuzzy search in text field
    - search: Fuzzy search across user, document, text
    
    Type Filters:
    - transaction_type, document_type: Filter by type
    
    Risk Filters:
    - min_risk_score, max_risk_score: Risk score range
    - is_duplicate, is_backdated, is_high_value: Boolean anomaly flags
    - has_anomaly: true/false (any anomaly)
    
    Ordering:
    - ordering: Field to order by (e.g., -amount_local_currency, user_name, -posting_date)
    """
    serializer_class = SAPGLPostingListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        file_id = self.kwargs.get('file_id')
        try:
            data_file = DataFile.objects.get(id=file_id)
            queryset = SAPGLPosting.objects.filter(data_file=data_file).select_related('gl_account_ref')
            queryset = self._apply_filters(queryset)
            return queryset
        except DataFile.DoesNotExist:
            raise serializers.ValidationError(f"Data file with ID {file_id} not found")
    
    def _apply_filters(self, queryset):
        params = self.request.query_params
        # Date range
        date_from = params.get('date_from')
        date_to = params.get('date_to')
        if date_from:
            queryset = queryset.filter(posting_date__gte=date_from)
        if date_to:
            queryset = queryset.filter(posting_date__lte=date_to)
        # Amount range
        amount_range = params.get('amount_range')
        if amount_range:
            try:
                min_amt, max_amt = [float(x) for x in amount_range.split(',') if x.strip()]
                queryset = queryset.filter(amount_local_currency__gte=min_amt, amount_local_currency__lte=max_amt)
            except Exception:
                pass
        # Multi-value GL account
        gl_accounts = params.get('gl_account')
        if gl_accounts:
            gl_list = [x.strip() for x in gl_accounts.split(',') if x.strip()]
            queryset = queryset.filter(gl_account__in=gl_list)
        # Multi-value user_name or fuzzy
        user_names = params.get('user_name')
        if user_names:
            if ',' in user_names:
                user_list = [x.strip() for x in user_names.split(',') if x.strip()]
                queryset = queryset.filter(user_name__in=user_list)
            else:
                queryset = queryset.filter(user_name__icontains=user_names)
        
        # User activity filters
        user_activity = params.get('user_activity')
        if user_activity:
            if user_activity == 'high_activity':
                # Users with more than 10 transactions
                from django.db.models import Count
                high_activity_users = queryset.values('user_name').annotate(
                    transaction_count=Count('id')
                ).filter(transaction_count__gt=10).values_list('user_name', flat=True)
                queryset = queryset.filter(user_name__in=high_activity_users)
            elif user_activity == 'low_activity':
                # Users with 5 or fewer transactions
                from django.db.models import Count
                low_activity_users = queryset.values('user_name').annotate(
                    transaction_count=Count('id')
                ).filter(transaction_count__lte=5).values_list('user_name', flat=True)
                queryset = queryset.filter(user_name__in=low_activity_users)
        
        # User risk filters
        user_risk = params.get('user_risk')
        if user_risk:
            if user_risk == 'high_risk':
                # Users with high average risk score
                from django.db.models import Avg
                high_risk_users = queryset.values('user_name').annotate(
                    avg_risk=Avg('overall_risk_score')
                ).filter(avg_risk__gt=50).values_list('user_name', flat=True)
                queryset = queryset.filter(user_name__in=high_risk_users)
            elif user_risk == 'low_risk':
                # Users with low average risk score
                from django.db.models import Avg
                low_risk_users = queryset.values('user_name').annotate(
                    avg_risk=Avg('overall_risk_score')
                ).filter(avg_risk__lte=20).values_list('user_name', flat=True)
                queryset = queryset.filter(user_name__in=low_risk_users)
        # Fuzzy document number
        doc_num = params.get('document_number')
        if doc_num:
            queryset = queryset.filter(document_number__icontains=doc_num)
        # Fuzzy text search
        text_search = params.get('text_search')
        if text_search:
            queryset = queryset.filter(text__icontains=text_search)
        # Transaction/document type
        transaction_type = params.get('transaction_type')
        if transaction_type:
            queryset = queryset.filter(transaction_type=transaction_type)
        document_type = params.get('document_type')
        if document_type:
            queryset = queryset.filter(document_type=document_type)
        # Risk score range
        min_risk_score = params.get('min_risk_score')
        max_risk_score = params.get('max_risk_score')
        if min_risk_score:
            queryset = queryset.filter(overall_risk_score__gte=float(min_risk_score))
        if max_risk_score:
            queryset = queryset.filter(overall_risk_score__lte=float(max_risk_score))
        # Boolean anomaly flags
        for flag in ['is_duplicate', 'is_backdated', 'is_high_value']:
            val = params.get(flag)
            if val is not None:
                if flag == 'is_high_value':
                    if val.lower() == 'true':
                        queryset = queryset.filter(amount_local_currency__gt=1000000)
                    else:
                        queryset = queryset.filter(amount_local_currency__lte=1000000)
                else:
                    queryset = queryset.filter(**{flag: val.lower() == 'true'})
        # Has anomaly (any anomaly)
        has_anomaly = params.get('has_anomaly')
        if has_anomaly is not None:
            if has_anomaly.lower() == 'true':
                queryset = queryset.filter(Q(is_duplicate=True) | Q(is_backdated=True) | Q(amount_local_currency__gt=1000000))
            else:
                queryset = queryset.exclude(Q(is_duplicate=True) | Q(is_backdated=True) | Q(amount_local_currency__gt=1000000))
        # Fuzzy search across user, document, text
        search = params.get('search')
        if search:
            queryset = queryset.filter(
                Q(user_name__icontains=search) |
                Q(document_number__icontains=search) |
                Q(text__icontains=search)
            )
        # Ordering
        ordering = params.get('ordering', '-posting_date')
        if ordering:
            queryset = queryset.order_by(ordering)
        return queryset
    
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        summary_stats = self._get_summary_stats(queryset)
        serializer = self.get_serializer(page, many=True) if page is not None else self.get_serializer(queryset, many=True)
        response_data = {
            'file_id': self.kwargs.get('file_id'),
            'summary': summary_stats,
            'transactions': serializer.data,
            'total_count': queryset.count(),
            'filters_applied': self._get_applied_filters(),
        }
        if page is not None:
            return self.get_paginated_response(response_data)
        return Response(response_data)
    
    def _get_summary_stats(self, queryset):
        """Get summary statistics for the filtered transactions"""
        from django.db.models import Sum, Count, Avg, Min, Max
        
        stats = queryset.aggregate(
            total_transactions=Count('id'),
            total_amount=Sum('amount_local_currency'),
            avg_amount=Avg('amount_local_currency'),
            min_amount=Min('amount_local_currency'),
            max_amount=Max('amount_local_currency'),
            avg_risk_score=Avg('overall_risk_score'),
            duplicate_count=Count('id', filter=Q(is_duplicate=True)),
            backdated_count=Count('id', filter=Q(is_backdated=True)),
            high_value_count=Count('id', filter=Q(amount_local_currency__gt=1000000))
        )
        
        # Add additional calculated fields
        stats['debit_count'] = queryset.filter(transaction_type='DEBIT').count()
        stats['credit_count'] = queryset.filter(transaction_type='CREDIT').count()
        stats['unique_users'] = queryset.values('user_name').distinct().count()
        stats['unique_accounts'] = queryset.values('gl_account').distinct().count()
        
        return stats
    
    def _get_applied_filters(self):
        """Get list of filters that were applied"""
        filters = []
        params = self.request.query_params
        
        # Date filters
        if params.get('date_from'):
            filters.append(f"Date from: {params.get('date_from')}")
        if params.get('date_to'):
            filters.append(f"Date to: {params.get('date_to')}")
        
        # Amount filters
        if params.get('amount_range'):
            filters.append(f"Amount range: {params.get('amount_range')}")
        if params.get('min_amount'):
            filters.append(f"Min amount: {params.get('min_amount')}")
        if params.get('max_amount'):
            filters.append(f"Max amount: {params.get('max_amount')}")
        
        # GL Account filters
        if params.get('gl_account'):
            filters.append(f"GL Account: {params.get('gl_account')}")
        
        # User filters
        if params.get('user_name'):
            filters.append(f"User: {params.get('user_name')}")
        if params.get('user_activity'):
            filters.append(f"User activity: {params.get('user_activity')}")
        if params.get('user_risk'):
            filters.append(f"User risk: {params.get('user_risk')}")
        
        # Search filters
        if params.get('document_number'):
            filters.append(f"Document number: {params.get('document_number')}")
        if params.get('text_search'):
            filters.append(f"Text search: {params.get('text_search')}")
        if params.get('search'):
            filters.append(f"General search: {params.get('search')}")
        
        # Type filters
        if params.get('transaction_type'):
            filters.append(f"Transaction type: {params.get('transaction_type')}")
        if params.get('document_type'):
            filters.append(f"Document type: {params.get('document_type')}")
        
        # Risk filters
        if params.get('min_risk_score'):
            filters.append(f"Min risk score: {params.get('min_risk_score')}")
        if params.get('max_risk_score'):
            filters.append(f"Max risk score: {params.get('max_risk_score')}")
        
        # Anomaly filters
        if params.get('is_duplicate'):
            filters.append(f"Duplicates only: {params.get('is_duplicate')}")
        if params.get('is_backdated'):
            filters.append(f"Backdated only: {params.get('is_backdated')}")
        if params.get('is_high_value'):
            filters.append(f"High value only: {params.get('is_high_value')}")
        if params.get('has_anomaly'):
            filters.append(f"Has anomaly: {params.get('has_anomaly')}")
        
        # Ordering
        if params.get('ordering'):
            filters.append(f"Ordering: {params.get('ordering')}")
        
        return filters 

class FilterDropdownDataView(generics.GenericAPIView):
    """API view for getting dropdown data for SAPGLPosting filters by file ID.
    
    Returns:
    - users: List of unique users with transaction counts
    - gl_accounts: List of unique GL accounts with transaction counts and amounts
    - amount_ranges: Predefined amount range options
    - transaction_types: List of unique transaction types
    - document_types: List of unique document types
    - risk_levels: Predefined risk level options
    - anomaly_types: Predefined anomaly type options
    """
    
    def get(self, request, file_id):
        """Get dropdown data for all filters"""
        try:
            # Verify the file exists
            data_file = DataFile.objects.get(id=file_id)
            
            # Get base queryset for this file
            base_queryset = SAPGLPosting.objects.filter(data_file=data_file)
            
            # Get unique users with transaction counts
            users_data = base_queryset.values('user_name').annotate(
                transaction_count=Count('id'),
                total_amount=Sum('amount_local_currency'),
                avg_risk_score=Avg('overall_risk_score')
            ).order_by('user_name')
            
            users = [
                {
                    'value': user['user_name'],
                    'label': user['user_name'],
                    'transaction_count': user['transaction_count'],
                    'total_amount': float(user['total_amount'] or 0),
                    'avg_risk_score': float(user['avg_risk_score'] or 0)
                }
                for user in users_data if user['user_name']
            ]
            
            # Get unique GL accounts with transaction counts and amounts
            gl_accounts_data = base_queryset.values('gl_account').annotate(
                transaction_count=Count('id'),
                total_amount=Sum('amount_local_currency'),
                avg_risk_score=Avg('overall_risk_score')
            ).order_by('gl_account')
            
            gl_accounts = [
                {
                    'value': account['gl_account'],
                    'label': f"{account['gl_account']} ({account['transaction_count']} transactions)",
                    'transaction_count': account['transaction_count'],
                    'total_amount': float(account['total_amount'] or 0),
                    'avg_risk_score': float(account['avg_risk_score'] or 0)
                }
                for account in gl_accounts_data if account['gl_account']
            ]
            
            # Calculate amount ranges based on actual data
            amount_stats = base_queryset.aggregate(
                min_amount=Min('amount_local_currency'),
                max_amount=Max('amount_local_currency'),
                avg_amount=Avg('amount_local_currency')
            )
            
            min_amount = float(amount_stats['min_amount'] or 0)
            max_amount = float(amount_stats['max_amount'] or 0)
            avg_amount = float(amount_stats['avg_amount'] or 0)
            
            # Generate amount ranges based on data distribution
            amount_ranges = [
                {
                    'value': f"0,{min_amount * 0.1}",
                    'label': f"0 - {min_amount * 0.1:,.0f} SAR"
                },
                {
                    'value': f"{min_amount * 0.1},{min_amount}",
                    'label': f"{min_amount * 0.1:,.0f} - {min_amount:,.0f} SAR"
                },
                {
                    'value': f"{min_amount},{avg_amount}",
                    'label': f"{min_amount:,.0f} - {avg_amount:,.0f} SAR"
                },
                {
                    'value': f"{avg_amount},{max_amount * 0.5}",
                    'label': f"{avg_amount:,.0f} - {max_amount * 0.5:,.0f} SAR"
                },
                {
                    'value': f"{max_amount * 0.5},{max_amount}",
                    'label': f"{max_amount * 0.5:,.0f} - {max_amount:,.0f} SAR"
                },
                {
                    'value': f"{max_amount},999999999",
                    'label': f"{max_amount:,.0f}+ SAR"
                }
            ]
            
            # Get unique transaction types
            transaction_types_data = base_queryset.values('transaction_type').annotate(
                count=Count('id')
            ).filter(transaction_type__isnull=False).exclude(transaction_type='').order_by('transaction_type')
            
            transaction_types = [
                {
                    'value': tt['transaction_type'],
                    'label': f"{tt['transaction_type']} ({tt['count']})",
                    'count': tt['count']
                }
                for tt in transaction_types_data
            ]
            
            # Get unique document types
            document_types_data = base_queryset.values('document_type').annotate(
                count=Count('id')
            ).filter(document_type__isnull=False).exclude(document_type='').order_by('document_type')
            
            document_types = [
                {
                    'value': dt['document_type'],
                    'label': f"{dt['document_type']} ({dt['count']})",
                    'count': dt['count']
                }
                for dt in document_types_data
            ]
            
            # Predefined risk levels
            risk_levels = [
                {
                    'value': 'low',
                    'label': 'Low Risk (0-29)',
                    'min': 0,
                    'max': 29,
                    'color': '#4BC0C0'
                },
                {
                    'value': 'medium',
                    'label': 'Medium Risk (30-59)',
                    'min': 30,
                    'max': 59,
                    'color': '#FFCE56'
                },
                {
                    'value': 'high',
                    'label': 'High Risk (60-79)',
                    'min': 60,
                    'max': 79,
                    'color': '#FF9F40'
                },
                {
                    'value': 'critical',
                    'label': 'Critical Risk (80-100)',
                    'min': 80,
                    'max': 100,
                    'color': '#FF6384'
                }
            ]
            
            # Predefined anomaly types
            anomaly_types = [
                {
                    'value': 'is_duplicate',
                    'label': 'Duplicate Transactions',
                    'count': base_queryset.filter(is_duplicate=True).count(),
                    'color': '#FF6384'
                },
                {
                    'value': 'is_backdated',
                    'label': 'Backdated Entries',
                    'count': base_queryset.filter(is_backdated=True).count(),
                    'color': '#36A2EB'
                },
                {
                    'value': 'is_high_value',
                    'label': 'High Value Transactions',
                    'count': base_queryset.filter(amount_local_currency__gt=1000000).count(),
                    'color': '#FFCE56'
                },
                {
                    'value': 'has_anomaly',
                    'label': 'Any Anomaly',
                    'count': base_queryset.filter(
                        Q(is_duplicate=True) | Q(is_backdated=True) | Q(amount_local_currency__gt=1000000)
                    ).count(),
                    'color': '#9966FF'
                }
            ]
            
            # User activity levels
            user_activity_levels = [
                {
                    'value': 'high_activity',
                    'label': 'High Activity (>10 transactions)',
                    'threshold': 10,
                    'count': base_queryset.values('user_name').annotate(
                        transaction_count=Count('id')
                    ).filter(transaction_count__gt=10).count()
                },
                {
                    'value': 'low_activity',
                    'label': 'Low Activity (≤5 transactions)',
                    'threshold': 5,
                    'count': base_queryset.values('user_name').annotate(
                        transaction_count=Count('id')
                    ).filter(transaction_count__lte=5).count()
                }
            ]
            
            # User risk levels
            user_risk_levels = [
                {
                    'value': 'high_risk',
                    'label': 'High Risk Users (avg risk >50)',
                    'threshold': 50,
                    'count': base_queryset.values('user_name').annotate(
                        avg_risk=Avg('overall_risk_score')
                    ).filter(avg_risk__gt=50).count()
                },
                {
                    'value': 'low_risk',
                    'label': 'Low Risk Users (avg risk ≤20)',
                    'threshold': 20,
                    'count': base_queryset.values('user_name').annotate(
                        avg_risk=Avg('overall_risk_score')
                    ).filter(avg_risk__lte=20).count()
                }
            ]
            
            # Date range suggestions
            date_range = base_queryset.aggregate(
                min_date=Min('posting_date'),
                max_date=Max('posting_date')
            )
            
            date_ranges = [
                {
                    'value': 'last_7_days',
                    'label': 'Last 7 Days',
                    'description': 'Transactions from the last 7 days'
                },
                {
                    'value': 'last_30_days',
                    'label': 'Last 30 Days',
                    'description': 'Transactions from the last 30 days'
                },
                {
                    'value': 'last_90_days',
                    'label': 'Last 90 Days',
                    'description': 'Transactions from the last 90 days'
                },
                {
                    'value': 'this_month',
                    'label': 'This Month',
                    'description': 'Transactions from the current month'
                },
                {
                    'value': 'this_year',
                    'label': 'This Year',
                    'description': 'Transactions from the current year'
                }
            ]
            
            # Ordering options
            ordering_options = [
                {
                    'value': '-posting_date',
                    'label': 'Date (Newest First)'
                },
                {
                    'value': 'posting_date',
                    'label': 'Date (Oldest First)'
                },
                {
                    'value': '-amount_local_currency',
                    'label': 'Amount (Highest First)'
                },
                {
                    'value': 'amount_local_currency',
                    'label': 'Amount (Lowest First)'
                },
                {
                    'value': 'user_name',
                    'label': 'User Name (A-Z)'
                },
                {
                    'value': '-user_name',
                    'label': 'User Name (Z-A)'
                },
                {
                    'value': '-overall_risk_score',
                    'label': 'Risk Score (Highest First)'
                },
                {
                    'value': 'overall_risk_score',
                    'label': 'Risk Score (Lowest First)'
                },
                {
                    'value': 'gl_account',
                    'label': 'GL Account (A-Z)'
                },
                {
                    'value': '-gl_account',
                    'label': 'GL Account (Z-A)'
                }
            ]
            
            response_data = {
                'file_id': str(file_id),
                'file_name': data_file.file_name,
                'total_transactions': base_queryset.count(),
                'data_range': {
                    'min_date': date_range['min_date'],
                    'max_date': date_range['max_date'],
                    'min_amount': min_amount,
                    'max_amount': max_amount,
                    'avg_amount': avg_amount
                },
                'dropdowns': {
                    'users': users,
                    'gl_accounts': gl_accounts,
                    'amount_ranges': amount_ranges,
                    'transaction_types': transaction_types,
                    'document_types': document_types,
                    'risk_levels': risk_levels,
                    'anomaly_types': anomaly_types,
                    'user_activity_levels': user_activity_levels,
                    'user_risk_levels': user_risk_levels,
                    'date_ranges': date_ranges,
                    'ordering_options': ordering_options
                }
            }
            
            return Response(response_data)
            
        except DataFile.DoesNotExist:
            raise serializers.ValidationError(f"Data file with ID {file_id} not found")

class DuplicateAnalysisView(generics.GenericAPIView):
    """API view for retrieving duplicate analysis results by file ID.
    
    This view provides comprehensive duplicate analysis results including:
    - Summary statistics
    - Detailed duplicate entries
    - Chart data for visualizations
    - Export-ready data
    - Risk assessment
    """
    
    def get(self, request, file_id):
        """Get duplicate analysis results for a specific file"""
        try:
            # Get the latest duplicate analysis result
            from .models import DuplicateAnalysisResult
            duplicate_analysis = DuplicateAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not duplicate_analysis:
                return Response({
                    'error': 'No duplicate analysis results found for this file',
                    'error_code': 'NO_DUPLICATE_ANALYSIS_FOUND',
                    'suggestions': [
                        'Run duplicate analysis for this file',
                        'Check if duplicate analysis processing completed successfully'
                    ]
                }, status=404)
            
            # Prepare response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(duplicate_analysis.id),
                    'analysis_date': duplicate_analysis.analysis_date,
                    'processing_duration': duplicate_analysis.processing_duration,
                    'status': duplicate_analysis.status,
                    'analysis_version': duplicate_analysis.analysis_version or '1.0.0'
                },
                'summary': {
                    'total_duplicates': len(duplicate_analysis.duplicate_list or []),
                    'total_amount': duplicate_analysis.get_total_amount(),
                    'risk_distribution': duplicate_analysis.get_risk_distribution(),
                    'compliance_issues': duplicate_analysis.get_compliance_issues(),
                    'high_priority_recommendations': duplicate_analysis.get_high_priority_recommendations()
                },
                'detailed_results': {
                    'duplicate_entries': duplicate_analysis.duplicate_list or [],
                    'duplicate_patterns': duplicate_analysis.breakdowns or {},
                    'audit_recommendations': duplicate_analysis.breakdowns.get('audit_recommendations', {}) if duplicate_analysis.breakdowns else {},
                    'detection_methods': ['business_rules', 'comprehensive_analysis', 'risk_scoring'],
                    'confidence_scores': self._generate_confidence_scores(duplicate_analysis),
                    'false_positive_indicators': self._generate_false_positive_indicators(duplicate_analysis)
                },
                'visualizations': {
                    'chart_data': duplicate_analysis.chart_data or {},
                    'slicer_filters': {
                        'risk_levels': ['low', 'medium', 'high', 'critical'],
                        'duplicate_types': ['type_1', 'type_2', 'type_3', 'type_4', 'type_5', 'type_6'],
                        'amount_ranges': ['0-1000', '1000-10000', '10000-100000', '100000+'],
                        'users': self._extract_unique_users(duplicate_analysis.duplicate_list or []),
                        'accounts': self._extract_unique_accounts(duplicate_analysis.duplicate_list or [])
                    }
                },
                'export_data': {
                    'summary_table': self._generate_summary_table(duplicate_analysis),
                    'detailed_export': duplicate_analysis.export_data or []
                }
            }
            
            # Add compliance assessment if available
            if duplicate_analysis.compliance_assessment:
                response_data['compliance'] = duplicate_analysis.compliance_assessment
            
            # Add financial statement impact if available
            if duplicate_analysis.financial_statement_impact:
                response_data['financial_impact'] = duplicate_analysis.financial_statement_impact
            
            # Add detailed insights if available
            if duplicate_analysis.detailed_insights:
                response_data['insights'] = duplicate_analysis.detailed_insights
            
            # Log successful retrieval
            duplicate_count = len(duplicate_analysis.duplicate_list or [])
            logger.info(f"Successfully retrieved duplicate analysis for file {file_id} with {duplicate_count} duplicates")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving duplicate analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving duplicate analysis',
                'error_code': 'DUPLICATE_ANALYSIS_ERROR',
                'details': str(e)
            }, status=500)
    
    def _generate_confidence_scores(self, duplicate_analysis):
        """Generate confidence scores for duplicate detection"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        confidence_scores = {}
        for duplicate in duplicate_analysis.duplicate_list:
            if isinstance(duplicate, dict):
                duplicate_type = duplicate.get('duplicate_type', 'unknown')
                similarity_score = duplicate.get('similarity_score', 0.0)
                risk_score = duplicate.get('risk_score', 0)
                
                if duplicate_type not in confidence_scores:
                    confidence_scores[duplicate_type] = {
                        'avg_similarity': 0.0,
                        'avg_risk_score': 0.0,
                        'count': 0
                    }
                
                confidence_scores[duplicate_type]['avg_similarity'] += similarity_score
                confidence_scores[duplicate_type]['avg_risk_score'] += risk_score
                confidence_scores[duplicate_type]['count'] += 1
        
        # Calculate averages
        for dup_type in confidence_scores:
            count = confidence_scores[dup_type]['count']
            if count > 0:
                confidence_scores[dup_type]['avg_similarity'] /= count
                confidence_scores[dup_type]['avg_risk_score'] /= count
        
        return confidence_scores
    
    def _generate_false_positive_indicators(self, duplicate_analysis):
        """Generate false positive indicators"""
        if not duplicate_analysis.duplicate_list:
            return []
        
        false_positive_indicators = []
        
        for duplicate in duplicate_analysis.duplicate_list:
            if isinstance(duplicate, dict):
                similarity_score = duplicate.get('similarity_score', 0.0)
                risk_score = duplicate.get('risk_score', 0)
                
                # Low similarity but high risk might indicate false positive
                if similarity_score < 0.5 and risk_score > 70:
                    false_positive_indicators.append({
                        'duplicate_id': duplicate.get('transaction1', {}).get('id', 'unknown'),
                        'indicator': 'low_similarity_high_risk',
                        'similarity_score': similarity_score,
                        'risk_score': risk_score,
                        'description': 'Low similarity score but high risk score - potential false positive'
                    })
                
                # Very high similarity but low risk might indicate legitimate duplicates
                if similarity_score > 0.9 and risk_score < 30:
                    false_positive_indicators.append({
                        'duplicate_id': duplicate.get('transaction1', {}).get('id', 'unknown'),
                        'indicator': 'high_similarity_low_risk',
                        'similarity_score': similarity_score,
                        'risk_score': risk_score,
                        'description': 'High similarity score but low risk score - likely legitimate duplicate'
                    })
        
        return false_positive_indicators
    
    def _generate_summary_table(self, duplicate_analysis):
        """Generate summary table data"""
        if not duplicate_analysis.duplicate_list:
            return []
        
        summary_table = []
        
        for duplicate in duplicate_analysis.duplicate_list:
            if isinstance(duplicate, dict):
                transaction1 = duplicate.get('transaction1', {})
                transaction2 = duplicate.get('transaction2', {})
                
                summary_table.append({
                    'duplicate_id': f"{transaction1.get('id', '')}_{transaction2.get('id', '')}",
                    'duplicate_type': duplicate.get('duplicate_type', 'unknown'),
                    'risk_score': duplicate.get('risk_score', 0),
                    'similarity_score': duplicate.get('similarity_score', 0.0),
                    'amount': transaction1.get('amount', 0),
                    'user': transaction1.get('user', ''),
                    'account': transaction1.get('account', ''),
                    'date': transaction1.get('date', ''),
                    'matching_fields': len(duplicate.get('matching_fields', [])),
                    'status': 'high_risk' if duplicate.get('risk_score', 0) > 70 else 'medium_risk' if duplicate.get('risk_score', 0) > 40 else 'low_risk'
                })
        
        return summary_table
    
    def _extract_unique_users(self, duplicate_list):
        """Extract unique users from duplicate list"""
        users = set()
        for duplicate in duplicate_list:
            if isinstance(duplicate, dict):
                transaction1 = duplicate.get('transaction1', {})
                transaction2 = duplicate.get('transaction2', {})
                if transaction1.get('user'):
                    users.add(transaction1.get('user'))
                if transaction2.get('user'):
                    users.add(transaction2.get('user'))
        return list(users)
    
    def _extract_unique_accounts(self, duplicate_list):
        """Extract unique accounts from duplicate list"""
        accounts = set()
        for duplicate in duplicate_list:
            if isinstance(duplicate, dict):
                transaction1 = duplicate.get('transaction1', {})
                transaction2 = duplicate.get('transaction2', {})
                if transaction1.get('account'):
                    accounts.add(transaction1.get('account'))
                if transaction2.get('account'):
                    accounts.add(transaction2.get('account'))
        return list(accounts)

class UserAnalysisView(generics.GenericAPIView):
    """API view for retrieving user analysis results by file ID.
    
    This view provides comprehensive user analysis results including:
    - User transaction summaries
    - User anomaly detection
    - User risk assessment
    - Chart data for visualizations
    - Export-ready data
    """
    
    def get(self, request, file_id):
        """Get user analysis results for a specific file"""
        try:
            # Get the latest user analysis result
            from .models import UserAnalysisResult
            user_analysis = UserAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not user_analysis:
                return Response({
                    'error': 'No user analysis results found for this file',
                    'error_code': 'NO_USER_ANALYSIS_FOUND',
                    'suggestions': [
                        'Run user analysis for this file',
                        'Check if user analysis processing completed successfully'
                    ]
                }, status=404)
            
            # Prepare response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(user_analysis.id),
                    'analysis_date': user_analysis.analysis_date,
                    'processing_duration': user_analysis.processing_duration,
                    'status': user_analysis.status,
                    'analysis_version': user_analysis.analysis_version or '1.0.0'
                },
                'summary': {
                    'total_users': user_analysis.get_total_users(),
                    'total_transactions': user_analysis.get_total_transactions(),
                    'anomalies_detected': user_analysis.get_anomalies_count(),
                    'high_risk_users': user_analysis.get_high_risk_users_count(),
                    'risk_distribution': self._get_risk_distribution_from_list(user_analysis.user_risk_assessment)
                },
                'user_analysis': {
                    'user_transaction_summary': user_analysis.user_transaction_summary or [],
                    'user_debit_analysis': user_analysis.user_debit_analysis or [],
                    'user_account_distribution': user_analysis.user_account_distribution or [],
                    'user_fs_line_distribution': user_analysis.user_fs_line_distribution or []
                },
                'anomaly_detection': {
                    'user_anomalies': user_analysis.user_anomalies or [],
                    'anomaly_types': self._get_anomaly_type_summary(user_analysis.user_anomalies or []),
                    'ml_detected_anomalies': self._get_ml_anomalies(user_analysis.user_anomalies or [])
                },
                'risk_assessment': {
                    'user_risk_scores': user_analysis.user_risk_assessment if isinstance(user_analysis.user_risk_assessment, list) else [],
                    'high_risk_users': self._get_high_risk_users_from_list(user_analysis.user_risk_assessment),
                    'risk_factors': self._extract_risk_factors_from_list(user_analysis.user_risk_assessment)
                },
                'patterns': {
                    'user_patterns': user_analysis.user_patterns or {},
                    'activity_trends': self._extract_activity_trends(user_analysis.user_transaction_summary or [])
                },
                'visualizations': {
                    'chart_data': user_analysis.chart_data or {},
                    'chart_types': [
                        'transaction_vs_average',
                        'debit_value_analysis', 
                        'users_per_account',
                        'users_per_fs_line',
                        'anomaly_distribution'
                    ]
                },
                'export_data': {
                    'user_summary': self._prepare_user_summary_export(user_analysis.user_transaction_summary or []),
                    'anomaly_export': self._prepare_anomaly_export(user_analysis.user_anomalies or []),
                    'risk_export': self._prepare_risk_export_from_list(user_analysis.user_risk_assessment)
                }
            }
            
            # Log successful retrieval
            user_count = user_analysis.get_total_users()
            anomaly_count = user_analysis.get_anomalies_count()
            logger.info(f"Successfully retrieved user analysis for file {file_id} with {user_count} users and {anomaly_count} anomalies")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving user analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving user analysis',
                'error_code': 'USER_ANALYSIS_ERROR',
                'details': str(e)
            }, status=500)
    
    def _get_risk_distribution_from_list(self, user_risk_assessment):
        """Get risk distribution from list-based user risk assessment"""
        if not isinstance(user_risk_assessment, list):
            return {}
        
        risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        
        for user_risk in user_risk_assessment:
            if isinstance(user_risk, dict):
                risk_level = user_risk.get('risk_level', 'low')
                if risk_level in risk_distribution:
                    risk_distribution[risk_level] += 1
        
        return risk_distribution
    
    def _get_high_risk_users_from_list(self, user_risk_assessment):
        """Get high risk users from list-based user risk assessment"""
        if not isinstance(user_risk_assessment, list):
            return []
        
        high_risk_users = []
        for user_risk in user_risk_assessment:
            if isinstance(user_risk, dict) and user_risk.get('risk_level') in ['high', 'critical']:
                high_risk_users.append(user_risk)
        
        return high_risk_users
    
    def _extract_risk_factors_from_list(self, user_risk_assessment):
        """Extract key risk factors from list-based user risk assessment"""
        risk_factors = []
        
        if not isinstance(user_risk_assessment, list):
            return risk_factors
        
        # High risk users
        high_risk_users = [user for user in user_risk_assessment if isinstance(user, dict) and user.get('risk_level') in ['high', 'critical']]
        if high_risk_users:
            risk_factors.append({
                'factor': 'High-risk users',
                'count': len(high_risk_users),
                'impact': 'HIGH',
                'description': f'{len(high_risk_users)} users identified with high or critical risk levels'
            })
        
        # Users with high risk scores
        high_score_users = [user for user in user_risk_assessment if isinstance(user, dict) and user.get('risk_score', 0) > 70]
        if high_score_users:
            risk_factors.append({
                'factor': 'Users with high risk scores',
                'count': len(high_score_users),
                'impact': 'MEDIUM',
                'description': f'{len(high_score_users)} users have risk scores above 70'
            })
        
        return risk_factors
    
    def _prepare_risk_export_from_list(self, user_risk_assessment):
        """Prepare risk assessment data for export from list-based structure"""
        if not isinstance(user_risk_assessment, list):
            return []
        return user_risk_assessment
    
    def _get_anomaly_type_summary(self, anomalies):
        """Get summary of anomaly types"""
        anomaly_types = {}
        
        if not anomalies or not isinstance(anomalies, list):
            return anomaly_types
        
        for anomaly in anomalies:
            if not isinstance(anomaly, dict):
                continue
                
            anomaly_type = anomaly.get('anomaly_type', 'unknown')
            if anomaly_type not in anomaly_types:
                anomaly_types[anomaly_type] = {
                    'count': 0,
                    'severity_breakdown': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
                }
            anomaly_types[anomaly_type]['count'] += 1
            severity = anomaly.get('severity', 'medium')
            if severity in anomaly_types[anomaly_type]['severity_breakdown']:
                anomaly_types[anomaly_type]['severity_breakdown'][severity] += 1
        
        return anomaly_types
    
    def _get_ml_anomalies(self, anomalies):
        """Get ML-detected anomalies"""
        if not anomalies or not isinstance(anomalies, list):
            return []
        return [anomaly for anomaly in anomalies if isinstance(anomaly, dict) and anomaly.get('ml_detected', False)]
    
    def _extract_risk_factors(self, risk_assessment):
        """Extract key risk factors from user risk assessment"""
        risk_factors = []
        
        # Ensure risk_assessment is a dictionary
        if not isinstance(risk_assessment, dict):
            return risk_factors
        
        user_risk_scores = risk_assessment.get('user_risk_scores', [])
        
        # Ensure user_risk_scores is a list
        if not isinstance(user_risk_scores, list):
            return risk_factors
        
        # High risk users
        high_risk_users = [user for user in user_risk_scores if isinstance(user, dict) and user.get('risk_level') in ['high', 'critical']]
        if high_risk_users:
            risk_factors.append({
                'factor': 'High-risk users',
                'count': len(high_risk_users),
                'impact': 'HIGH',
                'description': f'{len(high_risk_users)} users identified with high or critical risk levels'
            })
        
        # Users with anomalies
        users_with_anomalies = [user for user in user_risk_scores if isinstance(user, dict) and user.get('anomaly_count', 0) > 0]
        if users_with_anomalies:
            risk_factors.append({
                'factor': 'Users with anomalies',
                'count': len(users_with_anomalies),
                'impact': 'MEDIUM',
                'description': f'{len(users_with_anomalies)} users have detected anomalies'
            })
        
        return risk_factors
    
    def _extract_activity_trends(self, user_summary):
        """Extract activity trends from user summary"""
        if not user_summary or not isinstance(user_summary, list):
            return {}
        
        # Filter out non-dictionary items
        valid_users = [user for user in user_summary if isinstance(user, dict)]
        
        if not valid_users:
            return {}
        
        # Calculate averages
        total_transactions = sum(user.get('transaction_count', 0) for user in valid_users)
        total_amount = sum(user.get('total_amount', 0) for user in valid_users)
        avg_transactions = total_transactions / len(valid_users) if valid_users else 0
        avg_amount = total_amount / len(valid_users) if valid_users else 0
        
        # Identify top performers
        top_transaction_users = sorted(valid_users, key=lambda x: x.get('transaction_count', 0), reverse=True)[:5]
        top_amount_users = sorted(valid_users, key=lambda x: x.get('total_amount', 0), reverse=True)[:5]
        
        return {
            'averages': {
                'avg_transactions_per_user': avg_transactions,
                'avg_amount_per_user': avg_amount
            },
            'top_performers': {
                'top_transaction_users': top_transaction_users,
                'top_amount_users': top_amount_users
            }
        }
    
    def _prepare_user_summary_export(self, user_summary):
        """Prepare user summary data for export"""
        return user_summary
    
    def _prepare_anomaly_export(self, anomalies):
        """Prepare anomaly data for export"""
        return anomalies
    
    def _prepare_risk_export(self, risk_assessment):
        """Prepare risk assessment data for export"""
        if not isinstance(risk_assessment, dict):
            return []
        return risk_assessment.get('user_risk_scores', [])

class BackdatedAnalysisView(generics.GenericAPIView):
    """API view for retrieving backdated analysis results by file ID.
    
    Returns:
    - Analysis metadata (ID, date, status, processing duration)
    - Backdated entries with detailed information
    - Summary statistics
    - Risk assessment for backdated transactions
    - Comprehensive chart data for visualizations
    """
    
    def _get_risk_level(self, risk_score):
        """
        Get risk level classification based on risk score
        
        Risk Level Classification System:
        - CRITICAL (80-100): Extremely high risk requiring immediate attention
        - HIGH (60-79): High risk requiring prompt investigation  
        - MEDIUM (30-59): Moderate risk with some concerns
        - LOW (0-29): Low risk with normal transaction patterns
        
        Args:
            risk_score (float): Risk score between 0-100
            
        Returns:
            str: Risk level classification
        """
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_risk_color(self, risk_score):
        """
        Get color code for risk score visualization
        
        Color Coding System:
        - Red (#FF6384): Critical risk (80-100)
        - Orange (#FF9F40): High risk (60-79)
        - Yellow (#FFCE56): Medium risk (30-59)
        - Green (#4BC0C0): Low risk (0-29)
        
        Args:
            risk_score (float): Risk score between 0-100
            
        Returns:
            str: Hexadecimal color code for visualization
        """
        if risk_score >= 80:
            return '#FF6384'  # Red for critical
        elif risk_score >= 60:
            return '#FF9F40'  # Orange for high
        elif risk_score >= 30:
            return '#FFCE56'  # Yellow for medium
        else:
            return '#4BC0C0'  # Green for low
    
    def _generate_backdated_chart_data(self, backdated_transactions, all_transactions):
        """
        Generate comprehensive chart data for backdated analysis
        
        Args:
            backdated_transactions: QuerySet of backdated transactions
            all_transactions: QuerySet of all transactions for context
            
        Returns:
            dict: Chart data for various visualizations
        """
        if not backdated_transactions.exists():
            return {
                'backdated_days_distribution': {'labels': [], 'data': [], 'colors': []},
                'risk_level_distribution': {'labels': [], 'data': [], 'colors': []},
                'backdated_activity_by_user': {'labels': [], 'data': [], 'colors': []},
                'backdated_amount_distribution': {'labels': [], 'data': [], 'colors': []},
                'financial_statement_line_breakdown': {'labels': [], 'data': [], 'colors': []},
                'monthly_backdated_trend': {'labels': [], 'data': [], 'colors': []}
            }
        
        # 1. Backdated Days Distribution
        days_ranges = [
            (1, 7, '1-7 days'),
            (8, 30, '8-30 days'),
            (31, 90, '31-90 days'),
            (91, 365, '91-365 days'),
            (366, float('inf'), '365+ days')
        ]
        
        days_distribution_data = []
        days_labels = []
        
        for min_days, max_days, label in days_ranges:
            if max_days == float('inf'):
                count = backdated_transactions.filter(backdated_days__gte=min_days).count()
            else:
                count = backdated_transactions.filter(
                    backdated_days__gte=min_days,
                    backdated_days__lt=max_days
                ).count()
            
            days_distribution_data.append(count)
            days_labels.append(label)
        
        days_distribution_chart = {
            'labels': days_labels,
            'data': days_distribution_data,
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384', '#36A2EB']
        }
        
        # 2. Risk Level Distribution
        risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        risk_level_data = []
        risk_colors = ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384']
        
        for i, level in enumerate(risk_levels):
            if level == 'LOW':
                count = backdated_transactions.filter(backdated_risk_score__lt=30).count()
            elif level == 'MEDIUM':
                count = backdated_transactions.filter(backdated_risk_score__gte=30, backdated_risk_score__lt=60).count()
            elif level == 'HIGH':
                count = backdated_transactions.filter(backdated_risk_score__gte=60, backdated_risk_score__lt=80).count()
            else:  # CRITICAL
                count = backdated_transactions.filter(backdated_risk_score__gte=80).count()
            
            if count > 0:
                risk_level_data.append(count)
            else:
                risk_level_data.append(0)
        
        risk_level_chart = {
            'labels': risk_levels,
            'data': risk_level_data,
            'colors': risk_colors
        }
        
        # 3. Backdated Activity by User
        user_activity_data = backdated_transactions.values('user_name').annotate(
            count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            avg_backdated_days=Avg('backdated_days')
        ).order_by('-count')[:10]  # Top 10 users
        
        user_activity_chart = {
            'labels': [item['user_name'] for item in user_activity_data],
            'data': [item['count'] for item in user_activity_data],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # 4. Backdated Amount Distribution
        amount_ranges = [
            (0, 100000, '0-100K SAR'),
            (100000, 1000000, '100K-1M SAR'),
            (1000000, 10000000, '1M-10M SAR'),
            (10000000, float('inf'), '10M+ SAR')
        ]
        
        amount_distribution_data = []
        amount_labels = []
        
        for min_amount, max_amount, label in amount_ranges:
            if max_amount == float('inf'):
                count = backdated_transactions.filter(amount_local_currency__gte=min_amount).count()
            else:
                count = backdated_transactions.filter(
                    amount_local_currency__gte=min_amount,
                    amount_local_currency__lt=max_amount
                ).count()
            
            amount_distribution_data.append(count)
            amount_labels.append(label)
        
        amount_distribution_chart = {
            'labels': amount_labels,
            'data': amount_distribution_data,
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384']
        }
        
        # 5. Financial Statement Line Breakdown (by GL Account)
        gl_account_data = backdated_transactions.values('gl_account').annotate(
            count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            avg_backdated_days=Avg('backdated_days')
        ).order_by('-total_amount')[:10]  # Top 10 accounts by amount
        
        gl_account_chart = {
            'labels': [f"GL {item['gl_account']}" for item in gl_account_data],
            'data': [item['count'] for item in gl_account_data],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # 6. Monthly Backdated Trend
        monthly_trend_data = backdated_transactions.extra(
            select={'month': "EXTRACT(month FROM posting_date)"}
        ).values('month').annotate(
            count=Count('id')
        ).order_by('month')
        
        # Create complete month labels
        month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_data = [0] * 12
        
        for item in monthly_trend_data:
            month_index = int(item['month']) - 1  # Convert Decimal to int, then to 0-based index
            if 0 <= month_index < 12:
                monthly_data[month_index] = item['count']
        
        monthly_trend_chart = {
            'labels': month_labels,
            'data': monthly_data,
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384', '#36A2EB', '#9966FF', '#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384', '#36A2EB', '#9966FF']
        }
        
        return {
            'backdated_days_distribution': days_distribution_chart,
            'risk_level_distribution': risk_level_chart,
            'backdated_activity_by_user': user_activity_chart,
            'backdated_amount_distribution': amount_distribution_chart,
            'financial_statement_line_breakdown': gl_account_chart,
            'monthly_backdated_trend': monthly_trend_chart
        }
    
    def get(self, request, file_id):
        """Get backdated analysis results for a specific file"""
        try:
            # Verify the file exists
            data_file = DataFile.objects.get(id=file_id)
            
            # Get the latest backdated analysis result
            try:
                backdated_analysis = BackdatedAnalysisResult.objects.filter(
                    data_file=data_file,
                    status='COMPLETED'
                ).latest('analysis_date')
            except BackdatedAnalysisResult.DoesNotExist:
                return Response(
                    {
                        'error': 'No backdated analysis results found for this file',
                        'file_id': str(file_id),
                        'file_name': data_file.file_name,
                        'status': 'NOT_AVAILABLE',
                        'suggestions': [
                            'Run backdated analysis for this file',
                            'Check if backdated analysis processing completed successfully'
                        ]
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Get transactions for this file to enhance the analysis
            transactions = SAPGLPosting.objects.filter(data_file=data_file)
            
            # Calculate enhanced statistics
            total_transactions = transactions.count()
            backdated_transactions = transactions.filter(is_backdated=True)
            backdated_count = backdated_transactions.count()
            
            # Calculate risk statistics for backdated transactions
            if backdated_transactions.exists():
                avg_risk_score = backdated_transactions.aggregate(avg=Avg('backdated_risk_score'))['avg'] or 0
                max_risk_score = backdated_transactions.aggregate(max=Max('backdated_risk_score'))['max'] or 0
                min_risk_score = backdated_transactions.aggregate(min=Min('backdated_risk_score'))['min'] or 0
                total_backdated_amount = backdated_transactions.aggregate(total=Sum('amount_local_currency'))['total'] or 0
                avg_backdated_days = backdated_transactions.aggregate(avg=Avg('backdated_days'))['avg'] or 0
                max_backdated_days = backdated_transactions.aggregate(max=Max('backdated_days'))['max'] or 0
            else:
                avg_risk_score = max_risk_score = min_risk_score = total_backdated_amount = avg_backdated_days = max_backdated_days = 0
            
            # Risk level distribution for backdated transactions
            risk_distribution = {
                'low_risk': backdated_transactions.filter(backdated_risk_score__lt=30).count(),
                'medium_risk': backdated_transactions.filter(backdated_risk_score__gte=30, backdated_risk_score__lt=60).count(),
                'high_risk': backdated_transactions.filter(backdated_risk_score__gte=60, backdated_risk_score__lt=80).count(),
                'critical_risk': backdated_transactions.filter(backdated_risk_score__gte=80).count()
            }
            
            # Get high-risk backdated transactions
            high_risk_backdated = backdated_transactions.filter(
                backdated_risk_score__gte=60
            ).values(
                'id', 'document_number', 'gl_account', 'amount_local_currency',
                'user_name', 'posting_date', 'backdated_risk_score', 'backdated_days'
            ).order_by('-backdated_risk_score')[:10]
            
            # Generate comprehensive chart data
            chart_data = self._generate_backdated_chart_data(backdated_transactions, transactions)
            
            # Prepare response data
            response_data = {
                'file_info': {
                    'file_id': str(file_id),
                    'file_name': data_file.file_name,
                    'engagement_id': data_file.engagement_id,
                    'client_name': data_file.client_name,
                    'company_name': data_file.company_name,
                    'fiscal_year': data_file.fiscal_year,
                    'uploaded_at': data_file.uploaded_at,
                    'processed_at': data_file.processed_at
                },
                'analysis_info': {
                    'analysis_id': str(backdated_analysis.id),
                    'analysis_date': backdated_analysis.analysis_date,
                    'processing_duration': backdated_analysis.processing_duration,
                    'status': backdated_analysis.status,
                    'analysis_version': backdated_analysis.analysis_version or '1.0.0'
                },
                'summary_statistics': {
                    'total_transactions': total_transactions,
                    'backdated_transactions': backdated_count,
                    'backdated_percentage': (backdated_count / total_transactions * 100) if total_transactions > 0 else 0,
                    'total_backdated_amount': float(total_backdated_amount),
                    'avg_backdated_amount': float(total_backdated_amount / backdated_count) if backdated_count > 0 else 0,
                    'avg_risk_score': float(avg_risk_score),
                    'max_risk_score': float(max_risk_score),
                    'min_risk_score': float(min_risk_score),
                    'avg_backdated_days': float(avg_backdated_days),
                    'max_backdated_days': int(max_backdated_days)
                },
                'risk_assessment': {
                    'risk_distribution': risk_distribution,
                    'high_risk_backdated': list(high_risk_backdated),
                    'risk_level': self._get_risk_level(avg_risk_score),
                    'risk_color': self._get_risk_color(avg_risk_score),
                    'overall_risk_score': float(avg_risk_score)
                },
                'chart_data': chart_data,
                'backdated_entries': backdated_analysis.backdated_entries or [],
                'backdated_patterns': backdated_analysis.analysis_info or {},
                'analysis_metadata': {
                    'detection_methods': backdated_analysis.analysis_info.get('detection_methods', []),
                    'confidence_scores': backdated_analysis.analysis_info.get('confidence_scores', {}),
                    'false_positive_indicators': backdated_analysis.analysis_info.get('false_positive_indicators', [])
                },
                'recommendations': [
                    {
                        'priority': 'HIGH' if backdated_count > 5 else 'MEDIUM',
                        'action': 'Investigate backdated transactions',
                        'description': f'{backdated_count} backdated transactions detected',
                        'count': backdated_count
                    },
                    {
                        'priority': 'HIGH' if avg_risk_score > 60 else 'MEDIUM',
                        'action': 'Review high-risk backdated transactions',
                        'description': f'Average risk score: {avg_risk_score:.2f}',
                        'risk_score': avg_risk_score
                    },
                    {
                        'priority': 'HIGH' if avg_backdated_days > 30 else 'MEDIUM',
                        'action': 'Review transactions with long backdated periods',
                        'description': f'Average backdated days: {avg_backdated_days:.1f}',
                        'backdated_days': avg_backdated_days
                    },
                    {
                        'priority': 'MEDIUM',
                        'action': 'Implement backdated prevention controls',
                        'description': 'Establish controls to prevent future backdated entries'
                    }
                ],
                'audit_implications': {
                    'immediate_actions': [
                        'Review all backdated transactions for validity',
                        'Investigate high-risk backdated patterns',
                        'Verify business justification for backdated entries',
                        'Check compliance with accounting standards'
                    ],
                    'follow_up_actions': [
                        'Implement backdated detection controls',
                        'Train staff on proper posting procedures',
                        'Establish approval workflows for backdated entries',
                        'Review internal control effectiveness'
                    ],
                    'compliance_considerations': [
                        'Ensure proper documentation for backdated transactions',
                        'Verify compliance with accounting standards',
                        'Review internal control effectiveness',
                        'Assess fraud risk indicators'
                    ]
                }
            }
            
            # Add critical alerts if there are significant issues
            critical_alerts = []
            if backdated_count > 10:
                critical_alerts.append({
                    'type': 'CRITICAL',
                    'category': 'HIGH_BACKDATED_COUNT',
                    'message': f'High number of backdated transactions detected ({backdated_count})',
                    'severity': 'HIGH',
                    'action_required': 'Immediate investigation of backdated patterns'
                })
            
            if avg_risk_score > 70:
                critical_alerts.append({
                    'type': 'CRITICAL',
                    'category': 'HIGH_BACKDATED_RISK',
                    'message': f'High average risk score for backdated transactions ({avg_risk_score:.2f})',
                    'severity': 'CRITICAL',
                    'action_required': 'Immediate review of high-risk backdated transactions'
                })
            
            if avg_backdated_days > 90:
                critical_alerts.append({
                    'type': 'CRITICAL',
                    'category': 'LONG_BACKDATED_PERIOD',
                    'message': f'Long average backdated period ({avg_backdated_days:.1f} days)',
                    'severity': 'CRITICAL',
                    'action_required': 'Review transactions with extended backdated periods'
                })
            
            if total_backdated_amount > 10000000:  # 10M SAR
                critical_alerts.append({
                    'type': 'WARNING',
                    'category': 'HIGH_BACKDATED_AMOUNT',
                    'message': f'High total amount in backdated transactions ({total_backdated_amount:,.2f} SAR)',
                    'severity': 'HIGH',
                    'action_required': 'Review backdated transactions for financial impact'
                })
            
            response_data['critical_alerts'] = critical_alerts
            
            # Log successful response
            logger.info(f"Successfully retrieved backdated analysis for file {file_id} with {backdated_count} backdated transactions")
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except DataFile.DoesNotExist:
            return Response(
                {
                    'error': f'Data file with ID {file_id} not found',
                    'file_id': str(file_id)
                },
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error retrieving backdated analysis for file {file_id}: {e}")
            return Response(
                {
                    'error': 'Error occurred while retrieving backdated analysis',
                    'error_code': 'BACKDATED_ANALYSIS_ERROR',
                    'details': str(e),
                    'file_id': str(file_id),
                    'timestamp': timezone.now().isoformat()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )