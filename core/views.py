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
from rest_framework.views import APIView

from .models import SAPGLPosting, DataFile, FileProcessingJob, MLModelTraining, OverallAnalysisResult, RiskScoringDocument, DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult, ClosingEntriesAnalysisResult, UnusualDaysAnalysisResult, HolidayAnalysisResult, GeneralAnalysisResult
from .serializers import (
    DataFileSerializer, DataFileUploadSerializer, DataUploadResponseSerializer,
    FileProcessingJobSerializer, MLModelTrainingSerializer, TargetedAnomalyUploadSerializer,
    SAPGLPostingListSerializer, ClosingEntriesListSerializer, BackdatedEntriesListSerializer,
    UnusualDaysListSerializer,HolidayListSerializer, DuplicateListSerializer, UserListSerializer
)
from .tasks import run_restructured_analysis
from .excel_export import AuditExcelExporter

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
    
    @action(detail=False, methods=['post'])
    def train_holiday_model(self, request):
        """Train holiday analysis ML model specifically"""
        try:
            # Get the latest processing job or create a dummy one
            latest_job = FileProcessingJob.objects.filter(status='COMPLETED').order_by('-created_at').first()
            
            if not latest_job:
                return Response(
                    {'error': 'No completed processing jobs found. Please upload and process a file first.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get transactions for training
            from .models import SAPGLPosting
            transactions = list(SAPGLPosting.objects.filter(data_file=latest_job.data_file))
            
            if not transactions:
                return Response(
                    {'error': 'No transactions found for training.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Create training session
            training_session = MLModelTraining.objects.create(
                session_name=f"Holiday Model Training {latest_job.id}",
                description="Holiday analysis ML model training session",
                model_type='holiday',
                training_data_size=len(transactions),
                training_data_date_range={
                    'min_date': min(t.posting_date for t in transactions if t.posting_date).isoformat(),
                    'max_date': max(t.posting_date for t in transactions if t.posting_date).isoformat()
                },
                status='TRAINING',
                started_at=timezone.now()
            )
            
            # Train holiday model
            from .specialized_analysis_models import AnalysisModelManager
            model_manager = AnalysisModelManager()
            
            # Generate holiday labels using holiday_utils
            labels = []
            try:
                from .holiday_utils import is_holiday
                # Default to Saudi Arabian holidays
                country_code = 'saudiarabian'
                for t in transactions:
                    if t.posting_date:
                        is_holiday_posting = is_holiday(country_code, t.posting_date)
                        labels.append(1 if is_holiday_posting else 0)
                    else:
                        labels.append(0)
                logger.info(f"Generated {sum(labels)} holiday labels from {len(transactions)} transactions")
            except Exception as e:
                logger.warning(f"Could not generate holiday labels using holiday_utils: {e}")
                # Fallback to simple heuristic
                for t in transactions:
                    is_anomaly = (t.posting_date and t.posting_date.weekday() >= 5)  # Weekend
                    labels.append(1 if is_anomaly else 0)
                logger.info(f"Generated {sum(labels)} holiday labels using fallback heuristic")
            
            # Train the holiday model
            success = model_manager.ensure_model_trained('holiday', transactions, labels)
            
            # Update training session
            training_session.status = 'COMPLETED' if success else 'FAILED'
            training_session.completed_at = timezone.now()
            training_session.training_duration = (timezone.now() - training_session.started_at).total_seconds()
            
            if success:
                # Get model performance metrics
                holiday_model = model_manager.get_model('holiday')
                if holiday_model and holiday_model.is_trained:
                    training_session.performance_metrics = {
                        'model_type': 'holiday',
                        'training_success': True,
                        'feature_count': 12,  # Standard feature count for holiday model
                        'positive_cases': sum(labels),
                        'total_cases': len(transactions),
                        'positive_rate': (sum(labels) / len(transactions)) * 100 if transactions else 0
                    }
                training_session.save()
                
                return Response({
                    'message': 'Holiday model training completed successfully',
                    'status': 'COMPLETED',
                    'training_id': str(training_session.id),
                    'job_id': str(latest_job.id),
                    'training_duration': training_session.training_duration,
                    'performance_metrics': training_session.performance_metrics
                })
            else:
                training_session.error_message = "Failed to train holiday model"
                training_session.save()
                
                return Response({
                    'error': 'Failed to train holiday model',
                    'status': 'FAILED',
                    'training_id': str(training_session.id)
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                
        except Exception as e:
            logger.error(f"Error training holiday model: {e}")
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
    
    @action(detail=False, methods=['get'])
    def list_queued_tasks(self, request):
        """List all queued tasks"""
        try:
            import json
            import redis
            from django.conf import settings
            
            r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
            queued_tasks = []
            
            # Get all queued task keys
            pattern = "queued_task:*"
            keys = r.keys(pattern)
            
            for key in keys:
                task_data = r.get(key)
                if task_data:
                    task_info = json.loads(task_data)
                    queued_tasks.append(task_info)
            
            return Response({
                'status': 'success',
                'queued_tasks': queued_tasks,
                'count': len(queued_tasks)
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=False, methods=['post'])
    def execute_queued_task(self, request):
        """Execute a specific queued task"""
        try:
            job_id = request.data.get('job_id')
            if not job_id:
                return Response({
                    'status': 'error',
                    'message': 'job_id is required'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            import json
            import redis
            from django.conf import settings
            
            r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
            queue_key = f"queued_task:{job_id}"
            
            # Get task info from Redis
            task_data = r.get(queue_key)
            if not task_data:
                return Response({
                    'status': 'error',
                    'message': f'No queued task found for job_id: {job_id}'
                }, status=status.HTTP_404_NOT_FOUND)
            
            task_info = json.loads(task_data)
            
            # Execute the task
            from .tasks import run_restructured_analysis
            result = run_restructured_analysis.delay(job_id)
            
            # Remove from queue
            r.delete(queue_key)
            
            # Update job status
            from .models import FileProcessingJob
            try:
                job = FileProcessingJob.objects.get(id=job_id)
                job.status = 'PROCESSING'
                job.save()
            except FileProcessingJob.DoesNotExist:
                pass
            
            return Response({
                'status': 'success',
                'message': f'Task executed for job_id: {job_id}',
                'task_id': result.id
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=False, methods=['post'])
    def execute_all_queued_tasks(self, request):
        """Execute all queued tasks"""
        try:
            import json
            import redis
            from django.conf import settings
            
            r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
            pattern = "queued_task:*"
            keys = r.keys(pattern)
            
            executed_tasks = []
            
            for key in keys:
                task_data = r.get(key)
                if task_data:
                    task_info = json.loads(task_data)
                    job_id = task_info['job_id']
                    
                    # Execute the task
                    from .tasks import run_restructured_analysis
                    result = run_restructured_analysis.delay(job_id)
                    
                    # Remove from queue
                    r.delete(key)
                    
                    # Update job status
                    from .models import FileProcessingJob
                    try:
                        job = FileProcessingJob.objects.get(id=job_id)
                        job.status = 'PROCESSING'
                        job.save()
                    except FileProcessingJob.DoesNotExist:
                        pass
                    
                    executed_tasks.append({
                        'job_id': job_id,
                        'task_id': result.id
                    })
            
            return Response({
                'status': 'success',
                'message': f'Executed {len(executed_tasks)} tasks',
                'executed_tasks': executed_tasks
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
                status='QUEUED'  # Changed from 'PENDING' to 'QUEUED' to indicate it's in queue
            )
            
            # Add task to queue without executing it
            from .tasks import run_restructured_analysis
            # Store task info but don't execute immediately
            task_info = {
                'job_id': str(processing_job.id),
                'task_name': 'run_restructured_analysis',
                'status': 'QUEUED',
                'created_at': timezone.now().isoformat()
            }
            
            # Store task info in Redis for manual execution later
            import json
            import redis
            from django.conf import settings
            
            try:
                r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
                queue_key = f"queued_task:{processing_job.id}"
                r.setex(queue_key, 86400, json.dumps(task_info))  # Store for 24 hours
                logger.info(f"Task queued for job {processing_job.id} - ready for manual execution")
            except Exception as redis_error:
                logger.warning(f"Could not store task in Redis: {redis_error}")
            
            logger.info(f"Created processing job {processing_job.id} for file {data_file.file_name} - task queued for manual execution")
            return processing_job
            
        except Exception as e:
            logger.error(f"Error creating processing job: {e}")
            # Don't fail the upload if job creation fails
            return None



class FileListingAPIView(generics.GenericAPIView):
    """View for listing all files with processing status
    
    Query Parameters:
    - status: Filter by file status
    - engagement_id: Filter by engagement ID
    - client_name: Filter by client name
    - company_name: Filter by company name
    - fiscal_year: Filter by fiscal year
    - date_from: Filter by upload date from
    - date_to: Filter by upload date to
    - celery_status: Filter by celery processing status
        - 'processed': Files with PROCESSING or COMPLETED status
        - 'completed': Files with COMPLETED status only
        - 'processing': Files with PROCESSING status only
        - 'failed': Files with FAILED or CELERY_ERROR status
        - 'pending': Files with PENDING or QUEUED status
    """
    
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
            celery_status_filter = request.query_params.get('celery_status', None)
            
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
                
                # Apply celery status filter if specified
                if celery_status_filter:
                    if not processing_job:
                        # If no processing job exists, skip this file
                        continue
                    
                    # Filter by celery status
                    if celery_status_filter == 'processed':
                        if processing_job.status not in ['PROCESSING', 'COMPLETED']:
                            continue
                    elif celery_status_filter == 'completed':
                        if processing_job.status != 'COMPLETED':
                            continue
                    elif celery_status_filter == 'processing':
                        if processing_job.status != 'PROCESSING':
                            continue
                    elif celery_status_filter == 'failed':
                        if processing_job.status not in ['FAILED', 'CELERY_ERROR']:
                            continue
                    elif celery_status_filter == 'pending':
                        if processing_job.status not in ['PENDING', 'QUEUED']:
                            continue
                
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
            
            # Add celery status statistics
            celery_completed = len([f for f in file_list if f['processing_status'] == 'COMPLETED'])
            celery_processing = len([f for f in file_list if f['processing_status'] == 'PROCESSING'])
            celery_failed = len([f for f in file_list if f['processing_status'] in ['FAILED', 'CELERY_ERROR']])
            celery_pending = len([f for f in file_list if f['processing_status'] in ['PENDING', 'QUEUED']])
            
            return Response({
                'files': file_list,
                'summary': {
                    'total_files': total_files,
                    'processed_files': processed_files,
                    'pending_files': pending_files,
                    'failed_files': failed_files,
                    'processing_files': processing_files,
                    'celery_completed': celery_completed,
                    'celery_processing': celery_processing,
                    'celery_failed': celery_failed,
                    'celery_pending': celery_pending
                },
                'filters_applied': {
                    'status': status_filter,
                    'engagement_id': engagement_id,
                    'client_name': client_name,
                    'company_name': company_name,
                    'fiscal_year': fiscal_year,
                    'date_from': date_from,
                    'date_to': date_to,
                    'celery_status': celery_status_filter
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
    """
    Enhanced view for retrieving comprehensive file analysis statistics and chart data
    
    This view provides:
    - Overall analysis statistics from the new data structure
    - Risk analysis data and distributions
    - Chart data for visualizations
    - No listing functionality - only statistics and chart data
    """
    
    def get(self, request, file_id):
        """
        Get comprehensive overall analysis statistics and chart data for a specific file
        
        Args:
            file_id (str): UUID of the DataFile to get analysis for
            
        Returns:
            JSON response with comprehensive statistics and chart data
        """
        try:
            # Validate file exists
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
            
            # Get the latest overall analysis result for this file
            overall_analysis = OverallAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            # Get the latest risk scoring document for this file
            risk_document = RiskScoringDocument.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-document_date').first()
            
            # Get the latest general analysis result for this file
            general_analysis = GeneralAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            # Get all specific analysis results
            duplicate_analysis = DuplicateAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            backdated_analysis = BackdatedAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            user_analysis = UserAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            holiday_analysis = HolidayAnalysisResult.objects.filter(
                data_file=data_file,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            # Prepare response data structure
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
                'analysis_metadata': {
                    'has_overall_analysis': bool(overall_analysis),
                    'has_risk_analysis': bool(risk_document),
                    'has_general_analysis': bool(general_analysis),
                    'has_duplicate_analysis': bool(duplicate_analysis),
                    'has_backdated_analysis': bool(backdated_analysis),
                    'has_user_analysis': bool(user_analysis),
                    'has_unusual_days_analysis': bool(unusual_days_analysis),
                    'has_closing_entries_analysis': bool(closing_entries_analysis),
                    'has_holiday_analysis': bool(holiday_analysis),
                    'analysis_timestamp': timezone.now().isoformat(),
                    'analysis_version': '3.0.0'
                },
                'overall_statistics': {},
                'risk_statistics': {},
                'general_statistics': {},
                'anomaly_statistics': {},
                'chart_data': {},
                'summary_dashboard': {}
            }
            
            # Overall Analysis Statistics
            if overall_analysis:
                response_data['overall_statistics'] = {
                    'analysis_id': str(overall_analysis.id),
                    'analysis_date': overall_analysis.analysis_date.isoformat(),
                    'processing_duration': overall_analysis.processing_duration,
                    'transaction_summary': overall_analysis.transaction_summary or {},
                    'flag_summary': overall_analysis.flag_summary or {},
                    'expense_analysis': overall_analysis.expense_analysis or {},
                    'risk_assessment': overall_analysis.risk_assessment or {},
                    'flagged_transactions_count': len(overall_analysis.flagged_transactions) if overall_analysis.flagged_transactions else 0
                }
            
            # Risk Analysis Statistics
            if risk_document:
                response_data['risk_statistics'] = {
                    'document_id': str(risk_document.id),
                    'document_date': risk_document.document_date.isoformat(),
                    'total_transactions': risk_document.total_transactions,
                    'high_risk_transactions': risk_document.high_risk_transactions,
                    'medium_risk_transactions': risk_document.medium_risk_transactions,
                    'low_risk_transactions': risk_document.low_risk_transactions,
                    'critical_risk_transactions': risk_document.critical_risk_transactions,
                    'overall_risk_score': risk_document.overall_risk_score,
                    'risk_distributions': risk_document.risk_distributions or {},
                    'methodology_overview': risk_document.methodology_overview or {},
                    'recommendations': risk_document.recommendations or {},
                    'audit_implications': risk_document.audit_implications or {}
                }
            
            # General Analysis Statistics
            if general_analysis:
                response_data['general_statistics'] = {
                    'analysis_id': str(general_analysis.id),
                    'analysis_date': general_analysis.analysis_date.isoformat(),
                    'trial_balance_summary': general_analysis.trial_balance_summary or {},
                    'gl_account_summaries_count': len(general_analysis.gl_account_summaries) if general_analysis.gl_account_summaries else 0,
                    'user_summaries_count': len(general_analysis.user_summaries) if general_analysis.user_summaries else 0,
                    'statistical_calculations': general_analysis.statistical_calculations or {}
                }
            
            # Anomaly Statistics - Combine all anomaly types
            anomaly_stats = {
                'duplicate_analysis': {},
                'backdated_analysis': {},
                'user_analysis': {},
                'unusual_days_analysis': {},
                'closing_entries_analysis': {},
                'holiday_analysis': {},
                'total_anomalies': 0,
                'anomaly_types_detected': []
            }
            
            # Duplicate Analysis
            if duplicate_analysis:
                try:
                    duplicate_count = duplicate_analysis.get_duplicate_count()
                    total_amount = duplicate_analysis.get_total_amount()
                    risk_distribution = duplicate_analysis.get_risk_distribution()
                except Exception as e:
                    logger.warning(f"Error getting duplicate analysis data: {e}")
                    duplicate_count = 0
                    total_amount = 0
                    risk_distribution = {}
                
                anomaly_stats['duplicate_analysis'] = {
                    'analysis_id': str(duplicate_analysis.id),
                    'analysis_date': duplicate_analysis.analysis_date.isoformat(),
                    'duplicate_count': duplicate_count,
                    'total_amount': total_amount,
                    'risk_distribution': risk_distribution,
                    'analysis_info': duplicate_analysis.analysis_info or {}
                }
                anomaly_stats['total_anomalies'] += duplicate_count
                anomaly_stats['anomaly_types_detected'].append('duplicate')
            
            # Backdated Analysis
            if backdated_analysis:
                try:
                    backdated_count = backdated_analysis.get_backdated_count()
                    total_amount = backdated_analysis.get_total_amount()
                    risk_distribution = backdated_analysis.get_risk_distribution()
                except Exception as e:
                    logger.warning(f"Error getting backdated analysis data: {e}")
                    backdated_count = 0
                    total_amount = 0
                    risk_distribution = {}
                
                anomaly_stats['backdated_analysis'] = {
                    'analysis_id': str(backdated_analysis.id),
                    'analysis_date': backdated_analysis.analysis_date.isoformat(),
                    'backdated_count': backdated_count,
                    'total_amount': total_amount,
                    'risk_distribution': risk_distribution,
                    'analysis_info': backdated_analysis.analysis_info or {}
                }
                anomaly_stats['total_anomalies'] += backdated_count
                anomaly_stats['anomaly_types_detected'].append('backdated')
            
            # User Analysis
            if user_analysis:
                try:
                    anomalies_count = user_analysis.get_anomalies_count()
                    total_users = user_analysis.get_total_users()
                    total_transactions = user_analysis.get_total_transactions()
                    high_risk_users_count = user_analysis.get_high_risk_users_count()
                except Exception as e:
                    logger.warning(f"Error getting user analysis data: {e}")
                    anomalies_count = 0
                    total_users = 0
                    total_transactions = 0
                    high_risk_users_count = 0
                
                anomaly_stats['user_analysis'] = {
                    'analysis_id': str(user_analysis.id),
                    'analysis_date': user_analysis.analysis_date.isoformat(),
                    'total_users': total_users,
                    'total_transactions': total_transactions,
                    'anomalies_count': anomalies_count,
                    'high_risk_users_count': high_risk_users_count,
                    'analysis_info': user_analysis.analysis_info or {},
                    'user_anomalies': user_analysis.user_anomalies or [],
                    'user_risk_assessment': user_analysis.user_risk_assessment or {}
                }
                anomaly_stats['total_anomalies'] += anomalies_count
                anomaly_stats['anomaly_types_detected'].append('user_anomaly')
            
            # Unusual Days Analysis
            if unusual_days_analysis:
                try:
                    weekend_count = unusual_days_analysis.get_weekend_transactions_count()
                    unusual_days_count = unusual_days_analysis.get_unusual_days_count()
                except Exception as e:
                    logger.warning(f"Error getting unusual days analysis data: {e}")
                    weekend_count = 0
                    unusual_days_count = 0
                
                anomaly_stats['unusual_days_analysis'] = {
                    'analysis_id': str(unusual_days_analysis.id),
                    'analysis_date': unusual_days_analysis.analysis_date.isoformat(),
                    'weekend_transactions_count': weekend_count,
                    'unusual_days_count': unusual_days_count,
                    'analysis_info': unusual_days_analysis.analysis_info or {}
                }
                anomaly_stats['total_anomalies'] += weekend_count + unusual_days_count
                anomaly_stats['anomaly_types_detected'].extend(['weekend_activity', 'unusual_days'])
            
            # Closing Entries Analysis
            if closing_entries_analysis:
                try:
                    closing_count = closing_entries_analysis.get_closing_entries_count()
                    post_close_count = closing_entries_analysis.get_post_close_entries_count()
                except Exception as e:
                    logger.warning(f"Error getting closing entries analysis data: {e}")
                    closing_count = 0
                    post_close_count = 0
                
                anomaly_stats['closing_entries_analysis'] = {
                    'analysis_id': str(closing_entries_analysis.id),
                    'analysis_date': closing_entries_analysis.analysis_date.isoformat(),
                    'closing_entries_count': closing_count,
                    'post_close_entries_count': post_close_count,
                    'analysis_info': closing_entries_analysis.analysis_info or {}
                }
                anomaly_stats['total_anomalies'] += closing_count + post_close_count
                anomaly_stats['anomaly_types_detected'].extend(['closing_entries', 'post_close_entries'])
            
            # Holiday Analysis
            if holiday_analysis:
                try:
                    # Get data from analysis_info first, then fallback to model methods
                    analysis_info = holiday_analysis.analysis_info or {}
                    
                    # Extract holiday data from analysis_info
                    holiday_count = analysis_info.get('holiday_transactions_count', 0)
                    if holiday_count == 0:
                        holiday_count = holiday_analysis.get_holiday_postings_count()
                    
                    holiday_percentage = analysis_info.get('holiday_percentage', 0)
                    if holiday_percentage == 0:
                        holiday_percentage = holiday_analysis.get_holiday_percentage()
                    
                    # Count unique holidays from breakdown
                    holiday_breakdown = analysis_info.get('holiday_breakdown', [])
                    unique_holidays = len([h for h in holiday_breakdown if h and len(h) > 1 and h[1] > 0])
                    if unique_holidays == 0:
                        unique_holidays = holiday_analysis.get_unique_holidays()
                    
                    overall_risk_score = holiday_analysis.get_overall_risk_score()
                    
                    # Calculate total holiday amount
                    total_holiday_amount = analysis_info.get('total_holiday_amount', 0)
                    
                except Exception as e:
                    logger.warning(f"Error getting holiday analysis data: {e}")
                    holiday_count = 0
                    holiday_percentage = 0
                    unique_holidays = 0
                    overall_risk_score = 0
                    total_holiday_amount = 0
                
                anomaly_stats['holiday_analysis'] = {
                    'analysis_id': str(holiday_analysis.id),
                    'analysis_date': holiday_analysis.analysis_date.isoformat(),
                    'holiday_postings_count': holiday_count,
                    'holiday_percentage': holiday_percentage,
                    'unique_holidays': unique_holidays,
                    'overall_risk_score': overall_risk_score,
                    'total_holiday_amount': total_holiday_amount,
                    'analysis_info': holiday_analysis.analysis_info or {}
                }
                anomaly_stats['total_anomalies'] += holiday_count
                anomaly_stats['anomaly_types_detected'].append('holiday_posting')
            
            response_data['anomaly_statistics'] = anomaly_stats
            
            # Chart Data - Combine chart data from all analyses
            chart_data = {
                'overall_charts': {},
                'risk_charts': {},
                'anomaly_charts': {},
                'temporal_charts': {},
                'user_charts': {},
                'account_charts': {}
            }
            
            # Overall Analysis Chart Data
            if overall_analysis and overall_analysis.chart_data:
                chart_data['overall_charts'] = overall_analysis.chart_data
            
            # Risk Analysis Chart Data
            if risk_document and risk_document.risk_distributions:
                chart_data['risk_charts'] = {
                    'risk_distribution': risk_document.risk_distributions.get('distribution_chart', {}),
                    'risk_levels': risk_document.risk_distributions.get('risk_levels_chart', {}),
                    'risk_trends': risk_document.risk_distributions.get('risk_trends_chart', {})
                }
            
            # Anomaly Chart Data
            if duplicate_analysis and duplicate_analysis.chart_data:
                chart_data['anomaly_charts']['duplicate_charts'] = duplicate_analysis.chart_data
            
            if backdated_analysis and backdated_analysis.chart_data:
                chart_data['anomaly_charts']['backdated_charts'] = backdated_analysis.chart_data
            
            if holiday_analysis and holiday_analysis.chart_data:
                chart_data['anomaly_charts']['holiday_charts'] = holiday_analysis.chart_data
            
            # User Analysis Chart Data
            if user_analysis and user_analysis.chart_data:
                chart_data['user_charts'] = user_analysis.chart_data
            
            # General Analysis Chart Data
            if general_analysis and general_analysis.chart_data:
                chart_data['account_charts'] = general_analysis.chart_data
            
            response_data['chart_data'] = chart_data
            
            # Summary Dashboard - Key metrics for quick overview
            overall_risk_score = risk_document.overall_risk_score if risk_document else 0
            summary_dashboard = {
                'total_transactions': data_file.total_records,
                'total_anomalies': anomaly_stats['total_anomalies'],
                'anomaly_percentage': (anomaly_stats['total_anomalies'] / data_file.total_records * 100) if data_file.total_records > 0 else 0,
                'overall_risk_score': overall_risk_score,
                'risk_level': self._get_risk_level(overall_risk_score),
                'analysis_coverage': {
                    'overall_analysis': bool(overall_analysis),
                    'risk_analysis': bool(risk_document),
                    'duplicate_analysis': bool(duplicate_analysis),
                    'backdated_analysis': bool(backdated_analysis),
                    'user_analysis': bool(user_analysis),
                    'unusual_days_analysis': bool(unusual_days_analysis),
                    'closing_entries_analysis': bool(closing_entries_analysis),
                    'holiday_analysis': bool(holiday_analysis)
                },
                'key_metrics': {
                    'duplicate_transactions': anomaly_stats['duplicate_analysis'].get('duplicate_count', 0),
                    'backdated_transactions': anomaly_stats['backdated_analysis'].get('backdated_count', 0),
                    'user_anomalies': anomaly_stats['user_analysis'].get('anomalies_count', 0),
                    'weekend_transactions': anomaly_stats['unusual_days_analysis'].get('weekend_transactions_count', 0),
                    'holiday_transactions': anomaly_stats['holiday_analysis'].get('holiday_postings_count', 0),
                    'closing_entries': anomaly_stats['closing_entries_analysis'].get('closing_entries_count', 0)
                }
            }
            
            response_data['summary_dashboard'] = summary_dashboard
            
            # Add methodology clarification
            response_data['methodology_notes'] = {
                'overall_analysis': {
                    'description': 'Overall analysis uses flag-based detection focusing on specific anomaly types (duplicates, backdated, weekend, holiday, closing entries)',
                    'flagging_criteria': 'Transactions are flagged based on specific anomaly detection rules',
                    'risk_assessment': 'Simple risk scoring based on flagged transaction patterns'
                },
                'risk_analysis': {
                    'description': 'Risk analysis uses comprehensive ML-based scoring considering multiple risk factors',
                    'scoring_methodology': 'Advanced risk scoring using machine learning models and statistical analysis',
                    'risk_factors': 'Considers duplicate risk, backdated risk, user anomalies, unusual days, closing entries, and holiday postings',
                    'version': '2.0.0'
                },
                'data_discrepancy_explanation': 'Overall analysis and risk analysis use different methodologies, which may result in different transaction counts. Overall analysis focuses on specific anomaly flags, while risk analysis provides comprehensive risk scoring.'
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error in FileAnalysisStatisticsView for file {file_id}: {e}")
            return Response(
                {
                    'error': 'Internal server error occurred while processing analysis statistics',
                    'error_code': 'INTERNAL_ERROR',
                    'details': str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_risk_level(self, risk_score):
        """Get risk level based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        elif risk_score >= 20:
            return 'LOW'
        else:
            return 'VERY_LOW'

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
    - Summary statistics and risk assessment
    - Detailed duplicate entries with transaction details
    - Duplicate patterns and analysis breakdowns
    - Audit recommendations and compliance assessment
    - Chart data for visualizations
    - Export-ready data
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
                    'overall_risk_score': self._calculate_overall_risk_score(duplicate_analysis),
                    'overall_risk_level': self._calculate_overall_risk_level(duplicate_analysis),
                    'compliance_issues': duplicate_analysis.get_compliance_issues(),
                    'high_priority_recommendations': duplicate_analysis.get_high_priority_recommendations()
                },
                'detailed_results': {
                    'duplicate_entries': self._enhance_duplicate_entries(duplicate_analysis.duplicate_list or []),
                    'duplicate_patterns': self._generate_duplicate_patterns(duplicate_analysis),
                    'duplicate_by_type': self._generate_duplicate_by_type(duplicate_analysis),
                    'duplicate_by_user': self._generate_duplicate_by_user(duplicate_analysis),
                    'duplicate_by_account': self._generate_duplicate_by_account(duplicate_analysis),
                    'duplicate_by_amount_range': self._generate_duplicate_by_amount_range(duplicate_analysis),
                    'audit_recommendations': self._generate_audit_recommendations(duplicate_analysis),
                    'detection_methods': ['business_rules', 'comprehensive_analysis', 'risk_scoring'],
                    'confidence_scores': self._generate_confidence_scores(duplicate_analysis),
                    'false_positive_indicators': self._generate_false_positive_indicators(duplicate_analysis),
                    'risk_assessment': self._generate_risk_assessment(duplicate_analysis),
                    'compliance_analysis': self._generate_compliance_analysis(duplicate_analysis)
                },
                'visualizations': {
                    'chart_data': duplicate_analysis.chart_data or {},
                    'slicer_filters': {
                        'risk_levels': ['low', 'medium', 'high', 'critical'],
                        'duplicate_types': self._get_duplicate_type_options(),
                        'amount_ranges': ['0-1000', '1000-10000', '10000-100000', '100000+'],
                        'users': self._extract_unique_users(duplicate_analysis.duplicate_list or []),
                        'accounts': self._extract_unique_accounts(duplicate_analysis.duplicate_list or [])
                    }
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
            
            # Add duplicate activity summary if available
            duplicate_activity_summary = self._generate_duplicate_activity_summary(duplicate_analysis)
            if duplicate_activity_summary:
                response_data['duplicate_activity_summary'] = duplicate_activity_summary
            
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
                # Use inferred duplicate type if original is empty or unknown
                duplicate_type = duplicate.get('duplicate_type', '')
                if not duplicate_type or duplicate_type == 'unknown':
                    duplicate_type = self._infer_duplicate_type(duplicate)
                else:
                    # Convert type code to descriptive name
                    duplicate_type = self._get_duplicate_type_description(duplicate_type)
                
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
    
    def _generate_duplicate_patterns(self, duplicate_analysis):
        """Generate duplicate patterns analysis"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        patterns = {
            'duplicate_distribution': {},
            'user_activity_patterns': {},
            'amount_patterns': {},
            'temporal_patterns': {},
            'account_patterns': {}
        }
        
        # Duplicate distribution by type
        duplicate_counts = {}
        for duplicate in duplicate_analysis.duplicate_list:
            duplicate_type = duplicate.get('duplicate_type', 'Unknown')
            duplicate_counts[duplicate_type] = duplicate_counts.get(duplicate_type, 0) + 1
        
        patterns['duplicate_distribution'] = duplicate_counts
        
        # User activity patterns
        user_activity = {}
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            
            for transaction in [transaction1, transaction2]:
                user = transaction.get('user', 'Unknown')
                if user not in user_activity:
                    user_activity[user] = {'count': 0, 'total_amount': 0}
                user_activity[user]['count'] += 1
                user_activity[user]['total_amount'] += transaction.get('amount', 0)
        
        patterns['user_activity_patterns'] = user_activity
        
        # Amount patterns
        amounts = []
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            amounts.append(transaction1.get('amount', 0))
        
        patterns['amount_patterns'] = {
            'min_amount': min(amounts) if amounts else 0,
            'max_amount': max(amounts) if amounts else 0,
            'avg_amount': sum(amounts) / len(amounts) if amounts else 0,
            'high_value_count': len([a for a in amounts if a > 1000000])
        }
        
        # Account patterns
        account_activity = {}
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            account = transaction1.get('account', 'Unknown')
            if account not in account_activity:
                account_activity[account] = {'count': 0, 'total_amount': 0}
            account_activity[account]['count'] += 1
            account_activity[account]['total_amount'] += transaction1.get('amount', 0)
        
        patterns['account_patterns'] = account_activity
        
        return patterns
    
    def _generate_duplicate_by_type(self, duplicate_analysis):
        """Generate duplicate analysis by type"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        by_type = {}
        for duplicate in duplicate_analysis.duplicate_list:
            duplicate_type = duplicate.get('duplicate_type', 'Unknown')
            # Map type codes to descriptive names
            if duplicate_type.startswith('type_'):
                duplicate_type = self._get_duplicate_type_description(duplicate_type)
            elif duplicate_type == 'Unknown':
                # Try to infer duplicate type from matching fields
                duplicate_type = self._infer_duplicate_type(duplicate)
            
            if duplicate_type not in by_type:
                by_type[duplicate_type] = []
            by_type[duplicate_type].append(duplicate)
        
        return by_type
    
    def _generate_duplicate_by_user(self, duplicate_analysis):
        """Generate duplicate analysis by user"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        by_user = {}
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            
            for transaction in [transaction1, transaction2]:
                user = transaction.get('user', 'Unknown')
                if user not in by_user:
                    by_user[user] = []
                by_user[user].append(duplicate)
        
        return by_user
    
    def _generate_duplicate_by_account(self, duplicate_analysis):
        """Generate duplicate analysis by account"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        by_account = {}
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            account = transaction1.get('account', 'Unknown')
            if account not in by_account:
                by_account[account] = []
            by_account[account].append(duplicate)
        
        return by_account
    
    def _generate_duplicate_by_amount_range(self, duplicate_analysis):
        """Generate duplicate analysis by amount range"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        by_amount_range = {
            '0-1000': [],
            '1000-10000': [],
            '10000-100000': [],
            '100000-1000000': [],
            '1000000+': []
        }
        
        for duplicate in duplicate_analysis.duplicate_list:
            transaction1 = duplicate.get('transaction1', {})
            amount = transaction1.get('amount', 0)
            
            if amount <= 1000:
                by_amount_range['0-1000'].append(duplicate)
            elif amount <= 10000:
                by_amount_range['1000-10000'].append(duplicate)
            elif amount <= 100000:
                by_amount_range['10000-100000'].append(duplicate)
            elif amount <= 1000000:
                by_amount_range['100000-1000000'].append(duplicate)
            else:
                by_amount_range['1000000+'].append(duplicate)
        
        return by_amount_range
    
    def _generate_audit_recommendations(self, duplicate_analysis):
        """Generate audit recommendations for duplicate analysis"""
        if not duplicate_analysis.duplicate_list:
            return []
        
        recommendations = [
            "Review duplicate transactions for potential errors",
            "Verify if duplicates are intentional or data entry errors",
            "Check for systematic duplicate patterns",
            "Assess financial statement impact of duplicates",
            "Review internal controls for duplicate prevention"
        ]
        
        # Add specific recommendations based on analysis
        high_value_count = len([d for d in duplicate_analysis.duplicate_list 
                              if d.get('transaction1', {}).get('amount', 0) > 1000000])
        
        if high_value_count > 0:
            recommendations.append(f"Prioritize review of {high_value_count} high-value duplicate transactions")
        
        return recommendations
    
    def _generate_risk_assessment(self, duplicate_analysis):
        """Generate comprehensive risk assessment for duplicate analysis"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        risk_assessment = {
            'overall_risk_level': 'LOW',
            'risk_factors': [],
            'risk_score': 0,
            'risk_distribution': {
                'low_risk': 0,
                'medium_risk': 0,
                'high_risk': 0,
                'critical_risk': 0
            }
        }
        
        total_amount = sum(d.get('transaction1', {}).get('amount', 0) for d in duplicate_analysis.duplicate_list)
        high_value_count = len([d for d in duplicate_analysis.duplicate_list 
                              if d.get('transaction1', {}).get('amount', 0) > 1000000])
        
        # Calculate risk score
        risk_score = 0
        if high_value_count > 0:
            risk_score += 40
        if total_amount > 1000000:
            risk_score += 30
        if len(duplicate_analysis.duplicate_list) > 10:
            risk_score += 20
        if len(duplicate_analysis.duplicate_list) > 50:
            risk_score += 10
        
        risk_assessment['risk_score'] = risk_score
        
        # Determine risk level
        if risk_score >= 80:
            risk_assessment['overall_risk_level'] = 'CRITICAL'
        elif risk_score >= 60:
            risk_assessment['overall_risk_level'] = 'HIGH'
        elif risk_score >= 40:
            risk_assessment['overall_risk_level'] = 'MEDIUM'
        else:
            risk_assessment['overall_risk_level'] = 'LOW'
        
        # Risk factors
        if high_value_count > 0:
            risk_assessment['risk_factors'].append({
                'factor': 'High-value duplicate transactions',
                'count': high_value_count,
                'impact': 'HIGH'
            })
        
        if total_amount > 1000000:
            risk_assessment['risk_factors'].append({
                'factor': 'High total duplicate amount',
                'amount': total_amount,
                'impact': 'HIGH'
            })
        
        return risk_assessment
    
    def _generate_compliance_analysis(self, duplicate_analysis):
        """Generate compliance analysis for duplicate analysis"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        compliance_analysis = {
            'compliance_issues': [],
            'regulatory_concerns': [],
            'internal_control_gaps': [],
            'recommendations': []
        }
        
        # Check for compliance issues
        high_value_duplicates = [d for d in duplicate_analysis.duplicate_list 
                               if d.get('transaction1', {}).get('amount', 0) > 1000000]
        
        if high_value_duplicates:
            compliance_analysis['compliance_issues'].append({
                'issue': 'High-value duplicate transactions',
                'count': len(high_value_duplicates),
                'severity': 'HIGH',
                'description': 'Large amount duplicates may indicate control weaknesses'
            })
        
        # Add recommendations
        compliance_analysis['recommendations'].extend([
            'Implement duplicate detection controls',
            'Review and strengthen approval processes',
            'Consider automated duplicate prevention',
            'Enhance monitoring and reporting'
        ])
        
        return compliance_analysis
    
    def _generate_duplicate_activity_summary(self, duplicate_analysis):
        """Generate summary of duplicate activity patterns"""
        if not duplicate_analysis.duplicate_list:
            return {}
        
        summary = {
            'total_duplicates': len(duplicate_analysis.duplicate_list),
            'unique_users_involved': len(set(
                d.get('transaction1', {}).get('user', '') for d in duplicate_analysis.duplicate_list
            )),
            'unique_accounts_involved': len(set(
                d.get('transaction1', {}).get('account', '') for d in duplicate_analysis.duplicate_list
            )),
            'total_duplicate_amount': sum(
                d.get('transaction1', {}).get('amount', 0) for d in duplicate_analysis.duplicate_list
            ),
            'high_value_duplicates': len([
                d for d in duplicate_analysis.duplicate_list 
                if d.get('transaction1', {}).get('amount', 0) > 1000000
            ]),
            'duplicate_activity_breakdown': {}
        }
        
        # Activity breakdown by user
        user_breakdown = {}
        for duplicate in duplicate_analysis.duplicate_list:
            user = duplicate.get('transaction1', {}).get('user', 'Unknown')
            if user not in user_breakdown:
                user_breakdown[user] = {'count': 0, 'total_amount': 0}
            user_breakdown[user]['count'] += 1
            user_breakdown[user]['total_amount'] += duplicate.get('transaction1', {}).get('amount', 0)
        
        summary['duplicate_activity_breakdown']['by_user'] = user_breakdown
        
        return summary
    
    def _calculate_overall_risk_score(self, duplicate_analysis):
        """Calculate overall risk score for duplicate analysis"""
        if not duplicate_analysis.duplicate_list:
            return 0
        
        total_amount = sum(d.get('transaction1', {}).get('amount', 0) for d in duplicate_analysis.duplicate_list)
        high_value_count = len([d for d in duplicate_analysis.duplicate_list 
                              if d.get('transaction1', {}).get('amount', 0) > 1000000])
        
        # Calculate risk score based on multiple factors
        risk_score = 0
        
        # Factor 1: High value duplicates (40 points)
        if high_value_count > 0:
            risk_score += 40
        
        # Factor 2: Total amount (30 points)
        if total_amount > 1000000:
            risk_score += 30
        elif total_amount > 100000:
            risk_score += 20
        elif total_amount > 10000:
            risk_score += 10
        
        # Factor 3: Number of duplicates (20 points)
        duplicate_count = len(duplicate_analysis.duplicate_list)
        if duplicate_count > 50:
            risk_score += 20
        elif duplicate_count > 20:
            risk_score += 15
        elif duplicate_count > 10:
            risk_score += 10
        elif duplicate_count > 5:
            risk_score += 5
        
        # Factor 4: Risk level distribution (10 points)
        risk_distribution = duplicate_analysis.get_risk_distribution()
        high_risk_count = risk_distribution.get('High', 0) + risk_distribution.get('Critical', 0)
        if high_risk_count > 0:
            risk_score += 10
        
        return min(risk_score, 100)  # Cap at 100
    
    def _calculate_overall_risk_level(self, duplicate_analysis):
        """Calculate overall risk level for duplicate analysis"""
        risk_score = self._calculate_overall_risk_score(duplicate_analysis)
        
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_duplicate_type_options(self):
        """Get available duplicate type options for filtering"""
        return [
            'Type 1 Duplicate - Account Number + Amount',
            'Type 2 Duplicate - Account Number + Source + Amount', 
            'Type 3 Duplicate - Account Number + User + Amount',
            'Type 4 Duplicate - Account Number + Posted Date + Amount',
            'Type 5 Duplicate - Account Number + Effective Date + Amount',
            'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
        ]
    
    def _get_duplicate_type_description(self, duplicate_type_code):
        """Convert duplicate type code to descriptive name"""
        type_mapping = {
            'type_1': 'Type 1 Duplicate - Account Number + Amount',
            'type_2': 'Type 2 Duplicate - Account Number + Source + Amount',
            'type_3': 'Type 3 Duplicate - Account Number + User + Amount',
            'type_4': 'Type 4 Duplicate - Account Number + Posted Date + Amount',
            'type_5': 'Type 5 Duplicate - Account Number + Effective Date + Amount',
            'type_6': 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
        }
        return type_mapping.get(duplicate_type_code, duplicate_type_code)
    
    def _infer_duplicate_type(self, duplicate):
        """Infer duplicate type from matching fields and transaction data"""
        try:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            matching_fields = duplicate.get('matching_fields', [])
            
            # Check for Type 6: Account + Effective Date + Posted Date + User + Source + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('document_date') == transaction2.get('document_date') and
                transaction1.get('posting_date') == transaction2.get('posting_date') and
                transaction1.get('user_name') == transaction2.get('user_name') and
                transaction1.get('source') == transaction2.get('source') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
            
            # Check for Type 5: Account + Effective Date + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('document_date') == transaction2.get('document_date') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 5 Duplicate - Account Number + Effective Date + Amount'
            
            # Check for Type 4: Account + Posted Date + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('posting_date') == transaction2.get('posting_date') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 4 Duplicate - Account Number + Posted Date + Amount'
            
            # Check for Type 3: Account + User + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('user_name') == transaction2.get('user_name') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 3 Duplicate - Account Number + User + Amount'
            
            # Check for Type 2: Account + Source + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('source') == transaction2.get('source') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 2 Duplicate - Account Number + Source + Amount'
            
            # Check for Type 1: Account + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 1 Duplicate - Account Number + Amount'
            
            # If no specific pattern matches, return based on matching fields
            if 'gl_account' in matching_fields and 'amount' in matching_fields:
                if 'user_name' in matching_fields and 'source' in matching_fields:
                    return 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
                elif 'user_name' in matching_fields:
                    return 'Type 3 Duplicate - Account Number + User + Amount'
                elif 'source' in matching_fields:
                    return 'Type 2 Duplicate - Account Number + Source + Amount'
                else:
                    return 'Type 1 Duplicate - Account Number + Amount'
            
            return 'Unknown Duplicate Type'
            
        except Exception as e:
            return 'Unknown Duplicate Type'
    
    def _enhance_duplicate_entries(self, duplicate_list):
        """Enhance duplicate entries with inferred duplicate type and additional information"""
        if not duplicate_list:
            return []
        
        enhanced_entries = []
        for duplicate in duplicate_list:
            if isinstance(duplicate, dict):
                enhanced_duplicate = duplicate.copy()
                
                # Add duplicate type if missing
                if not enhanced_duplicate.get('duplicate_type'):
                    enhanced_duplicate['duplicate_type'] = self._infer_duplicate_type(duplicate)
                
                # Add duplicate type description
                enhanced_duplicate['duplicate_type_description'] = self._get_duplicate_type_description(
                    enhanced_duplicate.get('duplicate_type', '')
                ) or enhanced_duplicate['duplicate_type']
                
                # Add risk score if missing
                if not enhanced_duplicate.get('risk_score'):
                    enhanced_duplicate['risk_score'] = self._calculate_duplicate_risk_score(duplicate)
                
                # Add risk level if missing
                if not enhanced_duplicate.get('risk_level'):
                    enhanced_duplicate['risk_level'] = self._get_duplicate_risk_level(
                        enhanced_duplicate.get('risk_score', 0)
                    )
                
                # Add similarity score if missing
                if not enhanced_duplicate.get('similarity_score'):
                    enhanced_duplicate['similarity_score'] = self._calculate_similarity_score(duplicate)
                
                enhanced_entries.append(enhanced_duplicate)
            else:
                enhanced_entries.append(duplicate)
        
        return enhanced_entries
    
    def _calculate_duplicate_risk_score(self, duplicate):
        """Calculate risk score for a duplicate entry"""
        try:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            
            # Base risk score
            risk_score = 50.0
            
            # Factor 1: High value transactions (30 points)
            amount1 = float(transaction1.get('amount', 0))
            amount2 = float(transaction2.get('amount', 0))
            if amount1 > 1000000 or amount2 > 1000000:
                risk_score += 30
            
            # Factor 2: Same user (10 points)
            if transaction1.get('user') == transaction2.get('user'):
                risk_score += 10
            
            # Factor 3: Same account (5 points)
            if transaction1.get('account') == transaction2.get('account'):
                risk_score += 5
            
            # Factor 4: Same posting date (5 points)
            if transaction1.get('posting_date') == transaction2.get('posting_date'):
                risk_score += 5
            
            return min(risk_score, 100.0)
        except Exception as e:
            return 50.0
    
    def _get_duplicate_risk_level(self, risk_score):
        """Get risk level based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _calculate_similarity_score(self, duplicate):
        """Calculate similarity score for a duplicate entry"""
        try:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            
            matching_fields = 0
            total_fields = 6  # account, amount, user, posting_date, document_number, source
            
            # Check each field for similarity
            if transaction1.get('account') == transaction2.get('account'):
                matching_fields += 1
            if transaction1.get('amount') == transaction2.get('amount'):
                matching_fields += 1
            if transaction1.get('user') == transaction2.get('user'):
                matching_fields += 1
            if transaction1.get('posting_date') == transaction2.get('posting_date'):
                matching_fields += 1
            if transaction1.get('document_number') == transaction2.get('document_number'):
                matching_fields += 1
            if transaction1.get('source') == transaction2.get('source'):
                matching_fields += 1
            
            return matching_fields / total_fields
        except Exception as e:
            return 0.5

class UserAnalysisView(generics.GenericAPIView):
    """Enhanced API view for retrieving comprehensive user analysis results by file ID.
    
    This view provides comprehensive user analysis results including:
    - User transaction summaries and patterns
    - Enhanced user anomaly detection with ML integration
    - Comprehensive user risk assessment and scoring
    - Advanced chart data for visualizations
    - Export-ready data with multiple formats
    - Audit recommendations and compliance assessment
    - Financial statement impact analysis
    """
    
    def get(self, request, file_id):
        """Get comprehensive user analysis results for a specific file"""
        try:
            # Validate file exists
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
                        'Check if user analysis processing completed successfully',
                        'Verify the analysis status is COMPLETED'
                    ]
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Prepare comprehensive response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(user_analysis.id),
                    'analysis_date': user_analysis.analysis_date.isoformat(),
                    'analysis_type': user_analysis.analysis_type,
                    'analysis_version': user_analysis.analysis_version,
                    'processing_duration': user_analysis.processing_duration,
                    'status': user_analysis.status,
                    'file_info': {
                        'file_name': data_file.file_name,
                        'file_id': str(data_file.id),
                        'uploaded_at': data_file.uploaded_at.isoformat(),
                        'total_records': data_file.total_records
                    }
                },
                'summary': {
                    'total_users': user_analysis.get_total_users(),
                    'total_transactions': user_analysis.get_total_transactions(),
                    'anomalies_detected': user_analysis.get_anomalies_count(),
                    'high_risk_users': user_analysis.get_high_risk_users_count(),
                    'risk_distribution': self._get_risk_distribution(user_analysis.user_risk_assessment),
                    'anomaly_distribution': self._get_anomaly_distribution(user_analysis.user_anomalies),
                    'processing_summary': {
                        'analysis_duration': user_analysis.processing_duration,
                        'analysis_date': user_analysis.analysis_date.isoformat(),
                        'analysis_version': user_analysis.analysis_version
                    }
                },
                'user_analysis': {
                    'user_transaction_summary': user_analysis.user_transaction_summary or [],
                    'user_debit_analysis': user_analysis.user_debit_analysis or [],
                    'user_account_distribution': user_analysis.user_account_distribution or [],
                    'user_fs_line_distribution': user_analysis.user_fs_line_distribution or [],
                    'user_patterns': user_analysis.user_patterns or {}
                },
                'anomaly_detection': {
                    'user_anomalies': user_analysis.user_anomalies or [],
                    'anomaly_types': self._get_anomaly_type_summary(user_analysis.user_anomalies),
                    'ml_detected_anomalies': self._get_ml_anomalies(user_analysis.user_anomalies),
                    'anomaly_severity_breakdown': self._get_anomaly_severity_breakdown(user_analysis.user_anomalies),
                    'anomaly_trends': self._extract_anomaly_trends(user_analysis.user_anomalies)
                },
                'risk_assessment': {
                    'user_risk_scores': self._format_user_risk_assessment(user_analysis.user_risk_assessment),
                    'high_risk_users': self._get_high_risk_users(user_analysis.user_risk_assessment),
                    'risk_factors': self._extract_risk_factors(user_analysis.user_risk_assessment),
                    'risk_trends': self._extract_risk_trends(user_analysis.user_risk_assessment),
                    'risk_calculations': self._get_risk_calculations(user_analysis.user_risk_assessment)
                },
                'patterns': {
                    'user_patterns': user_analysis.user_patterns or {},
                    'activity_trends': self._extract_activity_trends(user_analysis.user_transaction_summary),
                    'user_behavior_analysis': self._analyze_user_behavior(user_analysis.user_transaction_summary),
                    'temporal_patterns': self._extract_temporal_patterns(user_analysis.user_transaction_summary)
                },
                'visualizations': {
                    'chart_data': user_analysis.chart_data or {},
                    'chart_types': [
                        'user_transaction_distribution',
                        'user_risk_distribution',
                        'anomaly_distribution',
                        'user_activity_trends',
                        'risk_score_distribution',
                        'user_account_distribution',
                        'user_fs_line_distribution'
                    ],
                    'chart_configurations': self._get_chart_configurations()
                },
                'audit_recommendations': {
                    'high_priority_recommendations': self._generate_high_priority_recommendations(user_analysis),
                    'medium_priority_recommendations': self._generate_medium_priority_recommendations(user_analysis),
                    'low_priority_recommendations': self._generate_low_priority_recommendations(user_analysis),
                    'compliance_issues': self._identify_compliance_issues(user_analysis),
                    'follow_up_actions': self._generate_follow_up_actions(user_analysis)
                },
                'compliance_assessment': {
                    'overall_compliance_score': self._calculate_compliance_score(user_analysis),
                    'compliance_risks': self._identify_compliance_risks(user_analysis),
                    'regulatory_implications': self._assess_regulatory_implications(user_analysis),
                    'internal_control_assessment': self._assess_internal_controls(user_analysis)
                },
                'financial_statement_impact': {
                    'material_impact_assessment': self._assess_material_impact(user_analysis),
                    'financial_statement_risks': self._identify_fs_risks(user_analysis),
                    'disclosure_requirements': self._assess_disclosure_requirements(user_analysis),
                    'audit_implications': self._assess_audit_implications(user_analysis)
                },
                'export_data': {
                    'user_summary': self._prepare_user_summary_export(user_analysis.user_transaction_summary),
                    'anomaly_export': self._prepare_anomaly_export(user_analysis.user_anomalies),
                    'risk_export': self._prepare_risk_export(user_analysis.user_risk_assessment),
                    'patterns_export': self._prepare_patterns_export(user_analysis.user_patterns),
                    'recommendations_export': self._prepare_recommendations_export(user_analysis)
                }
            }
            
            # Log successful retrieval
            user_count = user_analysis.get_total_users()
            anomaly_count = user_analysis.get_anomalies_count()
            high_risk_count = user_analysis.get_high_risk_users_count()
            logger.info(f"Successfully retrieved enhanced user analysis for file {file_id} with {user_count} users, {anomaly_count} anomalies, and {high_risk_count} high-risk users")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving user analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving user analysis',
                'error_code': 'USER_ANALYSIS_ERROR',
                'details': str(e),
                'suggestions': [
                    'Check if the analysis was completed successfully',
                    'Verify the file exists and has been processed',
                    'Contact system administrator if the issue persists'
                ]
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _get_risk_distribution(self, user_risk_assessment):
        """Get comprehensive risk distribution from user risk assessment"""
        if not user_risk_assessment:
            return {'low': 0, 'medium': 0, 'high': 0, 'critical': 0, 'total': 0}
        
        risk_distribution = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0, 'total': 0}
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user_risk in user_risk_assessment:
                if isinstance(user_risk, dict):
                    risk_level = user_risk.get('risk_level', 'low').lower()
                    if risk_level in risk_distribution:
                        risk_distribution[risk_level] += 1
                    risk_distribution['total'] += 1
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            if isinstance(user_risk_scores, list):
                for user_risk in user_risk_scores:
                    if isinstance(user_risk, dict):
                        risk_level = user_risk.get('risk_level', 'low').lower()
                        if risk_level in risk_distribution:
                            risk_distribution[risk_level] += 1
                        risk_distribution['total'] += 1
        
        return risk_distribution
    
    def _get_anomaly_distribution(self, user_anomalies):
        """Get anomaly distribution summary"""
        if not user_anomalies or not isinstance(user_anomalies, list):
            return {'total': 0, 'by_type': {}, 'by_severity': {}}
        
        distribution = {
            'total': len(user_anomalies),
            'by_type': {},
            'by_severity': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        }
        
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                # Count by type
                anomaly_type = anomaly.get('anomaly_type', 'unknown')
                distribution['by_type'][anomaly_type] = distribution['by_type'].get(anomaly_type, 0) + 1
                
                # Count by severity
                severity = anomaly.get('severity', 'medium').lower()
                if severity in distribution['by_severity']:
                    distribution['by_severity'][severity] += 1
        
        return distribution
    
    def _get_anomaly_type_summary(self, anomalies):
        """Get comprehensive summary of anomaly types"""
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
                    'severity_breakdown': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0},
                    'users_affected': set(),
                    'total_amount': 0.0,
                    'avg_risk_score': 0.0
                }
            
            anomaly_types[anomaly_type]['count'] += 1
            severity = anomaly.get('severity', 'medium')
            if severity in anomaly_types[anomaly_type]['severity_breakdown']:
                anomaly_types[anomaly_type]['severity_breakdown'][severity] += 1
            
            # Track users affected
            user = anomaly.get('user', 'unknown')
            anomaly_types[anomaly_type]['users_affected'].add(user)
            
            # Track amounts and risk scores
            amount = anomaly.get('amount', 0.0)
            anomaly_types[anomaly_type]['total_amount'] += amount
            risk_score = anomaly.get('risk_score', 0.0)
            anomaly_types[anomaly_type]['avg_risk_score'] += risk_score
        
        # Convert sets to lists and calculate averages
        for anomaly_type in anomaly_types:
            anomaly_types[anomaly_type]['users_affected'] = list(anomaly_types[anomaly_type]['users_affected'])
            if anomaly_types[anomaly_type]['count'] > 0:
                anomaly_types[anomaly_type]['avg_risk_score'] /= anomaly_types[anomaly_type]['count']
        
        return anomaly_types
    
    def _get_anomaly_severity_breakdown(self, anomalies):
        """Get detailed breakdown of anomalies by severity"""
        if not anomalies or not isinstance(anomalies, list):
            return {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        
        severity_breakdown = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        
        for anomaly in anomalies:
            if isinstance(anomaly, dict):
                severity = anomaly.get('severity', 'medium').lower()
                if severity in severity_breakdown:
                    severity_breakdown[severity] += 1
        
        return severity_breakdown
    
    def _extract_anomaly_trends(self, anomalies):
        """Extract trends from user anomalies"""
        if not anomalies or not isinstance(anomalies, list):
            return {}
        
        trends = {
            'most_common_types': [],
            'highest_risk_anomalies': [],
            'users_with_most_anomalies': {}
        }
        
        # Count anomaly types
        type_counts = {}
        user_anomaly_counts = {}
        
        for anomaly in anomalies:
            if isinstance(anomaly, dict):
                # Count by type
                anomaly_type = anomaly.get('anomaly_type', 'unknown')
                type_counts[anomaly_type] = type_counts.get(anomaly_type, 0) + 1
                
                # Count by user
                user = anomaly.get('user', 'unknown')
                user_anomaly_counts[user] = user_anomaly_counts.get(user, 0) + 1
        
        # Get most common types
        trends['most_common_types'] = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Get users with most anomalies
        trends['users_with_most_anomalies'] = dict(sorted(user_anomaly_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        
        return trends
    
    def _get_ml_anomalies(self, anomalies):
        """Get ML-detected anomalies"""
        if not anomalies or not isinstance(anomalies, list):
            return []
        return [anomaly for anomaly in anomalies if isinstance(anomaly, dict) and anomaly.get('ml_detected', False)]
    
    def _format_user_risk_assessment(self, user_risk_assessment):
        """Format user risk assessment for consistent response"""
        if not user_risk_assessment:
            return []
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            return user_risk_assessment
        
        # Handle dictionary-based user risk assessment (legacy)
        if isinstance(user_risk_assessment, dict):
            return user_risk_assessment.get('user_risk_scores', [])
        
        return []
    
    def _get_high_risk_users(self, user_risk_assessment):
        """Get high risk users from user risk assessment"""
        if not user_risk_assessment:
            return []
        
        high_risk_users = []
        
        # Handle list-based user risk assessment
        if isinstance(user_risk_assessment, list):
            for user_risk in user_risk_assessment:
                if isinstance(user_risk, dict) and user_risk.get('risk_level', 'low').lower() in ['high', 'critical']:
                    high_risk_users.append(user_risk)
        
        # Handle dictionary-based user risk assessment (legacy)
        elif isinstance(user_risk_assessment, dict):
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            if isinstance(user_risk_scores, list):
                for user_risk in user_risk_scores:
                    if isinstance(user_risk, dict) and user_risk.get('risk_level', 'low').lower() in ['high', 'critical']:
                        high_risk_users.append(user_risk)
        
        return high_risk_users
    
    def _extract_risk_factors(self, user_risk_assessment):
        """Extract key risk factors from user risk assessment"""
        risk_factors = []
        
        if not user_risk_assessment:
            return risk_factors
        
        # High risk users
        high_risk_users = self._get_high_risk_users(user_risk_assessment)
        if high_risk_users:
            risk_factors.append({
                'factor': 'High-risk users',
                'count': len(high_risk_users),
                'impact': 'HIGH',
                'description': f'{len(high_risk_users)} users identified with high or critical risk levels'
            })
        
        # Users with high risk scores
        formatted_assessment = self._format_user_risk_assessment(user_risk_assessment)
        high_score_users = [user for user in formatted_assessment if isinstance(user, dict) and user.get('risk_score', 0) > 70]
        if high_score_users:
            risk_factors.append({
                'factor': 'Users with high risk scores',
                'count': len(high_score_users),
                'impact': 'MEDIUM',
                'description': f'{len(high_score_users)} users have risk scores above 70'
            })
        
        return risk_factors
    
    def _extract_risk_trends(self, user_risk_assessment):
        """Extract risk trends from user risk assessment"""
        if not user_risk_assessment:
            return {}
        
        trends = {
            'risk_score_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0},
            'average_risk_score': 0.0,
            'highest_risk_users': []
        }
        
        formatted_assessment = self._format_user_risk_assessment(user_risk_assessment)
        
        if formatted_assessment:
            total_score = 0
            for user_risk in formatted_assessment:
                if isinstance(user_risk, dict):
                    risk_level = user_risk.get('risk_level', 'low').lower()
                    if risk_level in trends['risk_score_distribution']:
                        trends['risk_score_distribution'][risk_level] += 1
                    
                    risk_score = user_risk.get('risk_score', 0.0)
                    total_score += risk_score
            
            trends['average_risk_score'] = total_score / len(formatted_assessment) if formatted_assessment else 0.0
            
            # Get highest risk users
            trends['highest_risk_users'] = sorted(
                [user for user in formatted_assessment if isinstance(user, dict)],
                key=lambda x: x.get('risk_score', 0),
                reverse=True
            )[:10]
        
        return trends
    
    def _get_risk_calculations(self, user_risk_assessment):
        """Get detailed risk calculations"""
        if not user_risk_assessment:
            return {}
        
        calculations = {
            'total_users': 0,
            'risk_score_ranges': {'0-20': 0, '21-40': 0, '41-60': 0, '61-80': 0, '81-100': 0},
            'risk_level_percentages': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        }
        
        formatted_assessment = self._format_user_risk_assessment(user_risk_assessment)
        
        if formatted_assessment:
            calculations['total_users'] = len(formatted_assessment)
            
            for user_risk in formatted_assessment:
                if isinstance(user_risk, dict):
                    risk_score = user_risk.get('risk_score', 0)
                    risk_level = user_risk.get('risk_level', 'low').lower()
                    
                    # Count by score range
                    if risk_score <= 20:
                        calculations['risk_score_ranges']['0-20'] += 1
                    elif risk_score <= 40:
                        calculations['risk_score_ranges']['21-40'] += 1
                    elif risk_score <= 60:
                        calculations['risk_score_ranges']['41-60'] += 1
                    elif risk_score <= 80:
                        calculations['risk_score_ranges']['61-80'] += 1
                    else:
                        calculations['risk_score_ranges']['81-100'] += 1
                    
                    # Count by risk level
                    if risk_level in calculations['risk_level_percentages']:
                        calculations['risk_level_percentages'][risk_level] += 1
            
            # Calculate percentages
            if calculations['total_users'] > 0:
                for level in calculations['risk_level_percentages']:
                    calculations['risk_level_percentages'][level] = (
                        calculations['risk_level_percentages'][level] / calculations['total_users'] * 100
                    )
        
        return calculations
    
    def _extract_activity_trends(self, user_summary):
        """Extract activity trends from user summary"""
        if not user_summary or not isinstance(user_summary, list):
            return {}
        
        trends = {
            'most_active_users': [],
            'highest_value_users': [],
            'users_by_transaction_count': {'low': 0, 'medium': 0, 'high': 0}
        }
        
        if user_summary:
            # Most active users (by transaction count)
            trends['most_active_users'] = sorted(
                user_summary,
                key=lambda x: x.get('transaction_count', 0),
                reverse=True
            )[:10]
            
            # Highest value users (by total amount)
            trends['highest_value_users'] = sorted(
                user_summary,
                key=lambda x: x.get('total_amount', 0),
                reverse=True
            )[:10]
            
            # Users by transaction count
            for user in user_summary:
                transaction_count = user.get('transaction_count', 0)
                if transaction_count <= 10:
                    trends['users_by_transaction_count']['low'] += 1
                elif transaction_count <= 50:
                    trends['users_by_transaction_count']['medium'] += 1
                else:
                    trends['users_by_transaction_count']['high'] += 1
        
        return trends
    
    def _analyze_user_behavior(self, user_summary):
        """Analyze user behavior patterns"""
        if not user_summary or not isinstance(user_summary, list):
            return {}
        
        behavior = {
            'behavior_patterns': [],
            'unusual_activities': [],
            'user_segments': {'low_activity': 0, 'medium_activity': 0, 'high_activity': 0}
        }
        
        if user_summary:
            for user in user_summary:
                transaction_count = user.get('transaction_count', 0)
                total_amount = user.get('total_amount', 0)
                avg_amount = user.get('avg_amount', 0)
                
                # Segment users by activity level
                if transaction_count <= 5:
                    behavior['user_segments']['low_activity'] += 1
                elif transaction_count <= 20:
                    behavior['user_segments']['medium_activity'] += 1
                else:
                    behavior['user_segments']['high_activity'] += 1
                
                # Identify unusual activities
                if transaction_count > 100 or total_amount > 1000000:
                    behavior['unusual_activities'].append({
                        'user': user.get('user', 'unknown'),
                        'reason': 'High activity or value',
                        'transaction_count': transaction_count,
                        'total_amount': total_amount
                    })
        
        return behavior
    
    def _extract_temporal_patterns(self, user_summary):
        """Extract temporal patterns from user activity"""
        # This would require additional data not currently in user_summary
        # Placeholder for future enhancement
        return {
            'daily_patterns': {},
            'weekly_patterns': {},
            'monthly_patterns': {},
            'seasonal_trends': {}
        }
    
    def _get_chart_configurations(self):
        """Get chart configurations for visualizations"""
        return {
            'user_transaction_distribution': {
                'type': 'bar',
                'title': 'User Transaction Distribution',
                'x_axis': 'Users',
                'y_axis': 'Transaction Count'
            },
            'user_risk_distribution': {
                'type': 'pie',
                'title': 'User Risk Distribution',
                'labels': ['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk']
            },
            'anomaly_distribution': {
                'type': 'doughnut',
                'title': 'Anomaly Distribution by Type',
                'show_percentages': True
            },
            'user_activity_trends': {
                'type': 'line',
                'title': 'User Activity Trends',
                'x_axis': 'Time Period',
                'y_axis': 'Activity Level'
            }
        }
    
    def _generate_high_priority_recommendations(self, user_analysis):
        """Generate high priority audit recommendations"""
        recommendations = []
        
        high_risk_users = self._get_high_risk_users(user_analysis.user_risk_assessment)
        if high_risk_users:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'User Risk Management',
                'recommendation': f'Investigate {len(high_risk_users)} high-risk users immediately',
                'rationale': 'High-risk users pose significant audit and compliance risks',
                'action_items': [
                    'Review all transactions by high-risk users',
                    'Conduct detailed user access reviews',
                    'Implement additional monitoring controls'
                ]
            })
        
        anomalies = user_analysis.user_anomalies or []
        critical_anomalies = [a for a in anomalies if isinstance(a, dict) and a.get('severity') == 'critical']
        if critical_anomalies:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Anomaly Investigation',
                'recommendation': f'Investigate {len(critical_anomalies)} critical anomalies',
                'rationale': 'Critical anomalies require immediate attention',
                'action_items': [
                    'Review each critical anomaly in detail',
                    'Assess potential fraud indicators',
                    'Document findings and remediation actions'
                ]
            })
        
        return recommendations
    
    def _generate_medium_priority_recommendations(self, user_analysis):
        """Generate medium priority audit recommendations"""
        recommendations = []
        
        # Add medium priority recommendations based on analysis results
        total_users = user_analysis.get_total_users()
        if total_users > 100:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'User Management',
                'recommendation': 'Implement user activity monitoring system',
                'rationale': 'Large number of users requires systematic monitoring',
                'action_items': [
                    'Deploy automated monitoring tools',
                    'Establish regular user access reviews',
                    'Implement segregation of duties controls'
                ]
            })
        
        return recommendations
    
    def _generate_low_priority_recommendations(self, user_analysis):
        """Generate low priority audit recommendations"""
        recommendations = []
        
        # Add low priority recommendations for process improvements
        recommendations.append({
            'priority': 'LOW',
            'category': 'Process Improvement',
            'recommendation': 'Establish regular user analysis reviews',
            'rationale': 'Regular reviews help maintain control effectiveness',
            'action_items': [
                'Schedule quarterly user analysis reviews',
                'Update user risk assessment criteria',
                'Document lessons learned and best practices'
            ]
        })
        
        return recommendations
    
    def _identify_compliance_issues(self, user_analysis):
        """Identify compliance issues from user analysis"""
        issues = []
        
        # Check for segregation of duties violations
        users_with_high_activity = []
        user_summary = user_analysis.user_transaction_summary or []
        for user in user_summary:
            if isinstance(user, dict) and user.get('transaction_count', 0) > 50:
                users_with_high_activity.append(user.get('user', 'unknown'))
        
        if len(users_with_high_activity) > 0:
            issues.append({
                'issue_type': 'Segregation of Duties',
                'severity': 'MEDIUM',
                'description': f'{len(users_with_high_activity)} users have excessive transaction activity',
                'affected_users': users_with_high_activity,
                'compliance_impact': 'Potential SOD violations'
            })
        
        return issues
    
    def _generate_follow_up_actions(self, user_analysis):
        """Generate follow-up actions for audit team"""
        actions = []
        
        # Immediate actions
        high_risk_users = self._get_high_risk_users(user_analysis.user_risk_assessment)
        if high_risk_users:
            actions.append({
                'timeline': 'IMMEDIATE',
                'action': 'Investigate high-risk users',
                'responsible_party': 'Audit Team',
                'deadline': 'Within 1 week'
            })
        
        # Short-term actions
        actions.append({
            'timeline': 'SHORT_TERM',
            'action': 'Review and update user access controls',
            'responsible_party': 'IT Security Team',
            'deadline': 'Within 1 month'
        })
        
        # Long-term actions
        actions.append({
            'timeline': 'LONG_TERM',
            'action': 'Implement continuous monitoring system',
            'responsible_party': 'IT Team',
            'deadline': 'Within 3 months'
        })
        
        return actions
    
    def _calculate_compliance_score(self, user_analysis):
        """Calculate overall compliance score"""
        score = 100.0
        
        # Deduct points for high-risk users
        high_risk_users = self._get_high_risk_users(user_analysis.user_risk_assessment)
        if high_risk_users:
            score -= len(high_risk_users) * 5  # 5 points per high-risk user
        
        # Deduct points for anomalies
        anomalies = user_analysis.user_anomalies or []
        if anomalies:
            score -= len(anomalies) * 2  # 2 points per anomaly
        
        return max(0, score)
    
    def _identify_compliance_risks(self, user_analysis):
        """Identify compliance risks"""
        risks = []
        
        # Add compliance risks based on analysis
        risks.append({
            'risk_type': 'User Access Management',
            'risk_level': 'MEDIUM',
            'description': 'Potential segregation of duties violations',
            'mitigation': 'Implement role-based access controls'
        })
        
        return risks
    
    def _assess_regulatory_implications(self, user_analysis):
        """Assess regulatory implications"""
        implications = []
        
        # Add regulatory implications
        implications.append({
            'regulation': 'SOX Compliance',
            'implication': 'User access controls must be documented and tested',
            'impact': 'MEDIUM',
            'action_required': 'Document user access review procedures'
        })
        
        return implications
    
    def _assess_internal_controls(self, user_analysis):
        """Assess internal control effectiveness"""
        assessment = {
            'overall_effectiveness': 'ADEQUATE',
            'control_deficiencies': [],
            'recommendations': []
        }
        
        # Assess based on analysis results
        high_risk_users = self._get_high_risk_users(user_analysis.user_risk_assessment)
        if high_risk_users:
            assessment['control_deficiencies'].append({
                'deficiency': 'Inadequate user access controls',
                'impact': 'HIGH',
                'recommendation': 'Strengthen user access review process'
            })
        
        return assessment
    
    def _assess_material_impact(self, user_analysis):
        """Assess material impact on financial statements"""
        assessment = {
            'materiality_assessment': 'LOW',
            'impact_areas': [],
            'quantified_impact': 0.0
        }
        
        # Assess material impact based on analysis
        user_summary = user_analysis.user_transaction_summary or []
        total_amount = sum(user.get('total_amount', 0) for user in user_summary if isinstance(user, dict))
        
        if total_amount > 1000000:
            assessment['materiality_assessment'] = 'MEDIUM'
            assessment['quantified_impact'] = total_amount
        
        return assessment
    
    def _identify_fs_risks(self, user_analysis):
        """Identify financial statement risks"""
        risks = []
        
        # Add financial statement risks
        risks.append({
            'risk_type': 'User Error',
            'risk_level': 'LOW',
            'description': 'Potential errors due to user activity patterns',
            'mitigation': 'Implement additional review controls'
        })
        
        return risks
    
    def _assess_disclosure_requirements(self, user_analysis):
        """Assess disclosure requirements"""
        disclosures = []
        
        # Add disclosure requirements
        high_risk_users = self._get_high_risk_users(user_analysis.user_risk_assessment)
        if high_risk_users:
            disclosures.append({
                'disclosure_type': 'Internal Control Deficiency',
                'description': 'Significant number of high-risk users identified',
                'required': 'YES',
                'timeline': 'Immediate'
            })
        
        return disclosures
    
    def _assess_audit_implications(self, user_analysis):
        """Assess audit implications"""
        implications = []
        
        # Add audit implications
        implications.append({
            'implication_type': 'Testing Scope',
            'description': 'User analysis results may require expanded testing',
            'impact': 'MEDIUM',
            'action_required': 'Consider additional substantive testing'
        })
        
        return implications
    
    def _prepare_user_summary_export(self, user_summary):
        """Prepare user summary data for export"""
        if not user_summary or not isinstance(user_summary, list):
            return []
        
        export_data = []
        for user in user_summary:
            if isinstance(user, dict):
                export_data.append({
                    'user': user.get('user', ''),
                    'transaction_count': user.get('transaction_count', 0),
                    'total_amount': user.get('total_amount', 0.0),
                    'avg_amount': user.get('avg_amount', 0.0),
                    'accounts': ', '.join(user.get('accounts', []))
                })
        
        return export_data
    
    def _prepare_anomaly_export(self, anomalies):
        """Prepare anomaly data for export"""
        if not anomalies or not isinstance(anomalies, list):
            return []
        
        export_data = []
        for anomaly in anomalies:
            if isinstance(anomaly, dict):
                export_data.append({
                    'user': anomaly.get('user', ''),
                    'anomaly_type': anomaly.get('anomaly_type', ''),
                    'severity': anomaly.get('severity', ''),
                    'risk_score': anomaly.get('risk_score', 0.0),
                    'details': anomaly.get('details', '')
                })
        
        return export_data
    
    def _prepare_risk_export(self, user_risk_assessment):
        """Prepare risk assessment data for export"""
        formatted_assessment = self._format_user_risk_assessment(user_risk_assessment)
        
        export_data = []
        for user_risk in formatted_assessment:
            if isinstance(user_risk, dict):
                export_data.append({
                    'user': user_risk.get('user', ''),
                    'risk_level': user_risk.get('risk_level', ''),
                    'risk_score': user_risk.get('risk_score', 0.0),
                    'risk_factors': ', '.join(user_risk.get('risk_factors', []))
                })
        
        return export_data
    
    def _prepare_patterns_export(self, user_patterns):
        """Prepare user patterns data for export"""
        if not user_patterns or not isinstance(user_patterns, dict):
            return []
        
        export_data = []
        for pattern_type, pattern_data in user_patterns.items():
            if isinstance(pattern_data, list):
                for pattern in pattern_data:
                    if isinstance(pattern, dict):
                        export_data.append({
                            'pattern_type': pattern_type,
                            'pattern_data': pattern
                        })
        
        return export_data
    
    def _prepare_recommendations_export(self, user_analysis):
        """Prepare recommendations data for export"""
        recommendations = []
        
        # Add high priority recommendations
        high_priority = self._generate_high_priority_recommendations(user_analysis)
        for rec in high_priority:
            recommendations.append({
                'priority': rec['priority'],
                'category': rec['category'],
                'recommendation': rec['recommendation'],
                'rationale': rec['rationale']
            })
        
        # Add medium priority recommendations
        medium_priority = self._generate_medium_priority_recommendations(user_analysis)
        for rec in medium_priority:
            recommendations.append({
                'priority': rec['priority'],
                'category': rec['category'],
                'recommendation': rec['recommendation'],
                'rationale': rec['rationale']
            })
        
        return recommendations

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
    
    def _generate_backdated_chart_data_from_entries(self, backdated_entries, total_transactions):
        """
        Generate comprehensive chart data for backdated analysis from entries data
        
        Args:
            backdated_entries: List of backdated entry dictionaries
            total_transactions: Total number of transactions for context
            
        Returns:
            dict: Chart data for various visualizations
        """
        if not backdated_entries:
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
                count = len([entry for entry in backdated_entries if entry.get('days_difference', 0) >= min_days])
            else:
                count = len([entry for entry in backdated_entries if min_days <= entry.get('days_difference', 0) < max_days])
            
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
            count = len([entry for entry in backdated_entries if entry.get('risk_level') == level])
            risk_level_data.append(count)
        
        risk_level_chart = {
            'labels': risk_levels,
            'data': risk_level_data,
            'colors': risk_colors
        }
        
        # 3. Backdated Activity by User
        user_activity = {}
        for entry in backdated_entries:
            user = entry.get('user', 'UNKNOWN')
            if user not in user_activity:
                user_activity[user] = {
                    'count': 0,
                    'total_amount': 0.0,
                    'avg_days_difference': 0.0
                }
            user_activity[user]['count'] += 1
            user_activity[user]['total_amount'] += float(entry.get('amount', 0))
            user_activity[user]['avg_days_difference'] += entry.get('days_difference', 0)
        
        # Calculate averages and get top 10 users
        for user_data in user_activity.values():
            if user_data['count'] > 0:
                user_data['avg_days_difference'] = user_data['avg_days_difference'] / user_data['count']
        
        top_users = sorted(user_activity.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        
        user_activity_chart = {
            'labels': [user for user, _ in top_users],
            'data': [data['count'] for _, data in top_users],
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
                count = len([entry for entry in backdated_entries if float(entry.get('amount', 0)) >= min_amount])
            else:
                count = len([entry for entry in backdated_entries if min_amount <= float(entry.get('amount', 0)) < max_amount])
            
            amount_distribution_data.append(count)
            amount_labels.append(label)
        
        amount_distribution_chart = {
            'labels': amount_labels,
            'data': amount_distribution_data,
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384']
        }
        
        # 5. Financial Statement Line Breakdown (by Account)
        account_data = {}
        for entry in backdated_entries:
            account = entry.get('account', 'UNKNOWN')
            if account not in account_data:
                account_data[account] = {
                    'count': 0,
                    'total_amount': 0.0,
                    'avg_days_difference': 0.0
                }
            account_data[account]['count'] += 1
            account_data[account]['total_amount'] += float(entry.get('amount', 0))
            account_data[account]['avg_days_difference'] += entry.get('days_difference', 0)
        
        # Calculate averages and get top 10 accounts
        for account_info in account_data.values():
            if account_info['count'] > 0:
                account_info['avg_days_difference'] = account_info['avg_days_difference'] / account_info['count']
        
        top_accounts = sorted(account_data.items(), key=lambda x: x[1]['total_amount'], reverse=True)[:10]
        
        gl_account_chart = {
            'labels': [f"GL {account}" for account, _ in top_accounts],
            'data': [data['count'] for _, data in top_accounts],
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # 6. Monthly Backdated Trend
        monthly_data = {}
        for entry in backdated_entries:
            posting_date = entry.get('posting_date', '')
            if posting_date:
                try:
                    from datetime import datetime
                    date_obj = datetime.strptime(posting_date, '%Y-%m-%d')
                    month = date_obj.month
                    if month not in monthly_data:
                        monthly_data[month] = 0
                    monthly_data[month] += 1
                except (ValueError, TypeError):
                    pass
        
        # Create complete month labels
        month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_trend_data = [monthly_data.get(i, 0) for i in range(1, 13)]
        
        monthly_trend_chart = {
            'labels': month_labels,
            'data': monthly_trend_data,
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
            
            # Get backdated entries from the analysis result
            backdated_entries = backdated_analysis.backdated_entries or []
            
            # Calculate statistics from the actual data structure
            total_transactions = backdated_analysis.analysis_info.get('total_transactions', 0)
            backdated_count = len(backdated_entries)
            
            # Calculate risk statistics for backdated transactions
            if backdated_entries:
                risk_scores = [entry.get('risk_level', 'LOW') for entry in backdated_entries]
                amounts = [float(entry.get('amount', 0)) for entry in backdated_entries]
                days_differences = [entry.get('days_difference', 0) for entry in backdated_entries]
                
                # Risk level mapping
                risk_score_mapping = {'LOW': 20, 'MEDIUM': 45, 'HIGH': 70, 'CRITICAL': 90}
                numeric_risk_scores = [risk_score_mapping.get(risk, 20) for risk in risk_scores]
                
                avg_risk_score = sum(numeric_risk_scores) / len(numeric_risk_scores) if numeric_risk_scores else 0
                max_risk_score = max(numeric_risk_scores) if numeric_risk_scores else 0
                min_risk_score = min(numeric_risk_scores) if numeric_risk_scores else 0
                total_backdated_amount = sum(amounts)
                avg_backdated_days = sum(days_differences) / len(days_differences) if days_differences else 0
                max_backdated_days = max(days_differences) if days_differences else 0
            else:
                avg_risk_score = max_risk_score = min_risk_score = total_backdated_amount = avg_backdated_days = max_backdated_days = 0
            
            # Risk level distribution for backdated transactions
            risk_distribution = {
                'low_risk': len([entry for entry in backdated_entries if entry.get('risk_level') == 'LOW']),
                'medium_risk': len([entry for entry in backdated_entries if entry.get('risk_level') == 'MEDIUM']),
                'high_risk': len([entry for entry in backdated_entries if entry.get('risk_level') == 'HIGH']),
                'critical_risk': len([entry for entry in backdated_entries if entry.get('risk_level') == 'CRITICAL'])
            }
            
            # Get high-risk backdated transactions
            high_risk_backdated = [
                {
                    'id': entry.get('transaction_id'),
                    'document_number': entry.get('document_number'),
                    'account': entry.get('account'),
                    'amount': entry.get('amount'),
                    'user': entry.get('user'),
                    'posting_date': entry.get('posting_date'),
                    'document_date': entry.get('document_date'),
                    'risk_level': entry.get('risk_level'),
                    'days_difference': entry.get('days_difference')
                }
                for entry in backdated_entries if entry.get('risk_level') in ['HIGH', 'CRITICAL']
            ][:10]
            
            # Generate comprehensive chart data
            chart_data = self._generate_backdated_chart_data_from_entries(backdated_entries, total_transactions)
            
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

class UnusualDaysAnalysisView(generics.GenericAPIView):
    """API view for retrieving unusual days analysis results by file ID.
    
    This view provides comprehensive unusual days analysis results including:
    - Weekend and holiday postings
    - Unusual day activity patterns
    - Risk assessment for unusual day transactions
    - Chart data for visualizations
    - Export-ready data
    """
    
    def get(self, request, file_id):
        """Get unusual days analysis results for a specific file"""
        try:
            # Get the latest unusual days analysis result
            from .models import UnusualDaysAnalysisResult
            unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not unusual_days_analysis:
                return Response({
                    'error': 'No unusual days analysis results found for this file',
                    'error_code': 'NO_UNUSUAL_DAYS_ANALYSIS_FOUND',
                    'suggestions': [
                        'Run unusual days analysis for this file',
                        'Check if unusual days analysis processing completed successfully'
                    ]
                }, status=404)
            
            # Prepare response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(unusual_days_analysis.id),
                    'analysis_date': unusual_days_analysis.analysis_date,
                    'processing_duration': unusual_days_analysis.processing_duration,
                    'status': unusual_days_analysis.status,
                    'analysis_version': unusual_days_analysis.analysis_version or '1.0.0'
                },
                        'summary': {
            'total_transactions': unusual_days_analysis.analysis_info.get('total_transactions', 0),
            'weekend_postings': len(unusual_days_analysis.weekend_postings or []),
            'unusual_days_detected': len(unusual_days_analysis.unusual_days or []),
            'risk_level': self._calculate_risk_level(unusual_days_analysis),
            'overall_risk_score': self._calculate_overall_risk_score(unusual_days_analysis)
        },
                        'unusual_days_analysis': {
            'day_of_week_activity': self._generate_day_of_week_activity(unusual_days_analysis),
            'user_day_patterns': self._generate_user_day_patterns(unusual_days_analysis),
            'fs_line_day_patterns': unusual_days_analysis.fs_line_day_patterns or []
        },
                        'risk_assessment': {
            'risk_assessment': unusual_days_analysis.risk_assessment or {},
            'weekend_risk_score': self._calculate_weekend_risk_score(unusual_days_analysis),
            'unusual_pattern_risk_score': self._calculate_unusual_pattern_risk_score(unusual_days_analysis),
            'high_value_weekend_risk_score': self._calculate_high_value_weekend_risk_score(unusual_days_analysis),
            'overall_risk_score': self._calculate_overall_risk_score(unusual_days_analysis)
        },
                        'patterns': {
            'day_of_week_activity': self._generate_day_of_week_activity(unusual_days_analysis),
            'user_day_patterns': self._generate_user_day_patterns(unusual_days_analysis),
            'fs_line_day_patterns': unusual_days_analysis.fs_line_day_patterns or []
        },
                'visualizations': {
                    'chart_data': unusual_days_analysis.chart_data or {},
                    'chart_types': [
                        'weekend_activity_chart',
                        'holiday_activity_chart',
                        'day_of_week_activity',
                        'monthly_patterns',
                        'risk_distribution'
                    ]
                },
                # 'export_data': {
                #     'weekend_export': self._prepare_weekend_export(unusual_days_analysis.weekend_postings or []),
                #     'unusual_days_export': self._prepare_unusual_days_export(unusual_days_analysis.unusual_days or []),
                #     'user_patterns_export': self._prepare_user_patterns_export(unusual_days_analysis.user_day_patterns or [])
                # }
            }
            
            # Log successful retrieval
            weekend_count = unusual_days_analysis.get_weekend_transactions_count()
            unusual_days_count = unusual_days_analysis.get_unusual_days_count()
            logger.info(f"Successfully retrieved unusual days analysis for file {file_id} with {weekend_count} weekend and {unusual_days_count} unusual day postings")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving unusual days analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving unusual days analysis',
                'error_code': 'UNUSUAL_DAYS_ANALYSIS_ERROR',
                'details': str(e)
            }, status=500)
    
    def _prepare_weekend_export(self, weekend_postings):
        """Prepare weekend postings data for export"""
        return weekend_postings
    
    def _prepare_unusual_days_export(self, unusual_days):
        """Prepare unusual days data for export"""
        return unusual_days
    
    def _prepare_user_patterns_export(self, user_patterns):
        """Prepare user patterns data for export"""
        return user_patterns
    
    def _calculate_risk_level(self, unusual_days_analysis):
        """Calculate risk level based on unusual days data"""
        unusual_days = unusual_days_analysis.unusual_days or []
        
        total_unusual = len(unusual_days)
        total_transactions = unusual_days_analysis.analysis_info.get('total_transactions', 0)
        
        if total_transactions == 0:
            return 'LOW'
        
        unusual_percentage = (total_unusual / total_transactions) * 100
        
        if unusual_percentage > 10:
            return 'CRITICAL'
        elif unusual_percentage > 5:
            return 'HIGH'
        elif unusual_percentage > 2:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _calculate_overall_risk_score(self, unusual_days_analysis):
        """Calculate overall risk score based on unusual days data"""
        unusual_days = unusual_days_analysis.unusual_days or []
        
        total_unusual = len(unusual_days)
        total_transactions = unusual_days_analysis.analysis_info.get('total_transactions', 0)
        
        if total_transactions == 0:
            return 0.0
        
        unusual_percentage = (total_unusual / total_transactions) * 100
        
        # Calculate risk score based on percentage and high-value transactions
        high_value_count = sum(1 for entry in unusual_days 
                              if float(entry.get('amount', 0)) > 10000000)
        
        base_score = min(unusual_percentage * 10, 100)  # Max 100
        high_value_bonus = min(high_value_count * 5, 50)  # Max 50 bonus
        
        return min(base_score + high_value_bonus, 100)
    
    def _calculate_weekend_risk_score(self, unusual_days_analysis):
        """Calculate weekend risk score"""
        unusual_days = unusual_days_analysis.unusual_days or []
        total_transactions = unusual_days_analysis.analysis_info.get('total_transactions', 0)
        
        if total_transactions == 0:
            return 0.0
        
        # Since all unusual days are weekend postings in this case
        weekend_percentage = (len(unusual_days) / total_transactions) * 100
        high_value_weekend = sum(1 for entry in unusual_days 
                                if float(entry.get('amount', 0)) > 10000000)
        
        base_score = min(weekend_percentage * 15, 100)  # Weekend transactions are higher risk
        high_value_bonus = min(high_value_weekend * 10, 50)
        
        return min(base_score + high_value_bonus, 100)
    
    def _calculate_unusual_pattern_risk_score(self, unusual_days_analysis):
        """Calculate unusual pattern risk score"""
        unusual_days = unusual_days_analysis.unusual_days or []
        total_transactions = unusual_days_analysis.analysis_info.get('total_transactions', 0)
        
        if total_transactions == 0:
            return 0.0
        
        unusual_percentage = (len(unusual_days) / total_transactions) * 100
        high_value_unusual = sum(1 for entry in unusual_days 
                                if float(entry.get('amount', 0)) > 10000000)
        
        base_score = min(unusual_percentage * 12, 100)
        high_value_bonus = min(high_value_unusual * 8, 40)
        
        return min(base_score + high_value_bonus, 100)
    
    def _calculate_high_value_weekend_risk_score(self, unusual_days_analysis):
        """Calculate high value weekend risk score"""
        unusual_days = unusual_days_analysis.unusual_days or []
        total_transactions = unusual_days_analysis.analysis_info.get('total_transactions', 0)
        
        if total_transactions == 0:
            return 0.0
        
        high_value_weekend = sum(1 for entry in unusual_days 
                                if float(entry.get('amount', 0)) > 10000000)
        
        high_value_percentage = (high_value_weekend / total_transactions) * 100
        
        return min(high_value_percentage * 20, 100)  # High value weekend transactions are very high risk
    
    def _generate_day_of_week_activity(self, unusual_days_analysis):
        """Generate day of week activity from actual data"""
        unusual_days = unusual_days_analysis.unusual_days or []
        
        day_activity = {}
        
        for entry in unusual_days:
            day = entry.get('day_of_week', 'Unknown')
            if day not in day_activity:
                day_activity[day] = {
                    'count': 0,
                    'total_amount': 0.0,
                    'average_amount': 0.0,
                    'high_value_count': 0,
                    'high_value_amount': 0.0
                }
            
            amount = float(entry.get('amount', 0))
            day_activity[day]['count'] += 1
            day_activity[day]['total_amount'] += amount
            
            if amount > 10000000:
                day_activity[day]['high_value_count'] += 1
                day_activity[day]['high_value_amount'] += amount
        
        # Calculate averages
        for day_data in day_activity.values():
            if day_data['count'] > 0:
                day_data['average_amount'] = day_data['total_amount'] / day_data['count']
        
        return day_activity
    
    def _generate_user_day_patterns(self, unusual_days_analysis):
        """Generate user day patterns from actual data"""
        unusual_days = unusual_days_analysis.unusual_days or []
        
        user_patterns = {}
        
        for entry in unusual_days:
            user = entry.get('user', 'Unknown')
            day = entry.get('day_of_week', 'Unknown')
            
            if user not in user_patterns:
                user_patterns[user] = {
                    'total_transactions': 0,
                    'total_amount': 0.0,
                    'days_activity': {},
                    'high_value_transactions': 0,
                    'high_value_amount': 0.0
                }
            
            amount = float(entry.get('amount', 0))
            user_patterns[user]['total_transactions'] += 1
            user_patterns[user]['total_amount'] += amount
            
            if day not in user_patterns[user]['days_activity']:
                user_patterns[user]['days_activity'][day] = {
                    'count': 0,
                    'amount': 0.0
                }
            
            user_patterns[user]['days_activity'][day]['count'] += 1
            user_patterns[user]['days_activity'][day]['amount'] += amount
            
            if amount > 10000000:
                user_patterns[user]['high_value_transactions'] += 1
                user_patterns[user]['high_value_amount'] += amount
        
        # Convert to list format and add computed fields
        user_patterns_list = []
        for user, data in user_patterns.items():
            user_data = {
                'user': user,
                'total_transactions': data['total_transactions'],
                'total_amount': data['total_amount'],
                'average_amount': data['total_amount'] / data['total_transactions'] if data['total_transactions'] > 0 else 0,
                'high_value_transactions': data['high_value_transactions'],
                'high_value_amount': data['high_value_amount'],
                'high_value_percentage': (data['high_value_transactions'] / data['total_transactions'] * 100) if data['total_transactions'] > 0 else 0,
                'days_activity': data['days_activity']
            }
            user_patterns_list.append(user_data)
        
        # Sort by total amount descending
        user_patterns_list.sort(key=lambda x: x['total_amount'], reverse=True)
        
        return user_patterns_list


class ClosingEntriesAnalysisView(generics.GenericAPIView):
    """API view for retrieving closing entries analysis results by file ID.
    
    This view provides comprehensive closing entries analysis results including:
    - Month-end closing transactions with detailed analysis
    - Risk assessment and scoring
    - User activity patterns
    - Account-based analysis
    - Temporal patterns and trends
    - Chart data for visualizations
    - Export-ready data
    """
    
    def get(self, request, file_id):
        """Get closing entries analysis results for a specific file"""
        try:
            # Get the latest closing entries analysis result
            from .models import ClosingEntriesAnalysisResult
            closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not closing_entries_analysis:
                return Response({
                    'error': 'No closing entries analysis results found for this file',
                    'error_code': 'NO_CLOSING_ENTRIES_ANALYSIS_FOUND',
                    'suggestions': [
                        'Run closing entries analysis for this file',
                        'Check if closing entries analysis processing completed successfully'
                    ]
                }, status=404)
            
            # Get closing entries data
            closing_entries = closing_entries_analysis.closing_entries or []
            
            # Calculate risk scores from actual data
            total_transactions = closing_entries_analysis.get_total_transactions()
            closing_entries_count = closing_entries_analysis.get_closing_entries_count()
            post_close_entries_count = closing_entries_analysis.get_post_close_entries_count()
            
            # Calculate risk scores from closing entries data
            if closing_entries:
                risk_scores = [entry.get('risk_level', 'LOW') for entry in closing_entries]
                amounts = [float(entry.get('amount', 0)) for entry in closing_entries]
                
                # Risk level mapping
                risk_score_mapping = {'LOW': 20, 'MEDIUM': 45, 'HIGH': 70, 'CRITICAL': 90}
                numeric_risk_scores = [risk_score_mapping.get(risk, 20) for risk in risk_scores]
                
                avg_risk_score = sum(numeric_risk_scores) / len(numeric_risk_scores) if numeric_risk_scores else 0
                total_closing_amount = sum(amounts)
                
                # Calculate risk level based on average risk score
                if avg_risk_score >= 80:
                    risk_level = 'CRITICAL'
                elif avg_risk_score >= 60:
                    risk_level = 'HIGH'
                elif avg_risk_score >= 30:
                    risk_level = 'MEDIUM'
                else:
                    risk_level = 'LOW'
            else:
                avg_risk_score = 0.0
                total_closing_amount = 0.0
                risk_level = 'LOW'
            
            # Prepare comprehensive response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(closing_entries_analysis.id),
                    'analysis_date': closing_entries_analysis.analysis_date,
                    'processing_duration': closing_entries_analysis.processing_duration,
                    'status': closing_entries_analysis.status,
                    'analysis_version': closing_entries_analysis.analysis_version or '1.0.0'
                },
                'summary': {
                    'total_transactions': total_transactions,
                    'closing_entries_count': closing_entries_count,
                    'post_close_entries_count': post_close_entries_count,
                    'risk_level': risk_level,
                    'overall_risk_score': avg_risk_score
                },
                'closing_entries_analysis': {
                    'post_close_analysis': closing_entries_analysis.post_close_analysis or {},
                    'fs_line_closing': closing_entries_analysis.fs_line_closing or {},
                    'user_closing': closing_entries_analysis.user_closing or {},
                    'month_end_patterns': closing_entries_analysis.month_end_patterns or {},
                    'closing_window_analysis': closing_entries_analysis.closing_window_analysis or {}
                },
                'risk_assessment': {
                    'closing_entries_risk': closing_entries_analysis.get_closing_entries_risk_score(),
                    'post_close_risk': closing_entries_analysis.get_post_close_risk_score(),
                    'high_value_post_close_risk': closing_entries_analysis.get_high_value_post_close_risk_score(),
                    'risk_assessment_details': closing_entries_analysis.risk_assessment or {}
                },
                'patterns': {
                    'month_end_patterns': closing_entries_analysis.month_end_patterns or {},
                    'closing_window_analysis': closing_entries_analysis.closing_window_analysis or {},
                    'fs_line_closing': closing_entries_analysis.fs_line_closing or {},
                    'user_closing': closing_entries_analysis.user_closing or {}
                },
                'visualizations': {
                    'chart_data': closing_entries_analysis.chart_data or {},
                    'chart_types': [
                        'closing_entries_timeline',
                        'post_close_activity',
                        'fs_line_closing_distribution',
                        'user_closing_activity',
                        'month_end_patterns',
                        'risk_distribution'
                    ]
                },
                'detailed_analysis': self._generate_detailed_analysis(closing_entries),
                'export_data': {
                    'post_close_export': self._prepare_post_close_export(closing_entries_analysis.post_close_analysis or {}),
                    'fs_line_export': self._prepare_fs_line_export(closing_entries_analysis.fs_line_closing or {})
                }
            }
            
            # Log successful retrieval
            closing_count = closing_entries_analysis.get_closing_entries_count()
            post_close_count = closing_entries_analysis.get_post_close_entries_count()
            logger.info(f"Successfully retrieved closing entries analysis for file {file_id} with {closing_count} closing and {post_close_count} post-close entries")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving closing entries analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving closing entries analysis',
                'error_code': 'CLOSING_ENTRIES_ANALYSIS_ERROR',
                'details': str(e)
            }, status=500)
    
    def _generate_detailed_analysis(self, closing_entries):
        """Generate detailed analysis from closing entries data"""
        if not closing_entries:
            return {}
        
        # Risk distribution analysis
        risk_distribution = {}
        for entry in closing_entries:
            risk_level = entry.get('risk_level', 'UNKNOWN')
            risk_distribution[risk_level] = risk_distribution.get(risk_level, 0) + 1
        
        # User analysis
        user_analysis = {}
        for entry in closing_entries:
            user = entry.get('user', 'UNKNOWN')
            if user not in user_analysis:
                user_analysis[user] = {
                    'total_entries': 0,
                    'total_amount': 0.0,
                    'accounts': set(),
                    'risk_levels': {},
                    'posting_dates': set()
                }
            
            user_analysis[user]['total_entries'] += 1
            user_analysis[user]['total_amount'] += float(entry.get('amount', 0))
            user_analysis[user]['accounts'].add(entry.get('account', ''))
            user_analysis[user]['posting_dates'].add(entry.get('posting_date', ''))
            
            risk_level = entry.get('risk_level', 'UNKNOWN')
            user_analysis[user]['risk_levels'][risk_level] = user_analysis[user]['risk_levels'].get(risk_level, 0) + 1
        
        # Convert sets to lists for JSON serialization
        for user_data in user_analysis.values():
            user_data['accounts'] = list(user_data['accounts'])
            user_data['posting_dates'] = list(user_data['posting_dates'])
        
        # Account analysis
        account_analysis = {}
        for entry in closing_entries:
            account = entry.get('account', 'UNKNOWN')
            if account not in account_analysis:
                account_analysis[account] = {
                    'total_entries': 0,
                    'total_amount': 0.0,
                    'users': set(),
                    'risk_levels': {},
                    'posting_dates': set()
                }
            
            account_analysis[account]['total_entries'] += 1
            account_analysis[account]['total_amount'] += float(entry.get('amount', 0))
            account_analysis[account]['users'].add(entry.get('user', ''))
            account_analysis[account]['posting_dates'].add(entry.get('posting_date', ''))
            
            risk_level = entry.get('risk_level', 'UNKNOWN')
            account_analysis[account]['risk_levels'][risk_level] = account_analysis[account]['risk_levels'].get(risk_level, 0) + 1
        
        # Convert sets to lists for JSON serialization
        for account_data in account_analysis.values():
            account_data['users'] = list(account_data['users'])
            account_data['posting_dates'] = list(account_data['posting_dates'])
        
        # Temporal analysis
        temporal_analysis = {}
        for entry in closing_entries:
            posting_date = entry.get('posting_date', '')
            days_from_month_end = entry.get('days_from_month_end', 0)
            
            if posting_date not in temporal_analysis:
                temporal_analysis[posting_date] = {
                    'total_entries': 0,
                    'total_amount': 0.0,
                    'users': set(),
                    'accounts': set(),
                    'risk_levels': {},
                    'days_from_month_end': days_from_month_end
                }
            
            temporal_analysis[posting_date]['total_entries'] += 1
            temporal_analysis[posting_date]['total_amount'] += float(entry.get('amount', 0))
            temporal_analysis[posting_date]['users'].add(entry.get('user', ''))
            temporal_analysis[posting_date]['accounts'].add(entry.get('account', ''))
            
            risk_level = entry.get('risk_level', 'UNKNOWN')
            temporal_analysis[posting_date]['risk_levels'][risk_level] = temporal_analysis[posting_date]['risk_levels'].get(risk_level, 0) + 1
        
        # Convert sets to lists for JSON serialization
        for date_data in temporal_analysis.values():
            date_data['users'] = list(date_data['users'])
            date_data['accounts'] = list(date_data['accounts'])
        
        # Amount analysis
        amounts = [float(entry.get('amount', 0)) for entry in closing_entries]
        amount_analysis = {
            'total_amount': sum(amounts),
            'average_amount': sum(amounts) / len(amounts) if amounts else 0,
            'min_amount': min(amounts) if amounts else 0,
            'max_amount': max(amounts) if amounts else 0,
            'high_value_entries': len([amt for amt in amounts if amt > 10000000]),  # > 10M
            'medium_value_entries': len([amt for amt in amounts if 1000000 <= amt <= 10000000]),  # 1M-10M
            'low_value_entries': len([amt for amt in amounts if amt < 1000000])  # < 1M
        }
        
        return {
            'risk_distribution': risk_distribution,
            'user_analysis': user_analysis,
            'account_analysis': account_analysis,
            'temporal_analysis': temporal_analysis,
            'amount_analysis': amount_analysis,
            'summary_stats': {
                'total_entries': len(closing_entries),
                'unique_users': len(set(entry.get('user', '') for entry in closing_entries)),
                'unique_accounts': len(set(entry.get('account', '') for entry in closing_entries)),
                'unique_posting_dates': len(set(entry.get('posting_date', '') for entry in closing_entries)),
                'average_days_from_month_end': sum(entry.get('days_from_month_end', 0) for entry in closing_entries) / len(closing_entries) if closing_entries else 0
            }
        }
    
    def _prepare_closing_entries_export(self, closing_entries):
        """Prepare closing entries data for export"""
        return closing_entries
    
    def _prepare_post_close_export(self, post_close_analysis):
        """Prepare post-close analysis data for export"""
        if isinstance(post_close_analysis, dict):
            return post_close_analysis.get('post_close_entries', [])
        return []
    
    def _prepare_fs_line_export(self, fs_line_closing):
        """Prepare financial statement line closing data for export"""
        if isinstance(fs_line_closing, dict):
            return fs_line_closing.get('fs_line_summary', [])
        return []


class HolidayAnalysisView(generics.GenericAPIView):
    """API view for retrieving holiday analysis results by file ID.
    
    This view provides comprehensive holiday analysis results including:
    - Holiday posting detection
    - Holiday activity patterns by FS line, account, and user
    - GL activity by holiday
    - Risk assessment for holiday postings
    - Chart data for visualizations
    """
    
    def get(self, request, file_id):
        """Get holiday analysis results for a specific file"""
        try:
            # Get the latest holiday analysis result
            from .models import HolidayAnalysisResult
            holiday_analysis = HolidayAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not holiday_analysis:
                return Response({
                    'error': 'No holiday analysis results found for this file',
                    'error_code': 'NO_HOLIDAY_ANALYSIS_FOUND',
                    'suggestions': [
                        'Run holiday analysis for this file',
                        'Check if holiday analysis processing completed successfully'
                    ]
                }, status=404)
            
            # Prepare response data
            response_data = {
                'analysis_info': {
                    'analysis_id': str(holiday_analysis.id),
                    'analysis_date': holiday_analysis.analysis_date,
                    'processing_duration': holiday_analysis.processing_duration,
                    'status': holiday_analysis.status,
                    'analysis_version': holiday_analysis.analysis_version or '1.0.0'
                },
                'summary': {
                    'total_holiday_postings': len(holiday_analysis.holiday_postings or []),
                    'holiday_percentage': holiday_analysis.get_holiday_percentage(),
                    'unique_holidays': holiday_analysis.get_unique_holidays(),
                    'country_code': holiday_analysis.analysis_info.get('country_code', 'saudiarabian'),
                    'fiscal_year': holiday_analysis.analysis_info.get('fiscal_year', ''),
                    'risk_level': holiday_analysis.get_risk_level(),
                    'overall_risk_score': holiday_analysis.get_overall_risk_score(),
                    'high_value_holiday_count': len(holiday_analysis.get_high_value_holiday_postings()),
                    'total_holiday_amount': sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings or []),
                    'compliance_issues': self._generate_compliance_issues(holiday_analysis),
                    'high_priority_recommendations': self._generate_high_priority_recommendations(holiday_analysis)
                },
                'detailed_results': {
                    'holiday_postings': holiday_analysis.holiday_postings or [],
                    'holiday_patterns': self._generate_holiday_patterns(holiday_analysis),
                    'holiday_by_fs_line': holiday_analysis.holiday_by_fs_line or [],
                    'holiday_by_account': holiday_analysis.holiday_by_account or [],
                    'holiday_by_user': holiday_analysis.holiday_by_user or [],
                    'gl_activity_by_holiday': holiday_analysis.gl_activity_by_holiday or {},
                    'audit_recommendations': self._generate_audit_recommendations(holiday_analysis),
                    'detection_methods': ['holiday_utils', 'business_rules', 'risk_scoring'],
                    'confidence_scores': self._generate_confidence_scores(holiday_analysis),
                    'false_positive_indicators': self._generate_false_positive_indicators(holiday_analysis),
                    'risk_assessment': self._generate_risk_assessment(holiday_analysis),
                    'compliance_analysis': self._generate_compliance_analysis(holiday_analysis)
                },
                'visualizations': {
                    'chart_data': holiday_analysis.chart_data or {},
                    'slicer_filters': {
                        'risk_levels': ['low', 'medium', 'high', 'critical'],
                        'holiday_types': ['Public holiday', 'Observance', 'Bank holiday', 'National holiday'],
                        'amount_ranges': ['0-1000', '1000-10000', '10000-100000', '100000+'],
                        'users': self._extract_unique_users(holiday_analysis.holiday_postings or []),
                        'accounts': self._extract_unique_accounts(holiday_analysis.holiday_postings or []),
                        'fs_lines': self._extract_unique_fs_lines(holiday_analysis.holiday_postings or [])
                    }
                }
            }
            
            # Add compliance assessment if available
            if holiday_analysis.compliance_assessment:
                response_data['compliance'] = holiday_analysis.compliance_assessment
            
            # Add financial statement impact if available
            if holiday_analysis.financial_statement_impact:
                response_data['financial_impact'] = holiday_analysis.financial_statement_impact
            
            # Add holiday activity summary if available
            holiday_activity_summary = holiday_analysis.get_holiday_activity_summary()
            if holiday_activity_summary:
                response_data['holiday_activity_summary'] = holiday_activity_summary
            
            # Log successful retrieval
            holiday_count = len(holiday_analysis.holiday_postings or [])
            logger.info(f"Successfully retrieved holiday analysis for file {file_id} with {holiday_count} holiday postings")
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error retrieving holiday analysis for file {file_id}: {e}")
            return Response({
                'error': 'Error occurred while retrieving holiday analysis',
                'error_code': 'HOLIDAY_ANALYSIS_ERROR',
                'details': str(e)
            }, status=500)
    
    def _generate_confidence_scores(self, holiday_analysis):
        """Generate confidence scores for holiday detection"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        confidence_scores = {
            'holiday_detection_confidence': 0.95,  # High confidence for holiday_utils
            'risk_assessment_confidence': 0.85,    # Good confidence for risk scoring
            'pattern_detection_confidence': 0.80   # Moderate confidence for pattern analysis
        }
        
        # Adjust confidence based on data quality
        total_postings = len(holiday_analysis.holiday_postings)
        if total_postings > 0:
            # Higher confidence with more data
            confidence_scores['holiday_detection_confidence'] = min(0.98, 0.95 + (total_postings * 0.001))
        
        return confidence_scores
    
    def _generate_false_positive_indicators(self, holiday_analysis):
        """Generate false positive indicators for holiday analysis"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        indicators = {
            'potential_false_positives': [],
            'confidence_factors': [],
            'verification_required': []
        }
        
        for posting in holiday_analysis.holiday_postings:
            # Check for potential false positives
            amount = posting.get('amount', 0)
            holiday_type = posting.get('holiday_type', '')
            
            # Low amount transactions might be false positives
            if amount < 1000:
                indicators['potential_false_positives'].append({
                    'transaction_id': posting.get('transaction_id'),
                    'reason': 'Low amount transaction',
                    'amount': amount,
                    'confidence': 0.7
                })
            
            # Observance holidays might have legitimate business activity
            if 'Observance' in holiday_type and amount < 50000:
                indicators['potential_false_positives'].append({
                    'transaction_id': posting.get('transaction_id'),
                    'reason': 'Observance holiday with moderate amount',
                    'amount': amount,
                    'confidence': 0.8
                })
        
        return indicators
    

    
    def _extract_unique_users(self, holiday_postings):
        """Extract unique users from holiday postings"""
        users = set()
        for posting in holiday_postings:
            user = posting.get('user_name')
            if user:
                users.add(user)
        return list(users)
    
    def _extract_unique_accounts(self, holiday_postings):
        """Extract unique accounts from holiday postings"""
        accounts = set()
        for posting in holiday_postings:
            account = posting.get('gl_account')
            if account:
                accounts.add(account)
        return list(accounts)
    
    def _extract_unique_fs_lines(self, holiday_postings):
        """Extract unique FS lines from holiday postings"""
        fs_lines = set()
        for posting in holiday_postings:
            fs_line = posting.get('fs_line')
            if fs_line:
                fs_lines.add(fs_line)
        return list(fs_lines)
    
    def _generate_compliance_issues(self, holiday_analysis):
        """Generate compliance issues for holiday analysis"""
        if not holiday_analysis.holiday_postings:
            return []
        
        compliance_issues = []
        total_amount = sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings)
        
        # High value holiday transactions
        high_value_count = len([p for p in holiday_analysis.holiday_postings if p.get('amount', 0) > 1000000])
        if high_value_count > 0:
            compliance_issues.append({
                'issue_type': 'HIGH_VALUE_HOLIDAY_TRANSACTIONS',
                'severity': 'HIGH',
                'description': f'{high_value_count} high-value transactions posted on holidays',
                'recommendation': 'Review all high-value holiday transactions for business justification'
            })
        
        # Multiple users posting on same holiday
        holiday_users = {}
        for posting in holiday_analysis.holiday_postings:
            holiday_name = posting.get('holiday_name')
            user = posting.get('user')
            if holiday_name and user:
                if holiday_name not in holiday_users:
                    holiday_users[holiday_name] = set()
                holiday_users[holiday_name].add(user)
        
        for holiday, users in holiday_users.items():
            if len(users) > 3:
                compliance_issues.append({
                    'issue_type': 'MULTIPLE_USERS_HOLIDAY_ACTIVITY',
                    'severity': 'MEDIUM',
                    'description': f'{len(users)} users posted transactions on {holiday}',
                    'recommendation': 'Investigate why multiple users were active on this holiday'
                })
        
        return compliance_issues
    
    def _generate_high_priority_recommendations(self, holiday_analysis):
        """Generate high priority recommendations for holiday analysis"""
        if not holiday_analysis.holiday_postings:
            return []
        
        recommendations = []
        total_amount = sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings)
        
        if total_amount > 1000000:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Review all holiday transactions',
                'description': f'Total holiday transaction amount: {total_amount:,.2f} SAR',
                'impact': 'Financial risk'
            })
        
        high_value_count = len([p for p in holiday_analysis.holiday_postings if p.get('amount', 0) > 1000000])
        if high_value_count > 0:
            recommendations.append({
                'priority': 'CRITICAL',
                'action': 'Immediate review of high-value holiday transactions',
                'description': f'{high_value_count} transactions over 1M SAR posted on holidays',
                'impact': 'High financial risk'
            })
        
        return recommendations
    
    def _generate_holiday_patterns(self, holiday_analysis):
        """Generate holiday patterns analysis"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        patterns = {
            'holiday_distribution': {},
            'user_activity_patterns': {},
            'amount_patterns': {},
            'temporal_patterns': {}
        }
        
        # Holiday distribution
        holiday_counts = {}
        for posting in holiday_analysis.holiday_postings:
            holiday_name = posting.get('holiday_name', 'Unknown')
            holiday_counts[holiday_name] = holiday_counts.get(holiday_name, 0) + 1
        
        patterns['holiday_distribution'] = holiday_counts
        
        # User activity patterns
        user_activity = {}
        for posting in holiday_analysis.holiday_postings:
            user = posting.get('user', 'Unknown')
            if user not in user_activity:
                user_activity[user] = {'count': 0, 'total_amount': 0}
            user_activity[user]['count'] += 1
            user_activity[user]['total_amount'] += posting.get('amount', 0)
        
        patterns['user_activity_patterns'] = user_activity
        
        # Amount patterns
        amounts = [p.get('amount', 0) for p in holiday_analysis.holiday_postings]
        patterns['amount_patterns'] = {
            'min_amount': min(amounts) if amounts else 0,
            'max_amount': max(amounts) if amounts else 0,
            'avg_amount': sum(amounts) / len(amounts) if amounts else 0,
            'high_value_count': len([a for a in amounts if a > 1000000])
        }
        
        return patterns
    
    def _generate_audit_recommendations(self, holiday_analysis):
        """Generate audit recommendations for holiday analysis"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        recommendations = {
            'immediate_actions': [],
            'follow_up_actions': [],
            'monitoring_actions': []
        }
        
        total_amount = sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings)
        high_value_count = len([p for p in holiday_analysis.holiday_postings if p.get('amount', 0) > 1000000])
        
        if high_value_count > 0:
            recommendations['immediate_actions'].append({
                'action': 'Review all high-value holiday transactions',
                'priority': 'CRITICAL',
                'reason': f'{high_value_count} transactions over 1M SAR posted on holidays'
            })
        
        if total_amount > 1000000:
            recommendations['immediate_actions'].append({
                'action': 'Investigate holiday posting patterns',
                'priority': 'HIGH',
                'reason': f'Total holiday amount: {total_amount:,.2f} SAR'
            })
        
        recommendations['follow_up_actions'].append({
            'action': 'Implement holiday posting controls',
            'priority': 'MEDIUM',
            'reason': 'Prevent unauthorized holiday postings'
        })
        
        recommendations['monitoring_actions'].append({
            'action': 'Monitor holiday posting patterns',
            'priority': 'LOW',
            'reason': 'Track holiday activity for future audits'
        })
        
        return recommendations
    
    def _generate_risk_assessment(self, holiday_analysis):
        """Generate comprehensive risk assessment for holiday analysis"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        risk_assessment = {
            'overall_risk_level': 'LOW',
            'risk_factors': [],
            'risk_score': 0,
            'risk_distribution': {
                'low_risk': 0,
                'medium_risk': 0,
                'high_risk': 0,
                'critical_risk': 0
            }
        }
        
        total_amount = sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings)
        high_value_count = len([p for p in holiday_analysis.holiday_postings if p.get('amount', 0) > 1000000])
        
        # Calculate risk score
        risk_score = 0
        if high_value_count > 0:
            risk_score += 40
        if total_amount > 1000000:
            risk_score += 30
        if len(holiday_analysis.holiday_postings) > 10:
            risk_score += 20
        if len(holiday_analysis.holiday_postings) > 50:
            risk_score += 10
        
        risk_assessment['risk_score'] = risk_score
        
        # Determine risk level
        if risk_score >= 80:
            risk_assessment['overall_risk_level'] = 'CRITICAL'
        elif risk_score >= 60:
            risk_assessment['overall_risk_level'] = 'HIGH'
        elif risk_score >= 40:
            risk_assessment['overall_risk_level'] = 'MEDIUM'
        else:
            risk_assessment['overall_risk_level'] = 'LOW'
        
        # Risk factors
        if high_value_count > 0:
            risk_assessment['risk_factors'].append({
                'factor': 'High-value holiday transactions',
                'count': high_value_count,
                'impact': 'HIGH'
            })
        
        if total_amount > 1000000:
            risk_assessment['risk_factors'].append({
                'factor': 'High total holiday amount',
                'amount': total_amount,
                'impact': 'MEDIUM'
            })
        
        return risk_assessment
    
    def _generate_compliance_analysis(self, holiday_analysis):
        """Generate compliance analysis for holiday transactions"""
        if not holiday_analysis.holiday_postings:
            return {}
        
        compliance_analysis = {
            'compliance_score': 100,
            'compliance_issues': [],
            'regulatory_implications': [],
            'internal_control_weaknesses': []
        }
        
        total_amount = sum(p.get('amount', 0) for p in holiday_analysis.holiday_postings)
        high_value_count = len([p for p in holiday_analysis.holiday_postings if p.get('amount', 0) > 1000000])
        
        # Calculate compliance score
        compliance_score = 100
        if high_value_count > 0:
            compliance_score -= 30
        if total_amount > 1000000:
            compliance_score -= 20
        if len(holiday_analysis.holiday_postings) > 10:
            compliance_score -= 10
        
        compliance_analysis['compliance_score'] = max(0, compliance_score)
        
        # Compliance issues
        if high_value_count > 0:
            compliance_analysis['compliance_issues'].append({
                'issue': 'High-value holiday transactions',
                'severity': 'HIGH',
                'description': f'{high_value_count} transactions over 1M SAR posted on holidays'
            })
        
        # Regulatory implications
        if total_amount > 1000000:
            compliance_analysis['regulatory_implications'].append({
                'regulation': 'Internal Control Requirements',
                'implication': 'Holiday postings may indicate control weaknesses',
                'action_required': 'Review and strengthen controls'
            })
        
        # Internal control weaknesses
        if len(holiday_analysis.holiday_postings) > 10:
            compliance_analysis['internal_control_weaknesses'].append({
                'weakness': 'Multiple holiday postings',
                'description': 'Multiple transactions posted on holidays',
                'recommendation': 'Implement holiday posting restrictions'
            })
        
        return compliance_analysis

class GLAccountsPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 500

class FileGLAccountsView(APIView):
    """API view to return paginated GL accounts data for a given file."""
    def get(self, request, file_id):
        try:
            from .models import DataFile, SAPGLPosting
            data_file = DataFile.objects.get(id=file_id)
            transactions = SAPGLPosting.objects.filter(data_file=data_file)
            
            # Check if risk analysis has been run
            risk_analysis_status = self._check_risk_analysis_status(data_file)
            
            all_gl_accounts = self._generate_all_gl_accounts_data(transactions, data_file)

            paginator = GLAccountsPagination()
            page = paginator.paginate_queryset(all_gl_accounts, request)

            # Calculate comprehensive summary statistics
            summary_stats = self._calculate_comprehensive_summary_stats(all_gl_accounts)
            
            # The paginated response expects the main data to be the paginated list
            # We'll add comprehensive summary statistics as extra fields
            response_data = {
                'total_accounts': len(all_gl_accounts),
                'accounts': page,
                'summary': summary_stats,
                'risk_analysis_status': risk_analysis_status
            }
            return paginator.get_paginated_response(response_data)
        except DataFile.DoesNotExist:
            return Response({'error': f'File with ID {file_id} not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _generate_all_gl_accounts_data(self, transactions, data_file):
        """Generate comprehensive GL account data for all accounts in the file"""
        from django.db.models import Sum, Count, Avg, Q, Max, Min
        from decimal import Decimal
        
        # Debug: Check if risk scores are populated
        risk_score_stats = transactions.aggregate(
            avg_risk=Avg('overall_risk_score'),
            max_risk=Max('overall_risk_score'),
            min_risk=Min('overall_risk_score'),
            non_zero_risk=Count('id', filter=Q(overall_risk_score__gt=0))
        )
        print(f"DEBUG: Risk score stats - Avg: {risk_score_stats['avg_risk']}, Max: {risk_score_stats['max_risk']}, Min: {risk_score_stats['min_risk']}, Non-zero: {risk_score_stats['non_zero_risk']}")
        
        # Get all unique GL accounts with their aggregated data
        gl_accounts_data = transactions.values('gl_account').annotate(
            transaction_count=Count('id'),
            total_amount=Sum('amount_local_currency'),
            total_debits=Sum('amount_local_currency', filter=Q(transaction_type='DEBIT')),
            total_credits=Sum('amount_local_currency', filter=Q(transaction_type='CREDIT')),
            avg_amount=Avg('amount_local_currency'),
            avg_risk_score=Avg('overall_risk_score'),
            max_risk_score=Max('overall_risk_score'),
            min_risk_score=Min('overall_risk_score'),
            unique_users=Count('user_name', distinct=True),
            high_risk_transactions=Count('id', filter=Q(overall_risk_score__gte=60)),
            critical_risk_transactions=Count('id', filter=Q(overall_risk_score__gte=80)),
            duplicate_transactions=Count('id', filter=Q(is_duplicate=True)),
            backdated_transactions=Count('id', filter=Q(is_backdated=True)),
            holiday_transactions=Count('id', filter=Q(is_holiday_posting=True)),
            unusual_days_transactions=Count('id', filter=Q(anomaly_types__contains=['unusual_days'])),
            closing_entries_transactions=Count('id', filter=Q(anomaly_types__contains=['closing_entries']))
        ).order_by('gl_account')
        
        # Debug: Check anomaly detection
        print(f"DEBUG: Total transactions: {transactions.count()}")
        print(f"DEBUG: Transactions with is_duplicate=True: {transactions.filter(is_duplicate=True).count()}")
        print(f"DEBUG: Transactions with is_backdated=True: {transactions.filter(is_backdated=True).count()}")
        print(f"DEBUG: Transactions with is_holiday_posting=True: {transactions.filter(is_holiday_posting=True).count()}")
        print(f"DEBUG: Transactions with anomaly_types containing 'unusual_days': {transactions.filter(anomaly_types__contains=['unusual_days']).count()}")
        print(f"DEBUG: Transactions with anomaly_types containing 'closing_entries': {transactions.filter(anomaly_types__contains=['closing_entries']).count()}")
        
        # Use database flags for anomaly counts (they are correct based on debug output)
        # The database flags show the correct counts that match the dashboard
        actual_anomaly_counts = {
            'duplicate_transactions': transactions.filter(is_duplicate=True).count(),
            'backdated_transactions': transactions.filter(is_backdated=True).count(),
            'holiday_transactions': transactions.filter(is_holiday_posting=True).count(),
            'unusual_days_transactions': transactions.filter(anomaly_types__contains=['unusual_days']).count(),
            'closing_entries_transactions': transactions.filter(anomaly_types__contains=['closing_entries']).count(),
            'user_anomalies': transactions.filter(anomaly_types__contains=['user_analysis']).count(),
            'risk_anomalies': transactions.filter(anomaly_types__contains=['risk_analysis']).count(),
            'total_anomalies': 0  # Will be calculated below
        }
        
        # Calculate total anomalies
        actual_anomaly_counts['total_anomalies'] = sum([
            actual_anomaly_counts['duplicate_transactions'],
            actual_anomaly_counts['backdated_transactions'],
            actual_anomaly_counts['holiday_transactions'],
            actual_anomaly_counts['unusual_days_transactions'],
            actual_anomaly_counts['closing_entries_transactions'],
            actual_anomaly_counts['user_anomalies'],
            actual_anomaly_counts['risk_anomalies']
        ])
        
        print(f"DEBUG: Using database flags for anomaly counts: {actual_anomaly_counts}")
        
        all_gl_accounts = []
        
        for account_data in gl_accounts_data:
            gl_account = account_data['gl_account']
            
            # Use the actual anomaly counts we calculated above
            anomaly_counts = {
                'total_anomalies': actual_anomaly_counts['total_anomalies'],
                'duplicate_transactions': actual_anomaly_counts['duplicate_transactions'],
                'backdated_transactions': actual_anomaly_counts['backdated_transactions'],
                'holiday_transactions': actual_anomaly_counts['holiday_transactions'],
                'unusual_days_transactions': actual_anomaly_counts['unusual_days_transactions'],
                'closing_entries_transactions': actual_anomaly_counts['closing_entries_transactions'],
                'user_anomalies': actual_anomaly_counts['user_anomalies'],
                'risk_anomalies': actual_anomaly_counts['risk_anomalies'],
                'high_risk_transactions': account_data['high_risk_transactions'] or 0,
                'critical_risk_transactions': account_data['critical_risk_transactions'] or 0
            }
            
            print(f"DEBUG: Account {gl_account} - Using anomaly counts: {anomaly_counts}")
            
            # Calculate risk level based on average risk score, with fallback to anomaly-based calculation
            avg_risk_score = float(account_data['avg_risk_score'] or 0)
            max_risk_score = float(account_data['max_risk_score'] or 0)
            
            # Calculate risk level considering both stored scores and anomalies
            if avg_risk_score == 0 and max_risk_score == 0:
                # Calculate risk score based on anomalies
                calculated_risk_score = self._calculate_risk_score_from_anomalies(anomaly_counts, account_data['transaction_count'])
                risk_level = self._get_risk_level(calculated_risk_score)
                avg_risk_score = calculated_risk_score
                print(f"DEBUG: Account {gl_account} - No risk scores found, calculated: {calculated_risk_score}, risk_level: {risk_level}")
            else:
                # Use stored risk scores but also consider anomaly impact
                base_risk_level = self._get_risk_level(avg_risk_score)
                
                # Check if anomalies should elevate the risk level
                elevated_risk_level = self._calculate_elevated_risk_level(anomaly_counts, account_data['transaction_count'], avg_risk_score)
                
                # Use the higher risk level
                if self._get_risk_level_numeric(elevated_risk_level) > self._get_risk_level_numeric(base_risk_level):
                    risk_level = elevated_risk_level
                    print(f"DEBUG: Account {gl_account} - Risk level elevated from {base_risk_level} to {elevated_risk_level} due to anomalies")
                else:
                    risk_level = base_risk_level
                
                print(f"DEBUG: Account {gl_account} - Using stored risk scores - avg: {avg_risk_score}, max: {max_risk_score}, risk_level: {risk_level}")
            
            # Calculate balance
            total_debits = float(account_data['total_debits'] or 0)
            total_credits = float(account_data['total_credits'] or 0)
            balance = total_debits - total_credits
            
            # Get account details from GLAccount model if available
            account_details = self._get_account_details(gl_account)
            
            # Create account data structure
            account_info = {
                'gl_account': gl_account,
                'account_name': account_details.get('account_name', 'Unknown'),
                'account_type': account_details.get('account_type', 'Unknown'),
                'account_category': account_details.get('account_category', 'Unknown'),
                'transaction_count': account_data['transaction_count'],
                'total_amount': float(account_data['total_amount'] or 0),
                'total_debits': total_debits,
                'total_credits': total_credits,
                'balance': balance,
                'avg_amount': float(account_data['avg_amount'] or 0),
                'avg_risk_score': avg_risk_score,
                'max_risk_score': max_risk_score,
                'min_risk_score': float(account_data['min_risk_score'] or 0),
                'unique_users': account_data['unique_users'],
                'risk_level': risk_level,
                'anomaly_counts': anomaly_counts,
                'anomaly_percentage': (anomaly_counts['total_anomalies'] / account_data['transaction_count'] * 100) if account_data['transaction_count'] > 0 else 0,
                'high_risk_percentage': (account_data['high_risk_transactions'] / account_data['transaction_count'] * 100) if account_data['transaction_count'] > 0 else 0,
                'critical_risk_percentage': (account_data['critical_risk_transactions'] / account_data['transaction_count'] * 100) if account_data['transaction_count'] > 0 else 0,
                'balance_percentage': (abs(balance) / float(account_data['total_amount'] or 1) * 100) if account_data['total_amount'] else 0,
                'is_balanced': abs(balance) < 0.01,
                'has_anomalies': anomaly_counts['total_anomalies'] > 0,
                'is_high_risk': risk_level in ['HIGH', 'CRITICAL'],
                'activity_level': self._get_activity_level(account_data['transaction_count']),
                'amount_category': self._get_amount_category(float(account_data['total_amount'] or 0))
            }
            
            all_gl_accounts.append(account_info)
        
        return all_gl_accounts

    def _calculate_risk_score_from_anomalies(self, anomaly_counts, total_transactions):
        """Calculate risk score based on anomalies when overall_risk_score is not available"""
        if total_transactions == 0:
            return 0.0
        
        # Base risk score
        risk_score = 0.0
        
        # Add risk points for each anomaly type
        if anomaly_counts['duplicate_transactions'] > 0:
            risk_score += 80.0 * (anomaly_counts['duplicate_transactions'] / total_transactions)
        
        if anomaly_counts['backdated_transactions'] > 0:
            risk_score += 70.0 * (anomaly_counts['backdated_transactions'] / total_transactions)
        
        if anomaly_counts['holiday_transactions'] > 0:
            risk_score += 60.0 * (anomaly_counts['holiday_transactions'] / total_transactions)
        
        if anomaly_counts['unusual_days_transactions'] > 0:
            risk_score += 40.0 * (anomaly_counts['unusual_days_transactions'] / total_transactions)
        
        if anomaly_counts['closing_entries_transactions'] > 0:
            risk_score += 30.0 * (anomaly_counts['closing_entries_transactions'] / total_transactions)
        
        if anomaly_counts['user_anomalies'] > 0:
            risk_score += 50.0 * (anomaly_counts['user_anomalies'] / total_transactions)
        
        if anomaly_counts['risk_anomalies'] > 0:
            risk_score += 90.0 * (anomaly_counts['risk_anomalies'] / total_transactions)
        
        # Cap at 100
        return min(risk_score, 100.0)

    def _get_actual_anomaly_counts(self, data_file):
        """Get actual anomaly counts from analysis results"""
        from .models import DuplicateAnalysisResult, BackdatedAnalysisResult, HolidayAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult
        
        actual_counts = {
            'duplicate_transactions': 0,
            'backdated_transactions': 0,
            'holiday_transactions': 0,
            'unusual_days_transactions': 0,
            'closing_entries_transactions': 0,
            'total_anomalies': 0
        }
        
        # Get duplicate analysis results
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        if duplicate_analysis and duplicate_analysis.duplicate_list:
            actual_counts['duplicate_transactions'] = len(duplicate_analysis.duplicate_list) * 2  # Each duplicate has 2 transactions
        
        # Get backdated analysis results
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        if backdated_analysis and backdated_analysis.backdated_entries:
            actual_counts['backdated_transactions'] = len(backdated_analysis.backdated_entries)
        
        # Get holiday analysis results
        holiday_analysis = HolidayAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        if holiday_analysis and holiday_analysis.holiday_postings:
            actual_counts['holiday_transactions'] = len(holiday_analysis.holiday_postings)
        
        # Get unusual days analysis results
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        if unusual_days_analysis and unusual_days_analysis.weekend_postings:
            actual_counts['unusual_days_transactions'] = len(unusual_days_analysis.weekend_postings)
        
        # Get closing entries analysis results
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        if closing_entries_analysis and closing_entries_analysis.closing_entries:
            actual_counts['closing_entries_transactions'] = len(closing_entries_analysis.closing_entries)
        
        # Calculate total anomalies
        actual_counts['total_anomalies'] = sum([
            actual_counts['duplicate_transactions'],
            actual_counts['backdated_transactions'],
            actual_counts['holiday_transactions'],
            actual_counts['unusual_days_transactions'],
            actual_counts['closing_entries_transactions']
        ])
        
        return actual_counts

    def _calculate_elevated_risk_level(self, anomaly_counts, total_transactions, current_risk_score):
        """Calculate elevated risk level based on anomalies"""
        if total_transactions == 0:
            return 'LOW'
        
        # Calculate anomaly percentage
        total_anomalies = anomaly_counts['total_anomalies']
        anomaly_percentage = (total_anomalies / total_transactions) * 100
        
        # Calculate high/critical risk transaction percentage
        high_risk_transactions = anomaly_counts['high_risk_transactions']
        critical_risk_transactions = anomaly_counts['critical_risk_transactions']
        high_critical_percentage = ((high_risk_transactions + critical_risk_transactions) / total_transactions) * 100
        
        # Risk level elevation rules
        if critical_risk_transactions > 0 or anomaly_counts['risk_anomalies'] > 0:
            return 'CRITICAL'
        elif high_risk_transactions > 0 or anomaly_percentage > 30:
            return 'HIGH'
        elif anomaly_percentage > 15:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _get_risk_level_numeric(self, risk_level):
        """Convert risk level to numeric value for comparison"""
        risk_level_map = {
            'CRITICAL': 4,
            'HIGH': 3,
            'MEDIUM': 2,
            'LOW': 1
        }
        return risk_level_map.get(risk_level, 1)

    def _check_risk_analysis_status(self, data_file):
        """Check if risk analysis has been run for this file"""
        from .models import RiskScoringDocument, DuplicateAnalysisResult, BackdatedAnalysisResult, HolidayAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult
        
        # Check if risk scoring document exists
        risk_document = RiskScoringDocument.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).order_by('-created_at').first()
        
        # Check if individual analyses exist
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).first()
        
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).first()
        
        holiday_analysis = HolidayAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).first()
        
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).first()
        
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(
            data_file=data_file, 
            status='COMPLETED'
        ).first()
        
        return {
            'risk_scoring_document_exists': risk_document is not None,
            'duplicate_analysis_exists': duplicate_analysis is not None,
            'backdated_analysis_exists': backdated_analysis is not None,
            'holiday_analysis_exists': holiday_analysis is not None,
            'unusual_days_analysis_exists': unusual_days_analysis is not None,
            'closing_entries_analysis_exists': closing_entries_analysis is not None,
            'all_analyses_complete': all([
                duplicate_analysis is not None,
                backdated_analysis is not None,
                holiday_analysis is not None,
                unusual_days_analysis is not None,
                closing_entries_analysis is not None
            ]),
            'risk_document_id': str(risk_document.id) if risk_document else None,
            'last_analysis_date': risk_document.created_at.isoformat() if risk_document else None
        }

    def _calculate_comprehensive_summary_stats(self, all_gl_accounts):
        """Calculate comprehensive summary statistics for GL accounts"""
        if not all_gl_accounts:
            return {
                'total_accounts': 0,
                'total_transactions': 0,
                'total_amount': 0.0,
                'total_debits': 0.0,
                'total_credits': 0.0,
                'trading_equity': 0.0,
                'currency': 'SAR',
                'avg_risk_score': 0.0,
                'accounts_with_anomalies': 0,
                'high_risk_accounts': 0,
                'critical_risk_accounts': 0,
                'medium_risk_accounts': 0,
                'low_risk_accounts': 0,
                'anomaly_rate': 0.0,
                'avg_amount_per_account': 0.0,
                'avg_transactions_per_account': 0.0,
                'risk_distribution': {
                    'critical': 0,
                    'high': 0,
                    'medium': 0,
                    'low': 0
                },
                'anomaly_distribution': {
                    'duplicate_transactions': 0,
                    'backdated_transactions': 0,
                    'holiday_transactions': 0,
                    'unusual_days_transactions': 0,
                    'closing_entries_transactions': 0,
                    'total_anomalies': 0
                },
                'activity_distribution': {
                    'high_activity': 0,
                    'medium_activity': 0,
                    'low_activity': 0,
                    'minimal_activity': 0
                },
                'amount_distribution': {
                    'very_high': 0,
                    'high': 0,
                    'medium': 0,
                    'low': 0,
                    'minimal': 0
                }
            }
        
        # Calculate basic totals
        total_accounts = len(all_gl_accounts)
        total_transactions = sum(acc['transaction_count'] for acc in all_gl_accounts)
        total_amount = sum(acc['total_amount'] for acc in all_gl_accounts)
        total_debits = sum(acc['total_debits'] for acc in all_gl_accounts)
        total_credits = sum(acc['total_credits'] for acc in all_gl_accounts)
        trading_equity = total_debits - total_credits
        
        # Calculate risk statistics
        avg_risk_score = sum(acc['avg_risk_score'] for acc in all_gl_accounts) / total_accounts if total_accounts > 0 else 0
        accounts_with_anomalies = len([acc for acc in all_gl_accounts if acc['anomaly_counts']['total_anomalies'] > 0])
        
        # Count accounts by risk level (using the corrected risk levels)
        high_risk_accounts = len([acc for acc in all_gl_accounts if acc['risk_level'] in ['HIGH', 'CRITICAL']])
        critical_risk_accounts = len([acc for acc in all_gl_accounts if acc['risk_level'] == 'CRITICAL'])
        medium_risk_accounts = len([acc for acc in all_gl_accounts if acc['risk_level'] == 'MEDIUM'])
        low_risk_accounts = len([acc for acc in all_gl_accounts if acc['risk_level'] == 'LOW'])
        
        # Debug: Print risk level distribution
        print(f"DEBUG: Risk level distribution - Critical: {critical_risk_accounts}, High: {high_risk_accounts - critical_risk_accounts}, Medium: {medium_risk_accounts}, Low: {low_risk_accounts}")
        for acc in all_gl_accounts:
            print(f"DEBUG: Account {acc['gl_account']} - Risk Level: {acc['risk_level']}, Anomalies: {acc['anomaly_counts']['total_anomalies']}, Anomaly %: {acc['anomaly_percentage']:.2f}%")
        
        # Calculate anomaly statistics
        total_anomalies = sum(acc['anomaly_counts']['total_anomalies'] for acc in all_gl_accounts)
        anomaly_rate = (total_anomalies / total_transactions * 100) if total_transactions > 0 else 0
        
        # Calculate averages
        avg_amount_per_account = total_amount / total_accounts if total_accounts > 0 else 0
        avg_transactions_per_account = total_transactions / total_accounts if total_accounts > 0 else 0
        
        # Calculate distributions
        risk_distribution = {
            'critical': len([acc for acc in all_gl_accounts if acc['risk_level'] == 'CRITICAL']),
            'high': len([acc for acc in all_gl_accounts if acc['risk_level'] == 'HIGH']),
            'medium': len([acc for acc in all_gl_accounts if acc['risk_level'] == 'MEDIUM']),
            'low': len([acc for acc in all_gl_accounts if acc['risk_level'] == 'LOW'])
        }
        
        anomaly_distribution = {
            'duplicate_transactions': sum(acc['anomaly_counts']['duplicate_transactions'] for acc in all_gl_accounts),
            'backdated_transactions': sum(acc['anomaly_counts']['backdated_transactions'] for acc in all_gl_accounts),
            'holiday_transactions': sum(acc['anomaly_counts']['holiday_transactions'] for acc in all_gl_accounts),
            'unusual_days_transactions': sum(acc['anomaly_counts']['unusual_days_transactions'] for acc in all_gl_accounts),
            'closing_entries_transactions': sum(acc['anomaly_counts']['closing_entries_transactions'] for acc in all_gl_accounts),
            'total_anomalies': total_anomalies
        }
        
        activity_distribution = {
            'high_activity': len([acc for acc in all_gl_accounts if acc['activity_level'] == 'HIGH']),
            'medium_activity': len([acc for acc in all_gl_accounts if acc['activity_level'] == 'MEDIUM']),
            'low_activity': len([acc for acc in all_gl_accounts if acc['activity_level'] == 'LOW']),
            'minimal_activity': len([acc for acc in all_gl_accounts if acc['activity_level'] == 'MINIMAL'])
        }
        
        amount_distribution = {
            'very_high': len([acc for acc in all_gl_accounts if acc['amount_category'] == 'VERY_HIGH']),
            'high': len([acc for acc in all_gl_accounts if acc['amount_category'] == 'HIGH']),
            'medium': len([acc for acc in all_gl_accounts if acc['amount_category'] == 'MEDIUM']),
            'low': len([acc for acc in all_gl_accounts if acc['amount_category'] == 'LOW']),
            'minimal': len([acc for acc in all_gl_accounts if acc['amount_category'] == 'MINIMAL'])
        }
        
        return {
            'total_accounts': total_accounts,
            'total_transactions': total_transactions,
            'total_amount': total_amount,
            'total_debits': total_debits,
            'total_credits': total_credits,
            'trading_equity': trading_equity,
            'currency': 'SAR',
            'avg_risk_score': avg_risk_score,
            'accounts_with_anomalies': accounts_with_anomalies,
            'high_risk_accounts': high_risk_accounts,
            'critical_risk_accounts': critical_risk_accounts,
            'medium_risk_accounts': medium_risk_accounts,
            'low_risk_accounts': low_risk_accounts,
            'anomaly_rate': anomaly_rate,
            'avg_amount_per_account': avg_amount_per_account,
            'avg_transactions_per_account': avg_transactions_per_account,
            'risk_distribution': risk_distribution,
            'anomaly_distribution': anomaly_distribution,
            'activity_distribution': activity_distribution,
            'amount_distribution': amount_distribution
        }

    def _get_account_details(self, gl_account):
        """Get account details from GLAccount model, create if doesn't exist"""
        try:
            from .models import GLAccount
            account = GLAccount.objects.get(account_id=gl_account)
            return {
                'account_name': account.account_name,
                'account_type': account.account_type,
                'account_category': account.account_category,
                'account_subcategory': account.account_subcategory,
                'normal_balance': account.normal_balance,
                'is_active': account.is_active
            }
        except GLAccount.DoesNotExist:
            # Auto-create GL account with reasonable defaults based on account number patterns
            account_name, account_type, account_category = self._infer_account_details(gl_account)
            
            try:
                # Determine normal balance based on account type
                normal_balance = self._get_normal_balance(account_type)
                
                # Create the GL account record
                account = GLAccount.objects.create(
                    account_id=gl_account,
                    account_name=account_name,
                    account_type=account_type,
                    account_category=account_category,
                    account_subcategory=None,
                    normal_balance=normal_balance,
                    is_active=True
                )
                
                return {
                    'account_name': account.account_name,
                    'account_type': account.account_type,
                    'account_category': account.account_category,
                    'account_subcategory': account.account_subcategory,
                    'normal_balance': account.normal_balance,
                    'is_active': account.is_active
                }
            except Exception as e:
                 # If creation fails, return defaults
                 print(f"Failed to create GL account {gl_account}: {e}")
                 normal_balance = self._get_normal_balance(account_type)
                 return {
                     'account_name': account_name,
                     'account_type': account_type,
                     'account_category': account_category,
                     'account_subcategory': None,
                     'normal_balance': normal_balance,
                     'is_active': True
                 }
    
    def _infer_account_details(self, gl_account):
        """Infer account details based on account number patterns"""
        try:
            # Convert to string and get the first few digits
            account_str = str(gl_account).strip()
            
            if not account_str:
                return 'Unknown Account', 'Unknown', 'Unknown'
            
            # Get the first digit or first few digits for classification
            first_digit = account_str[0] if len(account_str) > 0 else '0'
            first_two = account_str[:2] if len(account_str) >= 2 else first_digit
            
            # Common SAP GL account classification patterns
            if first_digit == '1':
                if first_two in ['10', '11', '12', '13', '14', '15']:
                    return f'Cash & Cash Equivalents {gl_account}', 'Asset', 'Cash & Cash Equivalents'
                elif first_two in ['16', '17', '18', '19']:
                    return f'Other Current Assets {gl_account}', 'Asset', 'Other Current Assets'
                else:
                    return f'Current Assets {gl_account}', 'Asset', 'Current Assets'
            
            elif first_digit == '2':
                if first_two in ['20', '21', '22', '23', '24', '25']:
                    return f'Accounts Payable {gl_account}', 'Liability', 'Accounts Payable'
                elif first_two in ['26', '27', '28', '29']:
                    return f'Other Current Liabilities {gl_account}', 'Liability', 'Other Current Liabilities'
                else:
                    return f'Current Liabilities {gl_account}', 'Liability', 'Current Liabilities'
            
            elif first_digit == '3':
                if first_two in ['30', '31', '32', '33', '34', '35']:
                    return f'Long-term Debt {gl_account}', 'Liability', 'Long-term Debt'
                elif first_two in ['36', '37', '38', '39']:
                    return f'Other Long-term Liabilities {gl_account}', 'Liability', 'Other Long-term Liabilities'
                else:
                    return f'Long-term Liabilities {gl_account}', 'Liability', 'Long-term Liabilities'
            
            elif first_digit == '4':
                if first_two in ['40', '41', '42', '43', '44', '45']:
                    return f'Shareholders Equity {gl_account}', 'Equity', 'Shareholders Equity'
                elif first_two in ['46', '47', '48', '49']:
                    return f'Retained Earnings {gl_account}', 'Equity', 'Retained Earnings'
                else:
                    return f'Equity {gl_account}', 'Equity', 'Equity'
            
            elif first_digit == '5':
                if first_two in ['50', '51', '52', '53', '54', '55']:
                    return f'Revenue {gl_account}', 'Revenue', 'Revenue'
                elif first_two in ['56', '57', '58', '59']:
                    return f'Other Revenue {gl_account}', 'Revenue', 'Other Revenue'
                else:
                    return f'Revenue {gl_account}', 'Revenue', 'Revenue'
            
            elif first_digit == '6':
                if first_two in ['60', '61', '62', '63', '64', '65']:
                    return f'Cost of Sales {gl_account}', 'Expense', 'Cost of Sales'
                elif first_two in ['66', '67', '68', '69']:
                    return f'Other Cost of Sales {gl_account}', 'Expense', 'Other Cost of Sales'
                else:
                    return f'Cost of Sales {gl_account}', 'Expense', 'Cost of Sales'
            
            elif first_digit == '7':
                if first_two in ['70', '71', '72', '73', '74', '75']:
                    return f'Selling & Marketing {gl_account}', 'Expense', 'Selling & Marketing'
                elif first_two in ['76', '77', '78', '79']:
                    return f'Other Selling Expenses {gl_account}', 'Expense', 'Other Selling Expenses'
                else:
                    return f'Selling Expenses {gl_account}', 'Expense', 'Selling Expenses'
            
            elif first_digit == '8':
                if first_two in ['80', '81', '82', '83', '84', '85']:
                    return f'General & Administrative {gl_account}', 'Expense', 'General & Administrative'
                elif first_two in ['86', '87', '88', '89']:
                    return f'Other Administrative {gl_account}', 'Expense', 'Other Administrative'
                else:
                    return f'Administrative Expenses {gl_account}', 'Expense', 'Administrative Expenses'
            
            elif first_digit == '9':
                if first_two in ['90', '91', '92', '93', '94', '95']:
                    return f'Other Income & Expenses {gl_account}', 'Expense', 'Other Income & Expenses'
                elif first_two in ['96', '97', '98', '99']:
                    return f'Extraordinary Items {gl_account}', 'Expense', 'Extraordinary Items'
                else:
                    return f'Other Items {gl_account}', 'Expense', 'Other Items'
            
            else:
                # Default classification for unknown patterns
                return f'Account {gl_account}', 'Unknown', 'Unknown'
                
        except Exception as e:
            print(f"Error inferring account details for {gl_account}: {e}")
            return f'Account {gl_account}', 'Unknown', 'Unknown'
    
    def _get_normal_balance(self, account_type):
         """Determine normal balance based on account type"""
         if account_type in ['Asset', 'Expense']:
             return 'DEBIT'
         elif account_type in ['Liability', 'Equity', 'Revenue']:
             return 'CREDIT'
         else:
             return 'DEBIT'  # Default to debit

    def _get_risk_level(self, risk_score):
        """Determine risk level based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _get_activity_level(self, transaction_count):
        """Determine activity level based on transaction count"""
        if transaction_count >= 100:
            return 'HIGH'
        elif transaction_count >= 50:
            return 'MEDIUM'
        elif transaction_count >= 10:
            return 'LOW'
        else:
            return 'MINIMAL'

    def _get_amount_category(self, total_amount):
        """Determine amount category based on total amount"""
        if total_amount >= 1000000:
            return 'VERY_HIGH'
        elif total_amount >= 100000:
            return 'HIGH'
        elif total_amount >= 10000:
            return 'MEDIUM'
        elif total_amount >= 1000:
            return 'LOW'
        else:
            return 'MINIMAL'

class ClosingEntriesListView(generics.ListAPIView):
    """API view for listing closing entries with pagination.
    
    This view provides a paginated list of closing entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user_name: Filter by user name
    - account: Filter by account
    - date_from, date_to: Posting date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - days_from_month_end: Filter by days from month end
    - high_value: Filter by high value transactions (true/false)
    - ordering: Field to order by (e.g., -amount, posting_date, -risk_level)
    """
    
    serializer_class = ClosingEntriesListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get closing entries for the specified file"""
        file_id = self.kwargs.get('file_id')
        
        # Get the latest closing entries analysis result
        from .models import ClosingEntriesAnalysisResult
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(
            data_file_id=file_id, status='COMPLETED'
        ).order_by('-analysis_date').first()
        
        if not closing_entries_analysis:
            return []
        
        # Get closing entries from the analysis result
        closing_entries = closing_entries_analysis.closing_entries or []
        
        # Apply filters
        filtered_entries = self._apply_filters(closing_entries)
        
        # Enhance entries with risk scores
        enhanced_entries = self._enhance_closing_entries(filtered_entries)
        
        return enhanced_entries
    
    def _apply_filters(self, closing_entries):
        """Apply filters to closing entries based on actual data structure"""
        filtered_entries = closing_entries
        
        # Filter by user name
        user_name = self.request.query_params.get('user_name')
        if user_name:
            filtered_entries = [
                entry for entry in filtered_entries 
                if user_name.lower() in entry.get('user', '').lower()
            ]
        
        # Filter by account
        account = self.request.query_params.get('account')
        if account:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('account') == account
            ]
        
        # Filter by posting date range
        date_from = self.request.query_params.get('date_from')
        date_to = self.request.query_params.get('date_to')
        if date_from or date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('posting_date'), date_from, date_to)
            ]
        
        # Filter by amount range
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Filter by risk level
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('risk_level') == risk_level.upper()
            ]
        
        # Filter by days from month end
        days_from_month_end = self.request.query_params.get('days_from_month_end')
        if days_from_month_end:
            try:
                days = int(days_from_month_end)
                filtered_entries = [
                    entry for entry in filtered_entries
                    if entry.get('days_from_month_end', 0) == days
                ]
            except (ValueError, TypeError):
                pass
        
        # Filter by high value transactions
        high_value = self.request.query_params.get('high_value')
        if high_value is not None:
            high_value_bool = high_value.lower() == 'true'
            filtered_entries = [
                entry for entry in filtered_entries
                if (float(entry.get('amount', 0)) > 10000000) == high_value_bool
            ]
        
        # Apply sorting
        ordering = self.request.query_params.get('ordering', '-amount')
        if ordering:
            reverse = ordering.startswith('-')
            field = ordering[1:] if reverse else ordering
            
            if field == 'amount':
                filtered_entries.sort(key=lambda x: float(x.get('amount', 0)), reverse=reverse)
            elif field == 'posting_date':
                filtered_entries.sort(key=lambda x: x.get('posting_date', ''), reverse=reverse)
            elif field == 'user':
                filtered_entries.sort(key=lambda x: x.get('user', ''), reverse=reverse)
            elif field == 'account':
                filtered_entries.sort(key=lambda x: x.get('account', ''), reverse=reverse)
            elif field == 'risk_level':
                filtered_entries.sort(key=lambda x: x.get('risk_level', ''), reverse=reverse)
            elif field == 'days_from_month_end':
                filtered_entries.sort(key=lambda x: x.get('days_from_month_end', 0), reverse=reverse)
        
        return filtered_entries
    
    def _is_date_in_range(self, posting_date, date_from, date_to):
        """Check if posting date is within the specified range"""
        if not posting_date:
            return False
        
        from datetime import datetime
        try:
            if isinstance(posting_date, str):
                posting_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
            
            if date_from:
                from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                if posting_date < from_date:
                    return False
            
            if date_to:
                to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                if posting_date > to_date:
                    return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def _enhance_closing_entries(self, closing_entries):
        """Enhance closing entries with risk scores and additional information"""
        if not closing_entries:
            return []
        
        enhanced_entries = []
        for entry in closing_entries:
            if isinstance(entry, dict):
                enhanced_entry = entry.copy()
                
                # Add risk score if missing
                if not enhanced_entry.get('risk_score'):
                    enhanced_entry['risk_score'] = self._calculate_closing_risk_score(entry)
                
                # Add risk level if missing
                if not enhanced_entry.get('risk_level'):
                    enhanced_entry['risk_level'] = self._get_closing_risk_level(
                        enhanced_entry.get('risk_score', 0)
                    )
                
                enhanced_entries.append(enhanced_entry)
            else:
                enhanced_entries.append(entry)
        
        return enhanced_entries
    
    def _calculate_closing_risk_score(self, entry):
        """Calculate risk score for a closing entry based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md"""
        try:
            # Base risk score for closing entries (30 points)
            risk_score = 30.0
            
            # Factor 1: Post-close entries (+25 points)
            if entry.get('is_post_close', False):
                risk_score += 25.0
            
            # Factor 2: Debit closing entries (+10 points)
            amount = float(entry.get('amount', 0))
            if amount < 0:  # Debit transaction
                risk_score += 10.0
            
            # Factor 3: Month-end closing (+5 points)
            days_from_month_end = entry.get('days_from_month_end', 0)
            if days_from_month_end <= 1:  # Month-end or day after
                risk_score += 5.0
            
            # Factor 4: Year-end closing (+10 points)
            posting_date = entry.get('posting_date')
            if posting_date:
                try:
                    from datetime import datetime
                    if isinstance(posting_date, str):
                        posting_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
                    # Check if it's December and near year-end
                    if posting_date.month == 12 and posting_date.day >= 25:
                        risk_score += 10.0
                except:
                    pass
            
            return min(risk_score, 100.0)
        except Exception as e:
            return 30.0
    
    def _get_closing_risk_level(self, risk_score):
        """Get risk level based on risk score for closing entries"""
        if risk_score >= 90:
            return 'CRITICAL'
        elif risk_score >= 70:
            return 'HIGH'
        elif risk_score >= 50:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def list(self, request, *args, **kwargs):
        """Return only paginated closing entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    


class BackdatedEntriesListView(generics.ListAPIView):
    """API view for listing backdated entries with pagination.
    
    This view provides a paginated list of backdated entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user_name: Filter by user name
    - account: Filter by account
    - date_from, date_to: Posting date range
    - document_date_from, document_date_to: Document date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - days_difference_range: Comma-separated min,max (e.g., days_difference_range=1,30)
    - backdated_severity: Filter by severity (MINOR, MODERATE, SIGNIFICANT, CRITICAL)
    - ordering: Field to order by (e.g., -amount, posting_date, -days_difference)
    """
    
    serializer_class = BackdatedEntriesListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get backdated entries for the specified file"""
        file_id = self.kwargs.get('file_id')
        
        # Get the latest backdated analysis result
        from .models import BackdatedAnalysisResult
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file_id=file_id, status='COMPLETED'
        ).order_by('-analysis_date').first()
        
        if not backdated_analysis:
            return []
        
        # Get backdated entries from the analysis result
        backdated_entries = backdated_analysis.backdated_entries or []
        
        # Apply filters
        filtered_entries = self._apply_filters(backdated_entries)
        
        # Enhance entries with risk scores
        enhanced_entries = self._enhance_backdated_entries(filtered_entries)
        
        return enhanced_entries
    
    def _apply_filters(self, backdated_entries):
        """Apply filters to backdated entries based on actual data structure"""
        filtered_entries = backdated_entries
        
        # Filter by user name
        user_name = self.request.query_params.get('user_name')
        if user_name:
            filtered_entries = [
                entry for entry in filtered_entries 
                if user_name.lower() in entry.get('user', '').lower()
            ]
        
        # Filter by account
        account = self.request.query_params.get('account')
        if account:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('account') == account
            ]
        
        # Filter by posting date range
        date_from = self.request.query_params.get('date_from')
        date_to = self.request.query_params.get('date_to')
        if date_from or date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('posting_date'), date_from, date_to)
            ]
        
        # Filter by document date range
        document_date_from = self.request.query_params.get('document_date_from')
        document_date_to = self.request.query_params.get('document_date_to')
        if document_date_from or document_date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('document_date'), document_date_from, document_date_to)
            ]
        
        # Filter by amount range
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Filter by risk level
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('risk_level') == risk_level.upper()
            ]
        
        # Filter by days difference range
        days_difference_range = self.request.query_params.get('days_difference_range')
        if days_difference_range:
            try:
                min_days, max_days = map(int, days_difference_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_days <= entry.get('days_difference', 0) <= max_days
                ]
            except (ValueError, TypeError):
                pass
        
        # Filter by backdated severity
        backdated_severity = self.request.query_params.get('backdated_severity')
        if backdated_severity:
            severity_mapping = {
                'MINOR': (0, 7),
                'MODERATE': (8, 30),
                'SIGNIFICANT': (31, 90),
                'CRITICAL': (91, float('inf'))
            }
            if backdated_severity in severity_mapping:
                min_days, max_days = severity_mapping[backdated_severity]
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_days <= entry.get('days_difference', 0) <= max_days
                ]
        
        # Apply sorting
        ordering = self.request.query_params.get('ordering', '-amount')
        if ordering:
            reverse = ordering.startswith('-')
            field = ordering[1:] if reverse else ordering
            
            if field == 'amount':
                filtered_entries.sort(key=lambda x: float(x.get('amount', 0)), reverse=reverse)
            elif field == 'posting_date':
                filtered_entries.sort(key=lambda x: x.get('posting_date', ''), reverse=reverse)
            elif field == 'document_date':
                filtered_entries.sort(key=lambda x: x.get('document_date', ''), reverse=reverse)
            elif field == 'user':
                filtered_entries.sort(key=lambda x: x.get('user', ''), reverse=reverse)
            elif field == 'account':
                filtered_entries.sort(key=lambda x: x.get('account', ''), reverse=reverse)
            elif field == 'risk_level':
                filtered_entries.sort(key=lambda x: x.get('risk_level', ''), reverse=reverse)
            elif field == 'days_difference':
                filtered_entries.sort(key=lambda x: x.get('days_difference', 0), reverse=reverse)
        
        return filtered_entries
    
    def _is_date_in_range(self, posting_date, date_from, date_to):
        """Check if posting date is within the specified range"""
        if not posting_date:
            return False
        
        from datetime import datetime
        try:
            if isinstance(posting_date, str):
                posting_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
            
            if date_from:
                from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                if posting_date < from_date:
                    return False
            
            if date_to:
                to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                if posting_date > to_date:
                    return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def _enhance_backdated_entries(self, backdated_entries):
        """Enhance backdated entries with risk scores and additional information"""
        if not backdated_entries:
            return []
        
        enhanced_entries = []
        for entry in backdated_entries:
            if isinstance(entry, dict):
                enhanced_entry = entry.copy()
                
                # Add risk score if missing
                if not enhanced_entry.get('risk_score'):
                    enhanced_entry['risk_score'] = self._calculate_backdated_risk_score(entry)
                
                # Add risk level if missing
                if not enhanced_entry.get('risk_level'):
                    enhanced_entry['risk_level'] = self._get_backdated_risk_level(
                        enhanced_entry.get('risk_score', 0)
                    )
                
                # Add backdated severity if missing
                if not enhanced_entry.get('backdated_severity'):
                    enhanced_entry['backdated_severity'] = self._get_backdated_severity(
                        enhanced_entry.get('days_difference', 0)
                    )
                
                enhanced_entries.append(enhanced_entry)
            else:
                enhanced_entries.append(entry)
        
        return enhanced_entries
    
    def _calculate_backdated_risk_score(self, entry):
        """Calculate risk score for a backdated entry based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md"""
        try:
            # Base risk score for backdated entries (50 points)
            risk_score = 50.0
            
            # Factor 1: Days difference (primary factor)
            days_difference = entry.get('days_difference', 0)
            if days_difference > 30:
                risk_score = 100.0  # Critical
            elif days_difference > 14:
                risk_score = 85.0   # High
            elif days_difference > 7:
                risk_score = 70.0   # Medium
            else:
                risk_score = 50.0   # Low
            
            return min(risk_score, 100.0)
        except Exception as e:
            return 50.0
    
    def _get_backdated_risk_level(self, risk_score):
        """Get risk level based on risk score for backdated entries"""
        if risk_score >= 85:
            return 'CRITICAL'
        elif risk_score >= 70:
            return 'HIGH'
        elif risk_score >= 50:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_backdated_severity(self, days_difference):
        """Get backdated severity based on days difference"""
        if days_difference <= 7:
            return 'MINOR'
        elif days_difference <= 30:
            return 'MODERATE'
        elif days_difference <= 90:
            return 'SIGNIFICANT'
        else:
            return 'CRITICAL'
    
    def list(self, request, *args, **kwargs):
        """Return only paginated backdated entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    

class UnusualDaysListView(generics.ListAPIView):
    """API view for listing unusual days entries with pagination.
    
    This view provides a paginated list of unusual days entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user: Filter by user name
    - account: Filter by account
    - posting_date_from, posting_date_to: Posting date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - day_of_week: Filter by day of week (Monday, Tuesday, etc.)
    - day_type: Filter by day type (WEEKEND, WEEKDAY)
    - high_value: Filter by high value transactions (true/false)
    - ordering: Field to order by (e.g., -amount, posting_date, -risk_level)
    """
    
    serializer_class = UnusualDaysListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get unusual days entries from UnusualDaysAnalysisResult"""
        file_id = self.kwargs.get('file_id')
        
        try:
            unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(
                data_file_id=file_id,
                status='COMPLETED'
            ).first()
            
            if not unusual_days_analysis:
                return []
            
            unusual_days = unusual_days_analysis.unusual_days or []
            enhanced_days = self._enhance_unusual_days(unusual_days)
            return self._apply_filters(enhanced_days)
            
        except Exception as e:
            logger.error(f"Error fetching unusual days data: {e}")
            return []
    
    def _apply_filters(self, unusual_days):
        """Apply filters to unusual days entries"""
        filtered_entries = unusual_days
        
        # User filter
        user_filter = self.request.query_params.get('user')
        if user_filter:
            user_terms = [term.strip().lower() for term in user_filter.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if any(term in entry.get('user', '').lower() for term in user_terms)
            ]
        
        # Account filter
        account_filter = self.request.query_params.get('account')
        if account_filter:
            account_terms = [term.strip() for term in account_filter.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('account', '') in account_terms
            ]
        
        # Posting date range filter
        date_from = self.request.query_params.get('posting_date_from')
        date_to = self.request.query_params.get('posting_date_to')
        if date_from or date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('posting_date', ''), date_from, date_to)
            ]
        
        # Amount range filter
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Risk level filter
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            risk_levels = [level.strip().upper() for level in risk_level.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('risk_level', '').upper() in risk_levels
            ]
        
        # Day of week filter
        day_of_week = self.request.query_params.get('day_of_week')
        if day_of_week:
            days = [day.strip() for day in day_of_week.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('day_of_week', '') in days
            ]
        
        # Day type filter
        day_type = self.request.query_params.get('day_type')
        if day_type:
            if day_type.upper() == 'WEEKEND':
                filtered_entries = [
                    entry for entry in filtered_entries
                    if entry.get('day_of_week', '').lower() in ['friday', 'saturday']
                ]
            elif day_type.upper() == 'WEEKDAY':
                filtered_entries = [
                    entry for entry in filtered_entries
                    if entry.get('day_of_week', '').lower() not in ['friday', 'saturday']
                ]
        
        # High value filter
        high_value = self.request.query_params.get('high_value')
        if high_value is not None:
            is_high_value = high_value.lower() == 'true'
            filtered_entries = [
                entry for entry in filtered_entries
                if (float(entry.get('amount', 0)) > 10000000) == is_high_value
            ]
        
        # Sorting
        ordering = self.request.query_params.get('ordering', '-posting_date')
        reverse = ordering.startswith('-')
        field = ordering[1:] if reverse else ordering
        
        if field in ['amount', 'posting_date', 'risk_level', 'user', 'account', 'day_of_week']:
            try:
                filtered_entries.sort(
                    key=lambda x: (
                        float(x.get('amount', 0)) if field == 'amount' else
                        x.get('posting_date', '') if field == 'posting_date' else
                        x.get('risk_level', '') if field == 'risk_level' else
                        x.get('user', '') if field == 'user' else
                        x.get('account', '') if field == 'account' else
                        x.get('day_of_week', '')
                    ),
                    reverse=reverse
                )
            except (ValueError, TypeError):
                pass
        
        return filtered_entries
    
    def _is_date_in_range(self, posting_date, date_from, date_to):
        """Check if posting date is within the specified range"""
        try:
            if not posting_date:
                return False
            
            posting_date_obj = datetime.strptime(posting_date, '%Y-%m-%d').date()
            
            if date_from:
                from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                if posting_date_obj < from_date:
                    return False
            
            if date_to:
                to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                if posting_date_obj > to_date:
                    return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def _enhance_unusual_days(self, unusual_days):
        """Enhance unusual days entries with risk scores and additional information"""
        if not unusual_days:
            return []
        
        enhanced_entries = []
        for entry in unusual_days:
            if isinstance(entry, dict):
                enhanced_entry = entry.copy()
                
                # Add risk score if missing
                if not enhanced_entry.get('risk_score'):
                    enhanced_entry['risk_score'] = self._calculate_unusual_days_risk_score(entry)
                
                # Add risk level if missing
                if not enhanced_entry.get('risk_level'):
                    enhanced_entry['risk_level'] = self._get_unusual_days_risk_level(
                        enhanced_entry.get('risk_score', 0)
                    )
                
                enhanced_entries.append(enhanced_entry)
            else:
                enhanced_entries.append(entry)
        
        return enhanced_entries
    
    def _calculate_unusual_days_risk_score(self, entry):
        """Calculate risk score for unusual days entries based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md"""
        try:
            # Base risk score for unusual days (60 points)
            risk_score = 60.0
            
            # Factor 1: Day of week (primary factor)
            day_of_week = entry.get('day_of_week', '').lower()
            if day_of_week in ['friday', 'saturday']:
                risk_score = 100.0  # Critical - Weekend posting
            elif day_of_week == 'friday':
                risk_score = 85.0   # High - Friday posting
            else:
                risk_score = 60.0   # Medium - Other unusual days
            
            # Factor 2: High value transactions
            amount = float(entry.get('amount', 0))
            if amount > 10000000:  # > 10M
                risk_score = min(risk_score + 20.0, 100.0)
            elif amount > 1000000:  # > 1M
                risk_score = min(risk_score + 10.0, 100.0)
            
            return min(risk_score, 100.0)
        except Exception as e:
            return 60.0
    
    def _get_unusual_days_risk_level(self, risk_score):
        """Get risk level based on risk score for unusual days entries"""
        if risk_score >= 85:
            return 'CRITICAL'
        elif risk_score >= 70:
            return 'HIGH'
        elif risk_score >= 50:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def list(self, request, *args, **kwargs):
        """Return only paginated unusual days entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    

class DuplicateListView(generics.ListAPIView):
    """API view for listing duplicate entries with pagination.
    
    This view provides a paginated list of duplicate entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user: Filter by user name
    - account: Filter by account
    - posting_date_from, posting_date_to: Posting date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - duplicate_type: Filter by duplicate type (Type 1, Type 2, etc.)
    - similarity_score_range: Comma-separated min,max (e.g., similarity_score_range=0.8,1.0)
    - risk_score_range: Comma-separated min,max (e.g., risk_score_range=50,100)
    - high_value: Filter by high value transactions (true/false)
    - ordering: Field to order by (e.g., -amount, posting_date, -risk_score)
    """
    
    serializer_class = DuplicateListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get duplicate entries from DuplicateAnalysisResult"""
        file_id = self.kwargs.get('file_id')
        
        try:
            duplicate_analysis = DuplicateAnalysisResult.objects.filter(
                data_file_id=file_id,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not duplicate_analysis:
                return []
            
            duplicate_list = duplicate_analysis.duplicate_list or []
            return self._apply_filters(duplicate_list)
            
        except Exception as e:
            logger.error(f"Error fetching duplicate data: {e}")
            return []
    
    def _apply_filters(self, duplicate_list):
        """Apply filters to duplicate entries"""
        filtered_entries = []
        
        # Flatten duplicate list to individual entries
        for duplicate in duplicate_list:
            if isinstance(duplicate, dict):
                # Add transaction1
                transaction1 = duplicate.get('transaction1', {})
                if transaction1:
                    # Get the duplicate type description for display
                    duplicate_type_desc = self._get_duplicate_type_description(duplicate.get('duplicate_type', '')) or self._infer_duplicate_type(duplicate)
                    
                    # Create a modified duplicate object with the descriptive type for risk calculation
                    duplicate_for_risk = duplicate.copy()
                    duplicate_for_risk['duplicate_type'] = duplicate_type_desc
                    
                    entry = {
                        'transaction_id': transaction1.get('id', ''),
                        'document_number': transaction1.get('document_number', ''),
                        'posting_date': transaction1.get('posting_date', ''),
                        'account': transaction1.get('account', ''),
                        'amount': float(transaction1.get('amount', 0)),
                        'user': transaction1.get('user', ''),
                        'duplicate_type': duplicate_type_desc,
                        'duplicate_type_description': duplicate_type_desc,
                        'risk_level': duplicate.get('risk_level', 'LOW'),
                        'risk_score': float(duplicate.get('risk_score', 0)) or self._calculate_duplicate_risk_score(duplicate_for_risk),
                        'similarity_score': float(duplicate.get('similarity_score', 0)),
                        'matching_fields': duplicate.get('matching_fields', []),
                        'duplicate_group_id': f"{transaction1.get('id', '')}_{duplicate_type_desc}"
                    }
                    filtered_entries.append(entry)
                
                # Add transaction2
                transaction2 = duplicate.get('transaction2', {})
                if transaction2:
                    # Get the duplicate type description for display
                    duplicate_type_desc = self._get_duplicate_type_description(duplicate.get('duplicate_type', '')) or self._infer_duplicate_type(duplicate)
                    
                    # Create a modified duplicate object with the descriptive type for risk calculation
                    duplicate_for_risk = duplicate.copy()
                    duplicate_for_risk['duplicate_type'] = duplicate_type_desc
                    
                    entry = {
                        'transaction_id': transaction2.get('id', ''),
                        'document_number': transaction2.get('document_number', ''),
                        'posting_date': transaction2.get('posting_date', ''),
                        'account': transaction2.get('account', ''),
                        'amount': float(transaction2.get('amount', 0)),
                        'user': transaction2.get('user', ''),
                        'duplicate_type': duplicate_type_desc,
                        'duplicate_type_description': duplicate_type_desc,
                        'risk_level': duplicate.get('risk_level', 'LOW'),
                        'risk_score': float(duplicate.get('risk_score', 0)) or self._calculate_duplicate_risk_score(duplicate_for_risk),
                        'similarity_score': float(duplicate.get('similarity_score', 0)),
                        'matching_fields': duplicate.get('matching_fields', []),
                        'duplicate_group_id': f"{transaction2.get('id', '')}_{duplicate_type_desc}"
                    }
                    filtered_entries.append(entry)
        
        # Apply filters
        # User filter
        user_filter = self.request.query_params.get('user')
        if user_filter:
            user_terms = [term.strip().lower() for term in user_filter.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if any(term in entry.get('user', '').lower() for term in user_terms)
            ]
        
        # Account filter
        account_filter = self.request.query_params.get('account')
        if account_filter:
            account_terms = [term.strip() for term in account_filter.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('account', '') in account_terms
            ]
        
        # Posting date range filter
        date_from = self.request.query_params.get('posting_date_from')
        date_to = self.request.query_params.get('posting_date_to')
        if date_from or date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('posting_date', ''), date_from, date_to)
            ]
        
        # Amount range filter
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Risk level filter
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            risk_levels = [level.strip().upper() for level in risk_level.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('risk_level', '').upper() in risk_levels
            ]
        
        # Duplicate type filter
        duplicate_type = self.request.query_params.get('duplicate_type')
        if duplicate_type:
            types = [type_name.strip() for type_name in duplicate_type.split(',')]
            # Convert descriptive names to type codes for filtering
            type_codes = []
            for type_name in types:
                if 'Type 1' in type_name:
                    type_codes.append('type_1')
                elif 'Type 2' in type_name:
                    type_codes.append('type_2')
                elif 'Type 3' in type_name:
                    type_codes.append('type_3')
                elif 'Type 4' in type_name:
                    type_codes.append('type_4')
                elif 'Type 5' in type_name:
                    type_codes.append('type_5')
                elif 'Type 6' in type_name:
                    type_codes.append('type_6')
                else:
                    type_codes.append(type_name)
            
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('duplicate_type', '') in type_codes
            ]
        
        # Similarity score range filter
        similarity_range = self.request.query_params.get('similarity_score_range')
        if similarity_range:
            try:
                min_score, max_score = map(float, similarity_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_score <= float(entry.get('similarity_score', 0)) <= max_score
                ]
            except (ValueError, TypeError):
                pass
        
        # Risk score range filter
        risk_score_range = self.request.query_params.get('risk_score_range')
        if risk_score_range:
            try:
                min_score, max_score = map(float, risk_score_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_score <= float(entry.get('risk_score', 0)) <= max_score
                ]
            except (ValueError, TypeError):
                pass
        
        # High value filter
        high_value = self.request.query_params.get('high_value')
        if high_value is not None:
            is_high_value = high_value.lower() == 'true'
            filtered_entries = [
                entry for entry in filtered_entries
                if (float(entry.get('amount', 0)) > 10000000) == is_high_value
            ]
        
        # Sorting
        ordering = self.request.query_params.get('ordering', '-risk_score')
        reverse = ordering.startswith('-')
        field = ordering[1:] if reverse else ordering
        
        if field in ['amount', 'posting_date', 'risk_level', 'user', 'account', 'duplicate_type', 'risk_score', 'similarity_score']:
            try:
                filtered_entries.sort(
                    key=lambda x: (
                        float(x.get('amount', 0)) if field == 'amount' else
                        x.get('posting_date', '') if field == 'posting_date' else
                        x.get('risk_level', '') if field == 'risk_level' else
                        x.get('user', '') if field == 'user' else
                        x.get('account', '') if field == 'account' else
                        x.get('duplicate_type', '') if field == 'duplicate_type' else
                        float(x.get('risk_score', 0)) if field == 'risk_score' else
                        float(x.get('similarity_score', 0)) if field == 'similarity_score' else
                        0
                    ),
                    reverse=reverse
                )
            except (ValueError, TypeError):
                pass
        
        return filtered_entries
    
    def _is_date_in_range(self, posting_date, date_from, date_to):
        """Check if posting date is within the specified range"""
        try:
            if not posting_date:
                return False
            
            posting_date_obj = datetime.strptime(posting_date, '%Y-%m-%d').date()
            
            if date_from:
                from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                if posting_date_obj < from_date:
                    return False
            
            if date_to:
                to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                if posting_date_obj > to_date:
                    return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def list(self, request, *args, **kwargs):
        """Return only paginated duplicate entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    
    def _get_duplicate_type_description(self, duplicate_type_code):
        """Convert duplicate type code to descriptive name"""
        type_mapping = {
            'type_1': 'Type 1 Duplicate - Account Number + Amount',
            'type_2': 'Type 2 Duplicate - Account Number + Source + Amount',
            'type_3': 'Type 3 Duplicate - Account Number + User + Amount',
            'type_4': 'Type 4 Duplicate - Account Number + Posted Date + Amount',
            'type_5': 'Type 5 Duplicate - Account Number + Effective Date + Amount',
            'type_6': 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
        }
        return type_mapping.get(duplicate_type_code, duplicate_type_code)
    
    def _infer_duplicate_type(self, duplicate):
        """Infer duplicate type from matching fields and transaction data"""
        try:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            matching_fields = duplicate.get('matching_fields', [])
            
            # Check for Type 6: Account + Effective Date + Posted Date + User + Source + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('document_date') == transaction2.get('document_date') and
                transaction1.get('posting_date') == transaction2.get('posting_date') and
                transaction1.get('user_name') == transaction2.get('user_name') and
                transaction1.get('source') == transaction2.get('source') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
            
            # Check for Type 5: Account + Effective Date + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('document_date') == transaction2.get('document_date') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 5 Duplicate - Account Number + Effective Date + Amount'
            
            # Check for Type 4: Account + Posted Date + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('posting_date') == transaction2.get('posting_date') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 4 Duplicate - Account Number + Posted Date + Amount'
            
            # Check for Type 3: Account + User + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('user_name') == transaction2.get('user_name') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 3 Duplicate - Account Number + User + Amount'
            
            # Check for Type 2: Account + Source + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('source') == transaction2.get('source') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 2 Duplicate - Account Number + Source + Amount'
            
            # Check for Type 1: Account + Amount
            if (transaction1.get('gl_account') == transaction2.get('gl_account') and
                transaction1.get('amount') == transaction2.get('amount')):
                return 'Type 1 Duplicate - Account Number + Amount'
            
            # If no specific pattern matches, return based on matching fields
            if 'gl_account' in matching_fields and 'amount' in matching_fields:
                if 'user_name' in matching_fields and 'source' in matching_fields:
                    return 'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
                elif 'user_name' in matching_fields:
                    return 'Type 3 Duplicate - Account Number + User + Amount'
                elif 'source' in matching_fields:
                    return 'Type 2 Duplicate - Account Number + Source + Amount'
                else:
                    return 'Type 1 Duplicate - Account Number + Amount'
            
            return 'Unknown Duplicate Type'
            
        except Exception as e:
            return 'Unknown Duplicate Type'
    
    def _calculate_duplicate_risk_score(self, duplicate):
        """Calculate risk score for a duplicate entry based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md"""
        try:
            transaction1 = duplicate.get('transaction1', {})
            transaction2 = duplicate.get('transaction2', {})
            duplicate_type = duplicate.get('duplicate_type', '')
            
            # Base risk score for duplicates (40 points)
            risk_score = 40.0
            
            # Factor 1: Duplicate type (primary factor)
            if 'Type 6' in duplicate_type:
                risk_score = 100.0  # Critical
            elif 'Type 5' in duplicate_type:
                risk_score = 85.0   # High
            elif 'Type 4' in duplicate_type:
                risk_score = 75.0   # Medium-High
            elif 'Type 3' in duplicate_type:
                risk_score = 65.0   # Medium
            elif 'Type 2' in duplicate_type:
                risk_score = 55.0   # Medium-Low
            elif 'Type 1' in duplicate_type:
                risk_score = 45.0   # Low
            else:
                risk_score = 40.0   # Default
            
            return min(risk_score, 100.0)
        except Exception as e:
            return 40.0


class UserListView(generics.ListAPIView):
    """API view for listing user entries with pagination.
    
    This view provides a paginated list of user entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user: Filter by user name
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - risk_score_range: Comma-separated min,max (e.g., risk_score_range=50,100)
    - transaction_count_range: Comma-separated min,max (e.g., transaction_count_range=10,100)
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - anomaly_count_range: Comma-separated min,max (e.g., anomaly_count_range=1,10)
    - activity_category: Filter by activity category (LOW, MEDIUM, HIGH)
    - user_severity: Filter by user severity (LOW, MEDIUM, HIGH, CRITICAL)
    - high_activity: Filter by high activity users (true/false)
    - ordering: Field to order by (e.g., -risk_score, transaction_count, -total_amount)
    """
    
    serializer_class = UserListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get user entries from UserAnalysisResult"""
        file_id = self.kwargs.get('file_id')
        
        try:
            user_analysis = UserAnalysisResult.objects.filter(
                data_file_id=file_id,
                status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not user_analysis:
                return []
            
            user_transaction_summary = user_analysis.user_transaction_summary or []
            user_anomalies = user_analysis.user_anomalies or []
            user_risk_assessment = user_analysis.user_risk_assessment or {}
            
            return self._apply_filters(user_transaction_summary, user_anomalies, user_risk_assessment)
            
        except Exception as e:
            logger.error(f"Error fetching user data: {e}")
            return []
    
    def _apply_filters(self, user_transaction_summary, user_anomalies, user_risk_assessment):
        """Apply filters to user entries"""
        filtered_entries = []
        
        # Create a mapping of user anomalies and risk assessment
        anomalies_by_user = {}
        risk_by_user = {}
        
        # Process anomalies
        for anomaly in user_anomalies:
            if isinstance(anomaly, dict):
                user = anomaly.get('user', '')
                if user not in anomalies_by_user:
                    anomalies_by_user[user] = []
                anomalies_by_user[user].append(anomaly)
        
        # Process risk assessment
        if isinstance(user_risk_assessment, list):
            for risk in user_risk_assessment:
                if isinstance(risk, dict):
                    user = risk.get('user', '')
                    risk_by_user[user] = risk
        elif isinstance(user_risk_assessment, dict):
            # Try different possible structures
            user_risk_scores = user_risk_assessment.get('user_risk_scores', [])
            if not user_risk_scores:
                user_risk_scores = user_risk_assessment.get('user_risk_assessment', [])
            if not user_risk_scores:
                # If it's a direct mapping, try to extract user data
                for key, value in user_risk_assessment.items():
                    if isinstance(value, dict) and 'user' in value:
                        user = value.get('user', '')
                        risk_by_user[user] = value
            
            for risk in user_risk_scores:
                if isinstance(risk, dict):
                    user = risk.get('user', '')
                    risk_by_user[user] = risk
        
        # Combine user transaction summary with anomalies and risk data
        for user_data in user_transaction_summary:
            if isinstance(user_data, dict):
                user = user_data.get('user', '')
                
                # Get anomaly count for this user
                anomaly_count = len(anomalies_by_user.get(user, []))
                
                # Get risk data for this user
                risk_data = risk_by_user.get(user, {})
                
                # Try different possible field names for risk level and score
                risk_level = risk_data.get('risk_level', risk_data.get('level', 'LOW'))
                risk_score = risk_data.get('risk_score', risk_data.get('score', 0.0))
                
                # If risk_score is still 0, try to calculate from other fields
                if float(risk_score) == 0.0:
                    # Try to get risk score from different possible sources
                    if 'risk_factors' in risk_data:
                        # Calculate risk score based on risk factors (following RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md)
                        risk_factors = risk_data.get('risk_factors', [])
                        # Base risk score for users (30 points)
                        risk_score = 30.0
                        # Add points based on risk factors
                        risk_score += len(risk_factors) * 15.0  # 15 points per risk factor
                    elif 'anomaly_type' in risk_data:
                        # Calculate risk score based on anomaly type
                        anomaly_type = risk_data.get('anomaly_type', '')
                        if 'HIGH_ACTIVITY' in anomaly_type:
                            risk_score = 75.0  # High
                        elif 'MEDIUM_ACTIVITY' in anomaly_type:
                            risk_score = 55.0  # Medium
                        else:
                            risk_score = 35.0  # Low
                    else:
                        # Default risk score for users
                        risk_score = 30.0
                
                # Ensure risk level matches the score (following documented methodology)
                if float(risk_score) >= 85:
                    risk_level = 'CRITICAL'
                elif float(risk_score) >= 65:
                    risk_level = 'HIGH'
                elif float(risk_score) >= 45:
                    risk_level = 'MEDIUM'
                else:
                    risk_level = 'LOW'
                
                entry = {
                    'user': user,
                    'transaction_count': user_data.get('transaction_count', 0),
                    'total_amount': float(user_data.get('total_amount', 0)),
                    'avg_amount': float(user_data.get('avg_amount', 0)),
                    'risk_level': risk_level,
                    'risk_score': float(risk_score),
                    'anomaly_count': anomaly_count,
                    'accounts': user_data.get('accounts', [])
                }
                filtered_entries.append(entry)
        
        # Apply filters
        # User filter
        user_filter = self.request.query_params.get('user')
        if user_filter:
            user_terms = [term.strip().lower() for term in user_filter.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if any(term in entry.get('user', '').lower() for term in user_terms)
            ]
        
        # Risk level filter
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            risk_levels = [level.strip().upper() for level in risk_level.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if entry.get('risk_level', '').upper() in risk_levels
            ]
        
        # Risk score range filter
        risk_score_range = self.request.query_params.get('risk_score_range')
        if risk_score_range:
            try:
                min_score, max_score = map(float, risk_score_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_score <= float(entry.get('risk_score', 0)) <= max_score
                ]
            except (ValueError, TypeError):
                pass
        
        # Transaction count range filter
        transaction_count_range = self.request.query_params.get('transaction_count_range')
        if transaction_count_range:
            try:
                min_count, max_count = map(int, transaction_count_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_count <= int(entry.get('transaction_count', 0)) <= max_count
                ]
            except (ValueError, TypeError):
                pass
        
        # Amount range filter
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('total_amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Anomaly count range filter
        anomaly_count_range = self.request.query_params.get('anomaly_count_range')
        if anomaly_count_range:
            try:
                min_count, max_count = map(int, anomaly_count_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_count <= int(entry.get('anomaly_count', 0)) <= max_count
                ]
            except (ValueError, TypeError):
                pass
        
        # Activity category filter
        activity_category = self.request.query_params.get('activity_category')
        if activity_category:
            categories = [cat.strip().upper() for cat in activity_category.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if self._get_activity_category(entry) in categories
            ]
        
        # User severity filter
        user_severity = self.request.query_params.get('user_severity')
        if user_severity:
            severities = [sev.strip().upper() for sev in user_severity.split(',')]
            filtered_entries = [
                entry for entry in filtered_entries
                if self._get_user_severity(entry) in severities
            ]
        
        # High activity filter
        high_activity = self.request.query_params.get('high_activity')
        if high_activity is not None:
            is_high_activity = high_activity.lower() == 'true'
            filtered_entries = [
                entry for entry in filtered_entries
                if (int(entry.get('transaction_count', 0)) > 50) == is_high_activity
            ]
        
        # Sorting
        ordering = self.request.query_params.get('ordering', '-risk_score')
        reverse = ordering.startswith('-')
        field = ordering[1:] if reverse else ordering
        
        if field in ['risk_score', 'transaction_count', 'total_amount', 'avg_amount', 'anomaly_count', 'user']:
            try:
                filtered_entries.sort(
                    key=lambda x: (
                        float(x.get('risk_score', 0)) if field == 'risk_score' else
                        int(x.get('transaction_count', 0)) if field == 'transaction_count' else
                        float(x.get('total_amount', 0)) if field == 'total_amount' else
                        float(x.get('avg_amount', 0)) if field == 'avg_amount' else
                        int(x.get('anomaly_count', 0)) if field == 'anomaly_count' else
                        x.get('user', '')
                    ),
                    reverse=reverse
                )
            except (ValueError, TypeError):
                pass
        
        return filtered_entries
    
    def _get_activity_category(self, entry):
        """Get activity category based on transaction count"""
        transaction_count = int(entry.get('transaction_count', 0))
        if transaction_count > 100:
            return 'HIGH'
        elif transaction_count > 20:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_user_severity(self, entry):
        """Get user severity level based on risk score and anomalies"""
        risk_score = float(entry.get('risk_score', 0))
        anomaly_count = int(entry.get('anomaly_count', 0))
        
        if risk_score > 80 or anomaly_count > 10:
            return 'CRITICAL'
        elif risk_score > 60 or anomaly_count > 5:
            return 'HIGH'
        elif risk_score > 40 or anomaly_count > 2:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def list(self, request, *args, **kwargs):
        """Return only paginated user entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)


class HolidayListView(generics.ListAPIView):
    """API view for listing holiday entries with pagination.
    
    This view provides a paginated list of holiday entries for a specific file.
    
    Query Parameters:
    - page, page_size: Pagination controls
    - user_name: Filter by user name
    - account: Filter by account
    - date_from, date_to: Posting date range
    - amount_range: Comma-separated min,max (e.g., amount_range=1000,5000)
    - risk_level: Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
    - holiday_type: Filter by holiday type (Public holiday, Observance, etc.)
    - holiday_name: Filter by holiday name
    - high_value: Filter by high value transactions (true/false)
    - ordering: Field to order by (e.g., -amount, posting_date, -risk_level)
    """
    
    serializer_class = HolidayListSerializer
    pagination_class = SAPGLPostingPagination
    
    def get_queryset(self):
        """Get holiday entries for the specified file"""
        file_id = self.kwargs.get('file_id')
        
        try:
            # Get the latest holiday analysis result
            from .models import HolidayAnalysisResult
            holiday_analysis = HolidayAnalysisResult.objects.filter(
                data_file_id=file_id, status='COMPLETED'
            ).order_by('-analysis_date').first()
            
            if not holiday_analysis:
                return []
            
            # Get holiday entries from the analysis
            holiday_entries = holiday_analysis.holiday_postings or []
            
            # Apply filters
            filtered_entries = self._apply_filters(holiday_entries)
            
            return filtered_entries
            
        except Exception as e:
            logger.error(f"Error fetching holiday data: {e}")
            return []
    
    def _apply_filters(self, holiday_entries):
        """Apply filters to holiday entries based on actual data structure"""
        filtered_entries = holiday_entries
        
        # Filter by user name
        user_name = self.request.query_params.get('user_name')
        if user_name:
            filtered_entries = [
                entry for entry in filtered_entries 
                if user_name.lower() in entry.get('user_name', '').lower()
            ]
        
        # Filter by account
        account = self.request.query_params.get('account')
        if account:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('gl_account') == account
            ]
        
        # Filter by posting date range
        date_from = self.request.query_params.get('date_from')
        date_to = self.request.query_params.get('date_to')
        if date_from or date_to:
            filtered_entries = [
                entry for entry in filtered_entries
                if self._is_date_in_range(entry.get('posting_date'), date_from, date_to)
            ]
        
        # Filter by amount range
        amount_range = self.request.query_params.get('amount_range')
        if amount_range:
            try:
                min_amount, max_amount = map(float, amount_range.split(','))
                filtered_entries = [
                    entry for entry in filtered_entries
                    if min_amount <= float(entry.get('amount', 0)) <= max_amount
                ]
            except (ValueError, TypeError):
                pass
        
        # Filter by risk level
        risk_level = self.request.query_params.get('risk_level')
        if risk_level:
            filtered_entries = [
                entry for entry in filtered_entries 
                if entry.get('risk_level', '').upper() == risk_level.upper()
            ]
        
        # Filter by holiday type
        holiday_type = self.request.query_params.get('holiday_type')
        if holiday_type:
            filtered_entries = [
                entry for entry in filtered_entries 
                if holiday_type.lower() in entry.get('holiday_type', '').lower()
            ]
        
        # Filter by holiday name
        holiday_name = self.request.query_params.get('holiday_name')
        if holiday_name:
            filtered_entries = [
                entry for entry in filtered_entries 
                if holiday_name.lower() in entry.get('holiday_name', '').lower()
            ]
        
        # Filter by high value
        high_value = self.request.query_params.get('high_value')
        if high_value is not None:
            is_high_value = high_value.lower() == 'true'
            filtered_entries = [
                entry for entry in filtered_entries
                if float(entry.get('amount', 0)) > 10000000 == is_high_value
            ]
        
        # Apply ordering
        ordering = self.request.query_params.get('ordering', '-amount')
        if ordering:
            reverse = ordering.startswith('-')
            field = ordering[1:] if reverse else ordering
            
            if field == 'amount':
                filtered_entries.sort(key=lambda x: float(x.get('amount', 0)), reverse=reverse)
            elif field == 'posting_date':
                filtered_entries.sort(key=lambda x: x.get('posting_date', ''), reverse=reverse)
            elif field == 'risk_level':
                risk_order = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3, 'CRITICAL': 4}
                filtered_entries.sort(key=lambda x: risk_order.get(x.get('risk_level', 'LOW'), 0), reverse=reverse)
            elif field == 'risk_score':
                filtered_entries.sort(key=lambda x: float(x.get('risk_score', 0)), reverse=reverse)
        
        return filtered_entries
    
    def _is_date_in_range(self, posting_date, date_from, date_to):
        """Check if posting date is within the specified range"""
        try:
            if isinstance(posting_date, str):
                posting_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
            
            if date_from:
                from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                if posting_date < from_date:
                    return False
            
            if date_to:
                to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                if posting_date > to_date:
                    return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def list(self, request, *args, **kwargs):
        """Return only paginated holiday entries listing"""
        queryset = self.get_queryset()
        
        # Apply pagination
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)


class AnalysisExportView(generics.GenericAPIView):
    """API view for exporting transactions from any specific analysis type as CSV or XLSX"""
    
    def get(self, request, file_id, analysis_type):
        """
        Export transactions from a specific analysis type
        
        Parameters:
        - file_id: UUID of the data file
        - analysis_type: Type of analysis (duplicate, backdated, holiday, closing_entries, unusual_days, user)
        - format: Export format (csv or xlsx) - defaults to xlsx
        - limit: Maximum number of records to export (default: 10000)
        """
        try:
            # Get the data file
            data_file = DataFile.objects.get(id=file_id)
            
            # Get export parameters
            export_format = request.query_params.get('format', 'xlsx').lower()
            limit_param = request.query_params.get('limit')
            limit = int(limit_param) if limit_param else None
            
            # Validate format
            if export_format not in ['csv', 'xlsx']:
                return Response(
                    {'error': 'Invalid format. Use "csv" or "xlsx"'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get analysis data based on type
            analysis_data = self._get_analysis_data(data_file, analysis_type)
            
            if not analysis_data:
                return Response(
                    {'error': f'No data found for analysis type: {analysis_type}'}, 
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Limit the data only if limit parameter is provided
            if limit is not None and limit > 0:
                analysis_data = analysis_data[:limit]
            
            # Export the data
            if export_format == 'csv':
                return self._export_csv(analysis_data, data_file, analysis_type)
            else:
                return self._export_xlsx(analysis_data, data_file, analysis_type)
                
        except DataFile.DoesNotExist:
            return Response(
                {'error': 'Data file not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error exporting {analysis_type} data: {e}")
            return Response(
                {'error': f'Error exporting data: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_analysis_data(self, data_file, analysis_type):
        """Get analysis data based on type"""
        if analysis_type == 'duplicate':
            analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.duplicate_list if analysis else []
            
        elif analysis_type == 'backdated':
            analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.backdated_entries if analysis else []
            
        elif analysis_type == 'holiday':
            analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.holiday_postings if analysis else []
            
        elif analysis_type == 'closing_entries':
            analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.closing_entries if analysis else []
            
        elif analysis_type == 'unusual_days':
            analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.unusual_days if analysis else []
            
        elif analysis_type == 'user':
            analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
            return analysis.user_anomalies if analysis else []
            
        else:
            return None
    
    def _export_csv(self, data, data_file, analysis_type):
        """Export data as CSV"""
        import csv
        from django.http import HttpResponse
        
        # Create response
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{data_file.client_name}_{analysis_type}_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        
        if not data:
            return response
        
        # Get headers from first record
        headers = list(data[0].keys())
        
        # Write CSV
        writer = csv.DictWriter(response, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
        
        return response
    
    def _export_xlsx(self, data, data_file, analysis_type):
        """Export data as XLSX"""
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        from django.http import HttpResponse
        import io
        
        # Create workbook
        wb = Workbook()
        ws = wb.active
        ws.title = f"{analysis_type.replace('_', ' ').title()}"
        
        if not data:
            # Create empty file with headers
            response = HttpResponse(
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = f'attachment; filename="{data_file.client_name}_{analysis_type}_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
            
            # Save to response
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            response.write(output.getvalue())
            return response
        
        # Get headers from first record
        headers = list(data[0].keys())
        
        # Write headers
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
        
        # Write data
        for row, record in enumerate(data, 2):
            for col, header in enumerate(headers, 1):
                value = record.get(header, '')
                ws.cell(row=row, column=col, value=value)
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            ws.column_dimensions[column_letter].width = adjusted_width
        
        # Create response
        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{data_file.client_name}_{analysis_type}_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
        
        # Save to response
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        response.write(output.getvalue())
        
        return response


class ExcelExportView(generics.GenericAPIView):
    """API view for exporting comprehensive audit reports to Excel"""
    
    def get(self, request, file_id):
        """Generate and download comprehensive Excel audit report"""
        try:
            # Get data file
            data_file = DataFile.objects.get(id=file_id)
            
            # DEBUG: Print basic file info
            print("=" * 80)
            print("DEBUG: EXCEL EXPORT DATA STRUCTURE")
            print("=" * 80)
            print(f"File ID: {file_id}")
            print(f"Data File: {data_file.file_name}")
            print(f"Client Name: {data_file.client_name}")
            print(f"Company Name: {data_file.company_name}")
            print(f"Fiscal Year: {data_file.fiscal_year}")
            print()
            
            # Collect all analysis results
            analysis_results = {}
            
            # Get duplicate analysis
            duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
            if duplicate_analysis:
                analysis_results['duplicate_analysis'] = {
                    'duplicate_list': duplicate_analysis.duplicate_list or [],
                    'analysis_info': duplicate_analysis.analysis_info or {},
                    'chart_data': duplicate_analysis.chart_data or {},
                    'audit_recommendations': duplicate_analysis.audit_recommendations or {}
                }
                print(f"DUPLICATE ANALYSIS: {len(duplicate_analysis.duplicate_list or [])} items")
            else:
                print("DUPLICATE ANALYSIS: Not found")
            
            # Get backdated analysis
            backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
            if backdated_analysis:
                analysis_results['backdated_analysis'] = {
                    'backdated_entries': backdated_analysis.backdated_entries or [],
                    'analysis_info': backdated_analysis.analysis_info or {},
                    'chart_data': backdated_analysis.chart_data or {},
                    'audit_recommendations': backdated_analysis.audit_recommendations or {}
                }
                print(f"BACKDATED ANALYSIS: {len(backdated_analysis.backdated_entries or [])} items")
            else:
                print("BACKDATED ANALYSIS: Not found")
            
            # Get closing entries analysis
            closing_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
            if closing_analysis:
                analysis_results['closing_entries_analysis'] = {
                    'closing_entries': closing_analysis.closing_entries or [],
                    'analysis_info': closing_analysis.analysis_info or {},
                    'chart_data': closing_analysis.chart_data or {},
                    'audit_recommendations': getattr(closing_analysis, 'audit_recommendations', {})
                }
                print(f"CLOSING ENTRIES ANALYSIS: {len(closing_analysis.closing_entries or [])} items")
            else:
                print("CLOSING ENTRIES ANALYSIS: Not found")
            
            # Get holiday analysis
            holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
            if holiday_analysis:
                analysis_results['holiday_analysis'] = {
                    'holiday_postings': holiday_analysis.holiday_postings or [],
                    'analysis_info': holiday_analysis.analysis_info or {},
                    'chart_data': holiday_analysis.chart_data or {},
                    'audit_recommendations': holiday_analysis.audit_recommendations or {}
                }
                print(f"HOLIDAY ANALYSIS: {len(holiday_analysis.holiday_postings or [])} items")
            else:
                print("HOLIDAY ANALYSIS: Not found")
            
            # Get user analysis
            user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
            if user_analysis:
                analysis_results['user_analysis'] = {
                    'user_transaction_summary': user_analysis.user_transaction_summary or [],
                    'user_anomalies': user_analysis.user_anomalies or [],
                    'user_risk_assessment': user_analysis.user_risk_assessment or {},
                    'chart_data': user_analysis.chart_data or {},
                    'audit_recommendations': getattr(user_analysis, 'audit_recommendations', {})
                }
                print(f"USER ANALYSIS: {len(user_analysis.user_transaction_summary or [])} users, {len(user_analysis.user_anomalies or [])} anomalies")
            else:
                print("USER ANALYSIS: Not found")
            
            # Get unusual days analysis
            unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
            if unusual_days_analysis:
                analysis_results['unusual_days_analysis'] = {
                    'unusual_days': unusual_days_analysis.unusual_days or [],
                    'analysis_info': unusual_days_analysis.analysis_info or {},
                    'chart_data': unusual_days_analysis.chart_data or {},
                    'audit_recommendations': getattr(unusual_days_analysis, 'audit_recommendations', {})
                }
                print(f"UNUSUAL DAYS ANALYSIS: {len(unusual_days_analysis.unusual_days or [])} items")
            else:
                print("UNUSUAL DAYS ANALYSIS: Not found")
            
            # Get overall analysis
            overall_analysis = OverallAnalysisResult.objects.filter(data_file=data_file).first()
            if overall_analysis:
                analysis_results['overall_analysis'] = {
                    'flagged_transactions': overall_analysis.flagged_transactions or [],
                    'flag_summary': overall_analysis.flag_summary or {},
                    'risk_assessment': overall_analysis.risk_assessment or {},
                    'chart_data': overall_analysis.chart_data or {},
                    'export_data': overall_analysis.export_data or {}
                }
                print(f"OVERALL ANALYSIS: {len(overall_analysis.flagged_transactions or [])} flagged items")
            else:
                print("OVERALL ANALYSIS: Not found")
            
            # Get risk analysis
            risk_analysis = RiskScoringDocument.objects.filter(data_file=data_file).first()
            if risk_analysis:
                analysis_results['risk_analysis'] = {
                    'overall_risk_score': risk_analysis.overall_risk_score,
                    'risk_level': risk_analysis.get_risk_level(),
                    'methodology_overview': risk_analysis.methodology_overview or {},
                    'risk_factors': risk_analysis.risk_factors or {},
                    'recommendations': risk_analysis.recommendations or {}
                }
                print(f"RISK ANALYSIS: Score {risk_analysis.overall_risk_score}, Level {risk_analysis.get_risk_level()}")
            else:
                print("RISK ANALYSIS: Not found")
            
            # Calculate total anomalies for debugging
            total_anomalies = 0
            anomaly_breakdown = {}
            
            if 'duplicate_analysis' in analysis_results:
                count = len(analysis_results['duplicate_analysis']['duplicate_list'])
                anomaly_breakdown['duplicates'] = count
                total_anomalies += count
            
            if 'backdated_analysis' in analysis_results:
                count = len(analysis_results['backdated_analysis']['backdated_entries'])
                anomaly_breakdown['backdated'] = count
                total_anomalies += count
            
            if 'closing_entries_analysis' in analysis_results:
                count = len(analysis_results['closing_entries_analysis']['closing_entries'])
                anomaly_breakdown['closing_entries'] = count
                total_anomalies += count
            
            if 'holiday_analysis' in analysis_results:
                count = len(analysis_results['holiday_analysis']['holiday_postings'])
                anomaly_breakdown['holiday'] = count
                total_anomalies += count
            
            if 'unusual_days_analysis' in analysis_results:
                count = len(analysis_results['unusual_days_analysis']['unusual_days'])
                anomaly_breakdown['unusual_days'] = count
                total_anomalies += count
            
            if 'user_analysis' in analysis_results:
                count = len(analysis_results['user_analysis']['user_anomalies'])
                anomaly_breakdown['user_anomalies'] = count
                total_anomalies += count
            
            print(f"\nTOTAL ANOMALIES CALCULATED: {total_anomalies}")
            print(f"ANOMALY BREAKDOWN: {anomaly_breakdown}")
            print("=" * 80)
            print()
            
            # Generate Excel file
            exporter = AuditExcelExporter()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"audit_report_{data_file.client_name}_{timestamp}.xlsx"
            output_path = f"temp_uploads/{filename}"
            
            # Create Excel report
            exporter.create_audit_report(data_file, analysis_results, output_path)
            
            # Return file download response with automatic cleanup
            from django.http import StreamingHttpResponse
            import os
            
            if os.path.exists(output_path):
                def file_iterator():
                    with open(output_path, 'rb') as f:
                        while True:
                            chunk = f.read(8192)  # 8KB chunks
                            if not chunk:
                                break
                            yield chunk
                    
                    # Delete file after streaming is complete
                    try:
                        os.remove(output_path)
                        logger.info(f"Temporary file deleted: {output_path}")
                    except Exception as e:
                        logger.error(f"Error deleting temporary file {output_path}: {e}")
                
                response = StreamingHttpResponse(
                    file_iterator(),
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                response['Content-Disposition'] = f'attachment; filename="{filename}"'
                response['Content-Length'] = os.path.getsize(output_path)
                
                return response
            else:
                return Response(
                    {'error': 'Failed to generate Excel file'}, 
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
                
        except DataFile.DoesNotExist:
            return Response(
                {'error': 'Data file not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error generating Excel export: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )