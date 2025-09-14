"""
Core views for file upload functionality - Refactored with utility functions
Supports multiple file uploads (trial_balance, chart_of_accounts, gl_accounts)
"""

from rest_framework import status, generics
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
import logging
from uuid import uuid4
from django.utils import timezone

from .models import DataFile
from .serializers import DataFileUploadSerializer
from .file_processing_utils import FileProcessingManager

logger = logging.getLogger(__name__)


class FileUploadView(generics.CreateAPIView):
    """
    View for uploading multiple files containing SAP data
    Handles three file types: General Ledger (GL), Trial Balance (TB), and Chart of Accounts
    Supports multiple files in a single request
    """
    
    parser_classes = (MultiPartParser, FormParser)
    
    def create(self, request, *args, **kwargs):
        """
        Handle multiple file uploads with synchronous processing for better reliability:
        - All files (GL Listing, TB, Chart of Accounts): Processed synchronously from memory
        - No disk saving required - files processed directly from uploaded data
        - Immediate feedback and better error handling compared to background tasks
        """
        try:
            # Validate and extract data
            validation_result = self._validate_request(request)
            if not validation_result['success']:
                return Response(validation_result['error'], status=status.HTTP_400_BAD_REQUEST)
            
            files = validation_result['files']
            metadata = validation_result['metadata']
            
            # Process each file based on its type
            results = []
            for file_type, file_obj in files.items():
                try:
                    # Create DataFile record for each file
                    data_file = self._create_data_file_record(file_obj, metadata, file_type)
                    
                    # Process files based on type and size
                    if file_type == 'gl_accounts':
                        # GL Listing: Check size and use threading for large files
                        result = self._process_gl_file_with_threading(data_file, file_obj)
                    else:
                        # TB and Chart of Accounts: Process synchronously from memory
                        result = self._process_file_sync(data_file, file_obj, file_type)
                    
                    results.append(result)
                        
                except Exception as e:
                    logger.error(f"Error processing {file_type} file: {e}")
                    results.append({
                        'file_type': file_type,
                        'file_name': file_obj.name,
                        'status': 'failed',
                        'error': str(e)
                    })
            
            # Return success response
            return self._create_success_response(results)
                
        except Exception as e:
            logger.error(f"Error in file upload: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _validate_request(self, request):
        """
        Validate request data and extract files and metadata
        
        Args:
            request: HTTP request object
            
        Returns:
            Dict: Validation result with files and metadata
        """
        # Get files
        files = {}
        file_types = ['trial_balance', 'chart_of_accounts', 'gl_accounts']
        
        for file_type in file_types:
            file_obj = request.FILES.get(file_type)
            if file_obj:
                # Validate file type
                file_extension = file_obj.name.lower().split('.')[-1]
                if file_extension not in ['csv', 'xlsx', 'xls', 'xlsb']:
                    return {'success': False, 'error': {'error': f'Only CSV and Excel files are supported for {file_type}'}}
                files[file_type] = file_obj
        
        if not files:
            return {'success': False, 'error': {'error': 'No files provided. Expected: trial_balance, chart_of_accounts, gl_accounts'}}
        
        # Extract metadata from request data
        metadata = {
            'engagement_id': request.data.get('engagement_id', ''),
            'client_name': request.data.get('client_name', ''),
            'company_name': request.data.get('company_name', ''),
            'fiscal_year': request.data.get('fiscal_year', 2025),
            'audit_start_date': request.data.get('audit_start_date'),
            'audit_end_date': request.data.get('audit_end_date'),
            'description': request.data.get('description', '')
        }
        
        return {'success': True, 'files': files, 'metadata': metadata}
    
    def _create_data_file_record(self, file_obj, metadata, file_type):
        """
        Create DataFile record from file and metadata
        
        Args:
            file_obj: Uploaded file object
            metadata: File metadata dictionary
            file_type: Type of file (trial_balance, chart_of_accounts, gl_accounts)
            
        Returns:
            DataFile: Created DataFile instance
        """
        return DataFile.objects.create(
            file_name=file_obj.name,
            file_size=file_obj.size,
            engagement_id=metadata['engagement_id'],
            client_name=metadata['client_name'],
            company_name=metadata['company_name'],
            fiscal_year=metadata['fiscal_year'],
            audit_start_date=metadata['audit_start_date'],
            audit_end_date=metadata['audit_end_date'],
            status='PENDING'
        )
    
    def _create_success_response(self, results):
        """
        Create success response with file and task information
        
        Args:
            results: List of processing results for each file
            
        Returns:
            Response: Success response
        """
        successful_uploads = [r for r in results if r['status'] == 'success']
        failed_uploads = [r for r in results if r['status'] == 'failed']
        
        response_data = {
            'message': f'File upload completed. {len(successful_uploads)} successful, {len(failed_uploads)} failed.',
            'status': 'success' if not failed_uploads else 'partial_success',
            'total_files': len(results),
            'successful_uploads': len(successful_uploads),
            'failed_uploads': len(failed_uploads),
            'results': results
        }
        
        return Response(response_data, status=status.HTTP_201_CREATED)
    
    def _save_file_to_disk(self, data_file: DataFile, file_obj) -> str:
        """
        Save uploaded file to disk for background processing
        
        Args:
            data_file: DataFile instance
            file_obj: Uploaded file object
            
        Returns:
            str: Path to saved file
        """
        import os
        from django.conf import settings
        
        # Create temp_uploads directory if it doesn't exist
        temp_dir = os.path.join(settings.BASE_DIR, 'temp_uploads')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Generate unique filename with timestamp for extra safety
        file_extension = file_obj.name.split('.')[-1]
        timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{data_file.id}_{timestamp}.{file_extension}"
        file_path = os.path.join(temp_dir, filename)
        
        # Save file to disk with proper error handling
        try:
            with open(file_path, 'wb') as destination:
                for chunk in file_obj.chunks():
                    destination.write(chunk)
            
            # Verify file was saved correctly
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                logger.info(f"File saved to disk: {file_path} ({os.path.getsize(file_path)} bytes)")
                return file_path
            else:
                raise Exception("File was not saved correctly or is empty")
                
        except Exception as e:
            # Clean up partial file if it exists
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise Exception(f"Failed to save file to disk: {e}")
    
    # NOTE: _create_processing_job method removed - no longer needed since all files
    # are processed synchronously for better reliability. Background task processing
    # was causing database connection issues with large files.
    
    def _process_gl_file_with_threading(self, data_file: DataFile, file_obj):
        """
        Process GL file with intelligent threading strategy:
        - Small files (< 10k rows): Process synchronously
        - Large files (>= 10k rows): Process in background thread
        """
        try:
            # Check file size first
            file_obj.seek(0)
            from .file_processing_utils import FileReader
            file_reader = FileReader()
            df = file_reader.read_file(file_obj)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            row_count = len(df)
            logger.info(f"GL file {data_file.file_name} has {row_count} rows")
            
            if row_count < 10000:
                # Small file: process synchronously
                return self._process_gl_file_sync(data_file, file_obj)
            else:
                # Large file: process in background thread
                return self._process_gl_large_file_threaded(data_file, file_obj, row_count)
                
        except Exception as e:
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            raise e
    
    def _process_gl_large_file_threaded(self, data_file: DataFile, file_obj, row_count: int):
        """Process large GL files in background thread"""
        import threading
        import tempfile
        import os
        
        try:
            # Save file to temporary location
            file_extension = file_obj.name.split('.')[-1]
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_extension}')
            
            file_obj.seek(0)
            for chunk in file_obj.chunks():
                temp_file.write(chunk)
            temp_file.close()
            
            # Update data file status
            data_file.status = 'PROCESSING'
            data_file.total_records = row_count
            data_file.save()
            
            # Start background thread
            thread = threading.Thread(
                target=self._process_gl_background_thread,
                args=(data_file, temp_file.name, row_count)
            )
            thread.daemon = True
            thread.start()
            
            logger.info(f"Started background processing for large GL file {data_file.file_name} ({row_count} rows)")
            
            return {
                'file_type': 'gl_accounts',
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'processing',
                'total_records': row_count,
                'processed_records': 0,
                'failed_records': 0,
                'message': f"GL Listing processing started for {row_count} records. Check status via API."
            }
            
        except Exception as e:
            if 'temp_file' in locals() and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise e
    
    def _process_gl_background_thread(self, data_file: DataFile, file_path: str, row_count: int):
        """Background thread for processing large GL files"""
        import os
        from .file_processing_utils import DataProcessor
        
        try:
            logger.info(f"Background thread started for GL file {data_file.file_name}")
            
            # Process using chunked processing
            processor = DataProcessor(data_file)
            result = processor.process_gl_data_chunked(file_path, chunk_size=5000)
            
            # Update data file with results
            data_file.status = 'COMPLETED'
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.info(f"Background processing completed for GL file {data_file.file_name}: {result}")
            
            # Trigger completeness test as Celery task after background GL processing completes
            try:
                from .tasks import run_gl_completeness_analysis, train_ai_completeness_model
                completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                logger.info(f"Completeness analysis queued after background processing: {completeness_task.id}")
                
                # Check for AI training opportunity
                from .models import CompletenessTestResult
                client_tests_count = CompletenessTestResult.objects.filter(
                    data_file__client_name__icontains=data_file.client_name
                ).count()
                
                if client_tests_count >= 10:
                    try:
                        ai_training_task = train_ai_completeness_model.delay(
                            model_type='COMPLETENESS_PREDICTOR',
                            client_name=data_file.client_name
                        )
                        logger.info(f"🤖 AI model training queued for client {data_file.client_name}: {ai_training_task.id}")
                    except Exception as ai_error:
                        logger.warning(f"🤖 Could not queue AI training: {ai_error}")
                        
            except Exception as e:
                logger.warning(f"Could not queue completeness analysis after background processing: {e}")
            
        except Exception as e:
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            logger.error(f"Background processing failed for GL file {data_file.file_name}: {e}")
            
        finally:
            # Clean up temporary file
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    logger.info(f"Cleaned up temporary file: {file_path}")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file {file_path}: {cleanup_error}")
    
    def _process_gl_file_sync(self, data_file: DataFile, file_obj):
        """
        Process GL Listing file synchronously for better reliability
        
        Why synchronous instead of background tasks?
        1. Database Connection Issues: Background tasks can lose DB connections with large files
        2. Immediate Feedback: Users get instant processing results and error messages
        3. File Management: No need to save/delete files from disk
        4. Error Handling: Better error propagation and debugging
        5. Simplicity: Direct processing from memory is more straightforward
        
        Files are processed directly from memory - no disk saving/deletion
        
        Args:
            data_file: DataFile instance
            file_obj: Uploaded file object
            
        Returns:
            Dict: Processing result
        """
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Process the file synchronously directly from memory (same as TB and Chart)
            from .file_processing_utils import FileReader, FileTypeDetector, DataProcessor
            
            # Read the file directly from memory (no disk saving)
            file_reader = FileReader()
            df = file_reader.read_file(file_obj)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Process the GL data
            processor = DataProcessor(data_file)
            result = processor.process_gl_data(df)
            
            # Update data file with results
            data_file.status = 'COMPLETED'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.info(f"Successfully processed GL file {data_file.file_name} from memory: {result}")
            
            # Trigger completeness test as Celery task after GL processing completes
            try:
                from .tasks import run_gl_completeness_analysis, train_ai_completeness_model
                completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                logger.info(f"Completeness analysis queued: {completeness_task.id}")
                completeness_queued = True
                
                # Optionally trigger AI model training for client-specific patterns
                # Check if we have enough historical data for the client
                from .models import CompletenessTestResult
                client_tests_count = CompletenessTestResult.objects.filter(
                    data_file__client_name__icontains=data_file.client_name
                ).count()
                
                # If we have 10+ tests for this client, trigger model training
                if client_tests_count >= 10:
                    try:
                        ai_training_task = train_ai_completeness_model.delay(
                            model_type='COMPLETENESS_PREDICTOR',
                            client_name=data_file.client_name
                        )
                        logger.info(f"🤖 AI model training queued for client {data_file.client_name}: {ai_training_task.id}")
                    except Exception as ai_error:
                        logger.warning(f"🤖 Could not queue AI training: {ai_error}")
                        
            except Exception as e:
                logger.warning(f"Could not queue completeness analysis: {e}")
                completeness_queued = False
            
            return {
                'file_type': 'gl_accounts',
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'completed',
                'processed_records': result['processed_count'],
                'failed_records': result['failed_count'],
                'completeness_queued': completeness_queued,
                'message': f"GL Listing processed: {result['processed_count']} records. Completeness analysis {'queued' if completeness_queued else 'failed to queue'}."
            }
            
        except Exception as e:
            # Update data file with error
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.error(f"Error processing GL file {data_file.file_name}: {e}")
            raise e
    
    def _process_file_sync(self, data_file: DataFile, file_obj, file_type: str):
        """
        Process TB and Chart of Accounts files synchronously during upload
        Files are processed directly from memory - no disk saving/deletion
        
        Args:
            data_file: DataFile instance
            file_obj: Uploaded file object
            file_type: Type of file ('trial_balance' or 'chart_of_accounts')
            
        Returns:
            Dict: Processing result
        """
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Process the file synchronously directly from memory
            from .file_processing_utils import FileReader, FileTypeDetector, DataProcessor
            
            # Read the file directly from memory (no disk saving)
            file_reader = FileReader()
            df = file_reader.read_file(file_obj)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Process the data
            processor = DataProcessor(data_file)
            
            if file_type == 'trial_balance':
                result = processor.process_tb_data(df)
                message = f"Trial Balance processed: {result['processed_count']} records (no file saved)"
            elif file_type == 'chart_of_accounts':
                result = processor.process_chart_data(df)
                message = f"Chart of Accounts processed: {result['processed_count']} records (no file saved)"
            else:
                raise Exception(f"Unknown file type: {file_type}")
            
            # Update data file with results
            data_file.status = 'COMPLETED'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.info(f"Successfully processed {file_type} file {data_file.file_name} from memory: {result}")
            
            return {
                'file_type': file_type,
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'completed',
                'processed_records': result['processed_count'],
                'failed_records': result['failed_count'],
                'message': message
            }
            
        except Exception as e:
            # Update data file with error
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.error(f"Error processing {file_type} file {data_file.file_name}: {e}")
            raise e


# ============================================================================
# COMPLETENESS TEST API VIEWS
# ============================================================================

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.pagination import PageNumberPagination
from django.shortcuts import get_object_or_404
from .models import CompletenessTestResult
from .serializers import CompletenessTestResultSerializer, CompletenessTestSummarySerializer


class CompletenessTestPagination(PageNumberPagination):
    """Custom pagination for completeness test results"""
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 100


@api_view(['GET'])
@permission_classes([AllowAny])
def get_completeness_test_by_engagement(request, engagement_id):
    """
    Get completeness test results for a specific engagement
    
    GET /api/completeness-test/engagement/{engagement_id}/
    
    Query Parameters:
    - summary: boolean (default: false) - Return summary only
    - latest: boolean (default: true) - Return only the latest test result
    """
    try:
        # Get query parameters
        summary_only = request.query_params.get('summary', 'false').lower() == 'true'
        latest_only = request.query_params.get('latest', 'true').lower() == 'true'
        
        # Query completeness test results for the engagement
        queryset = CompletenessTestResult.objects.filter(engagement_id=engagement_id)
        
        if not queryset.exists():
            return Response({
                'error': f'No completeness test results found for engagement {engagement_id}',
                'engagement_id': engagement_id
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get latest result if requested
        if latest_only:
            queryset = queryset.order_by('-test_timestamp')[:1]
        
        # Choose serializer based on summary parameter
        if summary_only:
            serializer_class = CompletenessTestSummarySerializer
        else:
            serializer_class = CompletenessTestResultSerializer
        
        # Convert queryset to list to avoid QuerySet serialization issues
        queryset_list = list(queryset)
        
        # Serialize the results
        serializer = serializer_class(queryset_list, many=True)
        
        # Prepare response data
        response_data = {
            'engagement_id': engagement_id,
            'total_tests': queryset.count() if not latest_only else 1,
            'results': serializer.data
        }
        
        # Add summary statistics if not summary_only
        if not summary_only and queryset_list:
            latest_result = queryset_list[0]  # Get first item from the list
            response_data['summary'] = {
                'latest_test_timestamp': latest_result.test_timestamp,
                'overall_status': latest_result.overall_status,
                'completeness_score': latest_result.completeness_score,
                'tests_passed': latest_result.tests_passed,
                'total_tests': latest_result.total_tests,
                'critical_issues': latest_result.critical_issues_count,
                'total_gl_records': latest_result.total_gl_records,
                'total_tb_records': latest_result.total_tb_records
            }
        
        return Response(response_data, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving completeness test for engagement {engagement_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_completeness_test_by_id(request, test_id):
    """
    Get specific completeness test result by ID
    
    GET /api/completeness-test/{test_id}/
    """
    try:
        test_result = get_object_or_404(CompletenessTestResult, id=test_id)
        serializer = CompletenessTestResultSerializer(test_result)
        
        return Response({
            'test_id': test_id,
            'result': serializer.data
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving completeness test {test_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def list_completeness_tests(request):
    """
    List all completeness test results with pagination
    
    GET /api/completeness-test/
    
    Query Parameters:
    - engagement_id: Filter by engagement ID
    - status: Filter by overall status (PASS, FAIL, TB_NOT_AVAILABLE, ERROR)
    - page: Page number for pagination
    - page_size: Number of results per page
    """
    try:
        # Get query parameters
        engagement_id = request.query_params.get('engagement_id')
        status_filter = request.query_params.get('status')
        
        # Build queryset
        queryset = CompletenessTestResult.objects.all().order_by('-test_timestamp')
        
        # Apply filters
        if engagement_id:
            queryset = queryset.filter(engagement_id=engagement_id)
        
        if status_filter:
            queryset = queryset.filter(overall_status=status_filter)
        
        # Paginate results
        paginator = CompletenessTestPagination()
        page = paginator.paginate_queryset(queryset, request)
        
        if page is not None:
            serializer = CompletenessTestSummarySerializer(page, many=True)
            return paginator.get_paginated_response({
                'results': serializer.data,
                'filters': {
                    'engagement_id': engagement_id,
                    'status': status_filter
                }
            })
        
        # If no pagination, return all results
        serializer = CompletenessTestSummarySerializer(queryset, many=True)
        return Response({
            'results': serializer.data,
            'total_count': queryset.count(),
            'filters': {
                'engagement_id': engagement_id,
                'status': status_filter
            }
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error listing completeness tests: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

