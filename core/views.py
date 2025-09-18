"""
Core views for file upload functionality - Multiple File Upload Support
Supports multiple file uploads (trial_balance, chart_of_accounts, gl_accounts) with engagement-based structure
"""

# All imports at the top
from rest_framework import status, generics
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.pagination import PageNumberPagination
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.conf import settings
import logging
import hashlib
import threading
import tempfile
import os
from uuid import uuid4
from datetime import datetime
from typing import Optional

from .models import DataFile, Client, Engagement, CompletenessTestResult
from .serializers import CompletenessTestResultSerializer, CompletenessTestSummarySerializer
from .file_processing_utils import FileReader, FileTypeDetector, DataProcessor

logger = logging.getLogger(__name__)


class FileUploadView(generics.CreateAPIView):
    """
    View for uploading multiple audit files with engagement-based structure
    Always handles multiple files: Trial Balance (TB), General Ledger (GL), and Chart of Accounts (COA)
    Supports the original multiple file upload format with new engagement linking
    """
    
    parser_classes = (MultiPartParser, FormParser)
    
    def create(self, request, *args, **kwargs):
        """
        Handle multiple file uploads with engagement-based structure:
        - All files (GL Listing, TB, Chart of Accounts): Processed and linked to engagement
        - Files are processed directly from memory for better reliability
        - Immediate feedback and better error handling
        - Auto-creates Client and Engagement from legacy data
        """
        try:
            # Validate and extract data
            validation_result = self._validate_request(request)
            if not validation_result['success']:
                return Response(validation_result['error'], status=status.HTTP_400_BAD_REQUEST)
            
            files = validation_result['files']
            metadata = validation_result['metadata']
            
            # Get or create engagement from metadata
            engagement = self._get_or_create_engagement_from_metadata(metadata)
            
            # Process files in order: TB first (sync), then COA, then GL (both threaded)
            # This ensures COA is available before GL processing starts
            results = []
            
            # 1. Process TB files first (synchronously) - REQUEST COMPLETES AFTER THIS
            tb_processed = False
            for file_type, file_obj in files.items():
                if file_type == 'trial_balance':
                    try:
                        new_file_type = 'TB'
                        data_file = self._create_data_file_record(file_obj, engagement, new_file_type, metadata)
                        
                        # Check if this is a duplicate file
                        if data_file.status == 'COMPLETED' and data_file.file_hash == hashlib.sha256(file_obj.read()).hexdigest():
                            file_obj.seek(0)
                            result = {
                                'file_type': new_file_type,
                                'file_name': file_obj.name,
                                'file_id': str(data_file.id),
                                'engagement_id': engagement.engagement_id,
                                'status': 'duplicate',
                                'message': f'File with identical content already exists and has been processed for this engagement',
                                'records_processed': data_file.processed_records,
                                'total_records': data_file.total_records
                            }
                        else:
                            # Process TB synchronously
                            result = self._process_file_sync(data_file, file_obj, new_file_type)
                        
                        results.append(result)
                        tb_processed = True
                        logger.info(f"✅ TB processing completed: {result.get('message', 'Success')}")
                        
                    except Exception as e:
                        logger.error(f"Error processing TB file: {e}")
                        results.append({
                            'file_type': 'TB',
                            'file_name': file_obj.name,
                            'status': 'failed',
                            'error': str(e)
                        })
                        tb_processed = True  # Still mark as processed even if failed
            
            # Start background processing for COA and GL files (non-blocking)
            self._start_background_file_processing(files, engagement, metadata, results)
            
            # Return response immediately after TB processing
            logger.info("🚀 Returning response immediately after TB processing - COA/GL processing in background")
            return self._create_success_response(results, engagement)
                
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
        # Get files using the expected field names
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
        
        # Validate required fields
        required_fields = ['engagement_id', 'client_name', 'company_name', 'fiscal_year', 'audit_start_date', 'audit_end_date']
        missing_fields = [field for field in required_fields if not metadata.get(field)]
        
        if missing_fields:
            return {
                'success': False, 
                'error': {'error': f'Missing required fields: {", ".join(missing_fields)}'}
            }
        
        return {'success': True, 'files': files, 'metadata': metadata}
    
    def _get_or_create_engagement_from_metadata(self, metadata):
        """
        Get or create engagement from metadata with proper duplicate handling
        
        Args:
            metadata: Request metadata dictionary
            
        Returns:
            Engagement: Engagement instance
        """
        # Parse dates if they're strings
        audit_start_date = metadata['audit_start_date']
        audit_end_date = metadata['audit_end_date']
        
        if isinstance(audit_start_date, str):
            audit_start_date = datetime.strptime(audit_start_date, '%Y-%m-%d').date()
        if isinstance(audit_end_date, str):
            audit_end_date = datetime.strptime(audit_end_date, '%Y-%m-%d').date()
        
        # Get or create client with better duplicate handling
        client = self._get_or_create_client(metadata)
        
        # Get or create engagement (unique per client + fiscal year)
        engagement, created = Engagement.objects.get_or_create(
            client=client,
            fiscal_year=int(metadata['fiscal_year']),
            defaults={
                'engagement_id': metadata['engagement_id'],
                'engagement_name': f"{metadata['client_name']} FY{metadata['fiscal_year']} Audit",
                'fiscal_year_start': audit_start_date,
                'fiscal_year_end': audit_end_date,
                'audit_start_date': audit_start_date,
                'audit_end_date': audit_end_date,
                'status': 'ACTIVE',
                'required_files': ['TB', 'GL', 'COA']
            }
        )
        
        if created:
            logger.info(f"Created new engagement: {engagement.engagement_id}")
        else:
            logger.info(f"Using existing engagement: {engagement.engagement_id}")
        
        return engagement
    
    def _get_or_create_client(self, metadata):
        """
        Get or create client with proper handling of duplicate client codes
        
        Args:
            metadata: Request metadata dictionary
            
        Returns:
            Client: Client instance
        """
        # First try to find existing client by name
        try:
            client = Client.objects.get(client_name=metadata['client_name'])
            logger.info(f"Found existing client: {client.client_name}")
            return client
        except Client.DoesNotExist:
            pass
        
        # Generate unique client code with retry logic
        base_code = f"AUTO_{metadata['client_name'][:10].upper()}_{metadata['fiscal_year']}"
        client_code = base_code
        attempt = 0
        max_attempts = 10
        
        while attempt < max_attempts:
            try:
                # Try to create client with this code
                client = Client.objects.create(
                    client_code=client_code,
                    client_name=metadata['client_name'],
                    company_name=metadata['company_name'],
                    is_active=True
                )
                logger.info(f"Created new client: {client.client_name} with code: {client_code}")
                return client
                
            except Exception as e:
                if 'client_code' in str(e) and 'unique' in str(e).lower():
                    # Client code already exists, try with suffix
                    attempt += 1
                    client_code = f"{base_code}_{attempt}"
                    logger.warning(f"Client code {base_code} exists, trying {client_code}")
                    continue
                else:
                    # Different error, re-raise
                    raise e
        
        # If we still can't create after max attempts, generate UUID-based code
        import uuid
        unique_suffix = str(uuid.uuid4())[:8].upper()
        client_code = f"AUTO_{metadata['client_name'][:5].upper()}_{unique_suffix}"
        
        try:
            client = Client.objects.create(
                client_code=client_code,
                client_name=metadata['client_name'],
                company_name=metadata['company_name'],
                is_active=True
            )
            logger.info(f"Created new client with UUID-based code: {client.client_name} - {client_code}")
            return client
        except Exception as e:
            logger.error(f"Failed to create client after all attempts: {e}")
            raise e
    
    def _create_data_file_record(self, file_obj, engagement, file_type, metadata):
        """
        Create DataFile record from file and engagement
        
        Args:
            file_obj: Uploaded file object
            engagement: Engagement instance
            file_type: Type of file (TB, GL, COA)
            metadata: File metadata dictionary
            
        Returns:
            DataFile: Created DataFile instance
        """
        # Calculate file hash for duplicate detection
        file_obj.seek(0)
        file_hash = hashlib.sha256()
        for chunk in file_obj.chunks():
            file_hash.update(chunk)
        file_obj.seek(0)  # Reset file pointer
        
        # Create new DataFile record (multiple files of same type now allowed per engagement)
        data_file = DataFile.objects.create(
            file_name=file_obj.name,
            file_size=file_obj.size,
            file_hash=file_hash.hexdigest(),
            engagement=engagement,
            file_type=file_type,
            status='PENDING',
            is_validated=False,
            validation_errors=[]
        )
        
        logger.info(f"Created DataFile record: {data_file.id} for engagement {engagement.engagement_id}")
        return data_file
    
    def _create_success_response(self, results, engagement):
        """
        Create success response with file and engagement information
        
        Args:
            results: List of processing results for each file
            engagement: Engagement instance
            
        Returns:
            Response: Success response
        """
        successful_uploads = [r for r in results if r.get('status') == 'completed']
        failed_uploads = [r for r in results if r.get('status') == 'failed']
        duplicate_uploads = [r for r in results if r.get('status') == 'duplicate']
        processing_uploads = [r for r in results if r.get('status') == 'processing']
        
        # Determine overall status
        if failed_uploads:
            overall_status = 'partial_success'
        elif processing_uploads:
            overall_status = 'processing'
        else:
            overall_status = 'success'
        
        # Create appropriate message
        if processing_uploads:
            message = f'File upload initiated. {len(successful_uploads)} completed, {len(processing_uploads)} processing in background, {len(failed_uploads)} failed, {len(duplicate_uploads)} duplicates skipped.'
        else:
            message = f'File upload completed. {len(successful_uploads)} successful, {len(failed_uploads)} failed, {len(duplicate_uploads)} duplicates skipped.'
        
        response_data = {
            'message': message,
            'status': overall_status,
            'total_files': len(results),
            'successful_uploads': len(successful_uploads),
            'processing_uploads': len(processing_uploads),
            'failed_uploads': len(failed_uploads),
            'duplicate_uploads': len(duplicate_uploads),
            'engagement': {
                'engagement_id': engagement.engagement_id,
                'engagement_name': engagement.engagement_name,
                'client_name': engagement.client.client_name,
                'fiscal_year': engagement.fiscal_year,
                'status': engagement.status
            },
            'results': results
        }
        
        return Response(response_data, status=status.HTTP_201_CREATED)
    
    def _start_background_file_processing(self, files, engagement, metadata, results):
        """
        Start single background thread for sequential processing:
        - If COA file exists: Process COA first, then GL only if COA succeeds
        - If NO COA file: Process GL directly
        """
        import threading
        import tempfile
        import os
        
        # Save files with real names to avoid "read of closed file" error
        real_files = {}
        temp_dir = os.path.join(settings.MEDIA_ROOT, 'temp_uploads')
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            for file_type, file_obj in files.items():
                if file_type in ['chart_of_accounts', 'gl_accounts']:
                    # Save file with real name
                    real_file_name = file_obj.name
                    real_file_path = os.path.join(temp_dir, real_file_name)
                    
                    file_obj.seek(0)
                    with open(real_file_path, 'wb') as f:
                        for chunk in file_obj.chunks():
                            f.write(chunk)
                    
                    real_files[file_type] = real_file_path
                    logger.info(f"💾 Saved {file_type} file with real name: {real_file_name}")
            
            # Start single background thread for all processing (OUTSIDE the loop!)
            if real_files:  # Only start thread if there are files to process
                logger.info(f"🚀 Starting background processing for {len(real_files)} files")
                logger.info(f"📁 Files to process: {list(real_files.keys())}")
                
                thread = threading.Thread(
                    target=self._process_files_sequentially,
                    args=(real_files, engagement, metadata, results)
                )
                thread.daemon = True
                thread.start()
                
                logger.info("🚀 Single background thread started for sequential file processing")
            else:
                logger.info("ℹ️ No files to process in background")
                
        except Exception as e:
            # Clean up temp files on error
            for temp_path in real_files.values():
                try:
                    os.unlink(temp_path)
                except:
                    pass
            logger.error(f"❌ Error setting up background processing: {e}")
            raise e
    
    def _process_files_sequentially(self, real_files, engagement, metadata, results):
        """
        Process files in single background thread using temp file paths:
        - If COA file exists: COA first, then GL only if COA succeeds
        - If NO COA file: GL processes directly
        """
        import os
        import hashlib
        
        coa_success = False
        
        try:
            # Step 1: Process COA files if they exist
            if 'chart_of_accounts' in real_files:
                logger.info("🎯" + "="*60)
                logger.info("🎯 STEP 1: PROCESSING CHART OF ACCOUNTS 🎯")
                logger.info("🎯" + "="*60)
                
                real_file_path = real_files['chart_of_accounts']
                file_name = os.path.basename(real_file_path)  # This is now the real file name
                
                try:
                    # Create a mock file object for data file record creation
                    class MockFile:
                        def __init__(self, name, path):
                            self.name = name
                            self.path = path
                            # Get file size
                            self.size = os.path.getsize(path)
                        
                        def read(self):
                            with open(self.path, 'rb') as f:
                                return f.read()
                        
                        def seek(self, pos):
                            pass
                        
                        def chunks(self, chunk_size=8192):
                            """Generator that yields chunks of the file"""
                            with open(self.path, 'rb') as f:
                                while True:
                                    chunk = f.read(chunk_size)
                                    if not chunk:
                                        break
                                    yield chunk
                    
                    mock_file = MockFile(file_name, real_file_path)
                    data_file = self._create_data_file_record(mock_file, engagement, 'COA', metadata)
                
                    # Check if this is a duplicate file
                    file_hash = hashlib.sha256(mock_file.read()).hexdigest()
                    if data_file.status == 'COMPLETED' and data_file.file_hash == file_hash:
                        result = {
                            'file_type': 'COA',
                            'file_name': file_name,
                            'file_id': str(data_file.id),
                            'engagement_id': engagement.engagement_id,
                            'status': 'duplicate',
                            'message': f'File with identical content already exists and has been processed for this engagement',
                            'records_processed': data_file.processed_records,
                            'total_records': data_file.total_records
                        }
                        results.append(result)
                        coa_success = True
                        logger.info(f"✅ COA file already processed: {file_name}")
                    else:
                        # Process COA file using real file path
                        logger.info(f"🔄 Processing COA file: {file_name}")
                        result = self._process_coa_file_from_temp_path(data_file, real_file_path, file_name)
                        results.append(result)
                        
                        # Check if COA processing was successful
                        if result.get('status') in ['completed', 'partial']:
                            coa_success = True
                            logger.info(f"✅ COA processing successful: {file_name}")
                        else:
                            logger.error(f"❌ COA processing failed: {file_name}")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing COA file {file_name}: {e}")
                    results.append({
                        'file_type': 'COA',
                        'file_name': file_name,
                        'status': 'failed',
                        'error': str(e)
                    })
                    coa_success = False
            else:
                logger.info("ℹ️ No COA file provided - GL will process directly")
                coa_success = True  # No COA file means we can process GL directly
            
            # Step 2: Process GL files
            if 'gl_accounts' in real_files:
                if coa_success:
                    logger.info("🎯" + "="*60)
                    logger.info("🎯 STEP 2: PROCESSING GL ACCOUNTS 🎯")
                    logger.info("🎯" + "="*60)
                    
                    real_file_path = real_files['gl_accounts']
                    file_name = os.path.basename(real_file_path)  # This is now the real file name
                    
                    try:
                        # Create a mock file object for data file record creation
                        class MockFile:
                            def __init__(self, name, path):
                                self.name = name
                                self.path = path
                                # Get file size
                                self.size = os.path.getsize(path)
                            
                            def read(self):
                                with open(self.path, 'rb') as f:
                                    return f.read()
                            
                            def seek(self, pos):
                                pass
                            
                            def chunks(self, chunk_size=8192):
                                """Generator that yields chunks of the file"""
                                with open(self.path, 'rb') as f:
                                    while True:
                                        chunk = f.read(chunk_size)
                                        if not chunk:
                                            break
                                        yield chunk
                        
                        mock_file = MockFile(file_name, real_file_path)
                        data_file = self._create_data_file_record(mock_file, engagement, 'GL', metadata)
                
                        # Check if this is a duplicate file
                        file_hash = hashlib.sha256(mock_file.read()).hexdigest()
                        if data_file.status == 'COMPLETED' and data_file.file_hash == file_hash:
                            result = {
                                'file_type': 'GL',
                                'file_name': file_name,
                                'file_id': str(data_file.id),
                                'engagement_id': engagement.engagement_id,
                                'status': 'duplicate',
                                'message': f'File with identical content already exists and has been processed for this engagement',
                                'records_processed': data_file.processed_records,
                                'total_records': data_file.total_records
                            }
                            results.append(result)
                            logger.info(f"✅ GL file already processed: {file_name}")
                        else:
                            # Process GL file using real file path
                            logger.info(f"🔄 Processing GL file: {file_name}")
                            result = self._process_gl_file_from_temp_path(data_file, real_file_path, file_name)
                            results.append(result)
                    
                    except Exception as e:
                        logger.error(f"❌ Error processing GL file {file_name}: {e}")
                        results.append({
                            'file_type': 'GL',
                            'file_name': file_name,
                            'status': 'failed',
                            'error': str(e)
                        })
                else:
                    logger.error("❌" + "="*60)
                    logger.error("❌ GL PROCESSING SKIPPED - COA FAILED ❌")
                    logger.error("❌" + "="*60)
                    logger.error("COA processing failed, so GL processing is skipped to maintain data integrity")
                    
                    real_file_path = real_files['gl_accounts']
                    file_name = os.path.basename(real_file_path)
                    results.append({
                        'file_type': 'GL',
                        'file_name': file_name,
                        'status': 'skipped',
                        'message': 'GL processing skipped because COA processing failed'
                    })
            
            logger.info("🎯" + "="*60)
            logger.info("🎯 SINGLE THREAD PROCESSING COMPLETED! 🎯")
            logger.info("🎯" + "="*60)
            
        finally:
            # Clean up real files
            logger.info(f"🧹 Starting cleanup of {len(real_files)} files")
            for file_type, real_path in real_files.items():
                try:
                    if os.path.exists(real_path):
                        os.unlink(real_path)
                        logger.info(f"🗑️ Cleaned up {file_type} file: {real_path}")
                    else:
                        logger.warning(f"⚠️ File not found for cleanup: {real_path}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not clean up {file_type} file {real_path}: {e}")
    
    def _process_coa_file_from_temp_path(self, data_file, temp_file_path, file_name):
        """Process COA file from temporary file path"""
        try:
            # Check if file is already being processed or completed
            if data_file.status in ['PROCESSING', 'COMPLETED']:
                logger.warning(f"⚠️ COA file {file_name} already processed or processing (status: {data_file.status})")
                return {
                    'file_type': 'COA',
                    'file_name': file_name,
                    'file_id': str(data_file.id),
                    'engagement_id': data_file.engagement.engagement_id,
                    'status': 'skipped',
                    'message': f'File already processed (status: {data_file.status})'
                }
            
            logger.info(f"🔄 Starting COA processing for file: {file_name}")
            logger.info(f"📁 File path: {temp_file_path}")
            logger.info(f"📊 DataFile ID: {data_file.id}")
            logger.info(f"🎯 Engagement ID: {data_file.engagement.engagement_id}")
            
            # Mark as processing to prevent duplicate processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Since we're already in a background thread, process synchronously
            from .file_processing_utils import DataProcessor
            processor = DataProcessor(data_file)
            result = processor.process_chart_data_from_file(temp_file_path)
            
            logger.info(f"✅ COA processing completed for file: {file_name}")
            logger.info(f"📊 Processed: {result['processed_count']}, Failed: {result['failed_count']}")
            
            # Update data file with results
            data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            return {
                'file_type': 'COA',
                'file_name': file_name,
                'file_id': str(data_file.id),
                'engagement_id': data_file.engagement.engagement_id,
                'status': 'completed' if result['failed_count'] == 0 else 'partial',
                'processed_count': result['processed_count'],
                'failed_count': result['failed_count'],
                'message': f"COA processing completed: {result['processed_count']} records processed, {result['failed_count']} failed"
            }
        except Exception as e:
            logger.error(f"❌ Error processing COA file from temp path: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {
                'file_type': 'COA',
                'file_name': file_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def _process_gl_file_from_temp_path(self, data_file, temp_file_path, file_name):
        """Process GL file from temporary file path"""
        try:
            logger.info(f"🔄 Starting GL processing for file: {file_name}")
            logger.info(f"📁 File path: {temp_file_path}")
            logger.info(f"📊 DataFile ID: {data_file.id}")
            logger.info(f"🎯 Engagement ID: {data_file.engagement.engagement_id}")
            logger.info(f"🔧 Processing method: _process_gl_file_from_temp_path (Sequential)")
            
            # Since we're already in a background thread, process synchronously
            from .file_processing_utils import DataProcessor
            processor = DataProcessor(data_file)
            result = processor.process_gl_data_chunked(temp_file_path, chunk_size=25000)
            
            # Update data file with results
            data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            # Trigger completeness test in Celery after GL processing completion
            try:
                from .tasks import run_gl_completeness_analysis
                from django.db import connection
                
                # Ensure database connection is closed before task execution
                connection.close()
                
                completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                logger.info(f"📊 GL processing completed, starting completeness analysis for file: {file_name}")
                logger.info(f"🔗 Task details: {completeness_task.id} - {completeness_task.state}")
            except Exception as e:
                logger.error(f"❌ Could not queue completeness analysis: {e}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
            
            return {
                'file_type': 'GL',
                'file_name': file_name,
                'file_id': str(data_file.id),
                'engagement_id': data_file.engagement.engagement_id,
                'status': 'completed' if result['failed_count'] == 0 else 'partial',
                'processed_count': result['processed_count'],
                'failed_count': result['failed_count'],
                'message': f"GL processing completed: {result['processed_count']} records processed, {result['failed_count']} failed"
            }
        except Exception as e:
            logger.error(f"❌ Error processing GL file from temp path: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {
                'file_type': 'GL',
                'file_name': file_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def _process_gl_file_with_threading(self, data_file: DataFile, file_obj):
        """
        Process GL file with intelligent threading strategy:
        - Small files (< 10k rows): Process synchronously
        - Large files (>= 10k rows): Process in background thread
        """
        try:
            # Check file size first
            file_obj.seek(0)
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
        """
        Process large GL files in background thread with temp file management
        🗂️ TEMP STORAGE: Only GL files are saved to temp due to large size and async processing
        🗑️ AUTO CLEANUP: Temp files are automatically deleted after processing completion
        """
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
                'file_type': 'GL',
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'processing',
                'total_records': row_count,
                'processed_records': 0,
                'failed_records': 0,
                'message': f"GL Listing processing started for {row_count} records. Check status via API.",
                'engagement_id': data_file.engagement.engagement_id if data_file.engagement else None,
                'client_name': data_file.engagement.client.client_name if data_file.engagement and data_file.engagement.client else None
            }
            
        except Exception as e:
            if 'temp_file' in locals() and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise e
    
    def _process_gl_background_thread(self, data_file: DataFile, file_path: str, row_count: int):
        """Background thread for processing large GL files"""
        try:
            logger.info(f"Background thread started for GL file {data_file.file_name}")
            logger.info(f"🔧 Processing method: _process_gl_background_thread (OLD METHOD)")
            
            # Process using optimized chunked processing
            processor = DataProcessor(data_file)
            result = processor.process_gl_data_chunked(file_path, chunk_size=25000)
            
            # Update data file with results
            data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.info(f"Background processing completed for GL file {data_file.file_name}: {result}")
            
            # Trigger completeness test in Celery after GL processing completion
            try:
                from .tasks import run_gl_completeness_analysis
                completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                logger.info(f"📊 GL processing completed, starting completeness analysis for file: {data_file.file_name}")
                        
            except Exception as e:
                logger.warning(f"Could not queue completeness analysis: {e}")
            
        except Exception as e:
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            logger.error(f"Background processing failed for GL file {data_file.file_name}: {e}")
            
        finally:
            # 🗑️ Clean up temporary file after GL processing completion
            try:
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    os.unlink(file_path)
                    logger.info(f"🗑️ Cleaned up GL temp file: {file_path} ({file_size:,} bytes)")
                else:
                    logger.info(f"🗑️ GL temp file already cleaned up: {file_path}")
            except Exception as cleanup_error:
                logger.error(f"❌ Error cleaning up GL temp file {file_path}: {cleanup_error}")
            
            # 🧹 Clean up all engagement temp files after GL processing completion
            try:
                self._cleanup_engagement_temp_files(data_file.engagement)
                logger.info(f"🧹 Engagement temp files cleanup completed for: {data_file.engagement.engagement_id}")
            except Exception as cleanup_error:
                logger.error(f"❌ Error cleaning up engagement temp files: {cleanup_error}")
    
    def _process_gl_coa_file_with_threading(self, data_file: DataFile, file_obj_or_path, file_type: str):
        """
        Process GL or COA files with intelligent threading strategy:
        - Small files (< 10k rows): Process synchronously
        - Large files (>= 10k rows): Process in background thread
        - COA files are processed first, then GL files
        - Can handle both file objects and file paths
        """
        try:
            # Handle both file objects and file paths
            if isinstance(file_obj_or_path, str):
                # It's a file path (from temp file)
                file_path = file_obj_or_path
                file_reader = FileReader()
                df = file_reader.read_file(file_path)
            else:
                # It's a file object
                file_obj = file_obj_or_path
                file_obj.seek(0)
                file_reader = FileReader()
                df = file_reader.read_file(file_obj)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            row_count = len(df)
            logger.info(f"{file_type} file {data_file.file_name} has {row_count} rows")
            
            if row_count < 10000:
                # Small file: process synchronously
                return self._process_file_sync(data_file, file_obj_or_path, file_type)
            else:
                # Large file: process in background thread
                return self._process_gl_coa_large_file_threaded(data_file, file_obj_or_path, row_count, file_type)
                
        except Exception as e:
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            raise e
    
    def _process_gl_coa_large_file_threaded(self, data_file: DataFile, file_obj_or_path, row_count: int, file_type: str):
        """
        Process large GL or COA files in background thread with temp file management
        🗂️ TEMP STORAGE: Large files are saved to temp due to size and async processing
        🗑️ AUTO CLEANUP: Temp files are automatically deleted after processing completion
        - Can handle both file objects and file paths
        """
        try:
            # Handle both file objects and file paths
            if isinstance(file_obj_or_path, str):
                # It's already a file path (from temp file)
                temp_file_path = file_obj_or_path
                logger.info(f"Using existing temp file path: {temp_file_path}")
            else:
                # It's a file object - save to temporary location
                file_obj = file_obj_or_path
                file_extension = file_obj.name.split('.')[-1]
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_extension}')
                
                file_obj.seek(0)
                for chunk in file_obj.chunks():
                    temp_file.write(chunk)
                temp_file.close()
                temp_file_path = temp_file.name
                logger.info(f"Created new temp file: {temp_file_path}")
            
            # Update data file status
            data_file.status = 'PROCESSING'
            data_file.total_records = row_count
            data_file.save()
            
            # Start background thread for processing
            thread = threading.Thread(
                target=self._process_gl_coa_background_thread,
                args=(data_file, temp_file_path, row_count, file_type)
            )
            thread.daemon = True
            thread.start()
            
            logger.info(f"Background thread started for {file_type} file {data_file.file_name}")
            
            return {
                'file_type': file_type,
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'processing',
                'total_records': row_count,
                'message': f"Started background processing for large {file_type} file {data_file.file_name} ({row_count} rows)"
            }
            
        except Exception as e:
            # Clean up temp file on error (only if we created it)
            try:
                if not isinstance(file_obj_or_path, str) and 'temp_file' in locals():
                    os.unlink(temp_file.name)
            except:
                pass
            raise e
    
    def _process_gl_coa_background_thread(self, data_file: DataFile, file_path: str, row_count: int, file_type: str):
        """Background thread for processing large GL or COA files"""
        try:
            logger.info(f"Background thread started for {file_type} file {data_file.file_name}")
            
            if file_type == 'COA':
                # Process COA file
                logger.info(f"🎯 Starting COA background processing for file {data_file.file_name}")
                processor = DataProcessor(data_file)
                result = processor.process_chart_data_from_file(file_path)
                
                # Update data file with results
                data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
                data_file.total_records = result['processed_count'] + result['failed_count']
                data_file.processed_records = result['processed_count']
                data_file.failed_records = result['failed_count']
                data_file.processed_at = timezone.now()
                data_file.save()
                
                logger.info("🎯" + "="*60)
                logger.info(f"🎯 COA BACKGROUND PROCESSING COMPLETED! 🎯")
                logger.info(f"📊 File: {data_file.file_name}")
                logger.info(f"📊 Records processed: {result['processed_count']}")
                logger.info(f"📊 Records failed: {result['failed_count']}")
                logger.info(f"📊 Status: {data_file.status}")
                logger.info("🎯" + "="*60)
                
            elif file_type == 'GL':
                # Process GL file using optimized chunked processing
                processor = DataProcessor(data_file)
                result = processor.process_gl_data_chunked(file_path, chunk_size=25000)
                
                # Update data file with results
                data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
                data_file.total_records = result['processed_count'] + result['failed_count']
                data_file.processed_records = result['processed_count']
                data_file.failed_records = result['failed_count']
                data_file.processed_at = timezone.now()
                data_file.save()
                
                logger.info(f"Background GL processing completed for file {data_file.file_name}: {result}")
                
                # Trigger completeness test in Celery after GL processing completion
                try:
                    from .tasks import run_gl_completeness_analysis
                    completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                    logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                    logger.info(f"📊 GL processing completed, starting completeness analysis for file: {data_file.file_name}")
                            
                except Exception as e:
                    logger.warning(f"Could not queue completeness analysis: {e}")
            
        except Exception as e:
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            logger.error(f"Background processing failed for {file_type} file {data_file.file_name}: {e}")
            
        finally:
            # Clean up temp file
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temp file {file_path}: {cleanup_error}")
    
    def _process_gl_file_sync(self, data_file: DataFile, file_obj):
        """Process GL Listing file synchronously for better reliability"""
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Read the file directly from memory
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
            
            # Trigger completeness test in Celery after synchronous GL processing completion
            try:
                from .tasks import run_gl_completeness_analysis
                completeness_task = run_gl_completeness_analysis.delay(str(data_file.id))
                logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                logger.info(f"📊 Sync GL processing completed, starting completeness analysis for file: {data_file.file_name}")
                completeness_queued = True
                        
            except Exception as e:
                logger.warning(f"Could not queue completeness analysis: {e}")
                completeness_queued = False
            
            # 🧹 Clean up engagement temp files after successful synchronous GL processing
            try:
                self._cleanup_engagement_temp_files(data_file.engagement)
                logger.info(f"🧹 Engagement temp files cleanup completed for sync GL: {data_file.engagement.engagement_id}")
            except Exception as cleanup_error:
                logger.error(f"❌ Error cleaning up engagement temp files: {cleanup_error}")
            
            return {
                'file_type': 'GL',
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'completed',
                'total_records': data_file.total_records,
                'processed_records': result['processed_count'],
                'failed_records': result['failed_count'],
                'completeness_queued': completeness_queued,
                'message': f"GL Listing processed: {result['processed_count']} records. Completeness analysis {'queued' if completeness_queued else 'failed to queue'}.",
                'engagement_id': data_file.engagement.engagement_id if data_file.engagement else None,
                'client_name': data_file.engagement.client.client_name if data_file.engagement and data_file.engagement.client else None
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
        Process TB and COA files synchronously during upload
        ⚡ OPTIMIZED: Files are processed directly from memory - NO temp file saving/deletion needed!
        Only GL files use temp storage due to their large size and background processing requirements.
        """
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Read the file directly from memory
            file_reader = FileReader()
            df = file_reader.read_file(file_obj)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Process the data
            processor = DataProcessor(data_file)
            
            if file_type == 'TB':
                result = processor.process_tb_data(df)
                message = f"Trial Balance processed: {result['processed_count']} records"
            elif file_type == 'COA':
                result = processor.process_chart_data(df)
                message = f"Chart of Accounts processed: {result['processed_count']} records"
            elif file_type == 'OTHER':
                # For OTHER files, we just validate structure but don't process
                data_file.status = 'COMPLETED'
                data_file.total_records = len(df)
                data_file.processed_records = len(df)
                data_file.failed_records = 0
                data_file.processed_at = timezone.now()
                data_file.is_validated = True
                data_file.save()
                
                return {
                    'file_type': file_type,
                    'file_id': str(data_file.id),
                    'file_name': data_file.file_name,
                    'file_size': data_file.file_size,
                    'status': 'completed',
                    'total_records': len(df),
                    'processed_records': len(df),
                    'failed_records': 0,
                    'message': f"Other file uploaded and validated: {len(df)} records"
                }
            else:
                raise Exception(f"Unknown file type: {file_type}")
            
            # Update data file with results
            data_file.status = 'COMPLETED'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.is_validated = True
            data_file.save()
            
            logger.info(f"Successfully processed {file_type} file {data_file.file_name}: {result}")
            
            return {
                'file_type': file_type,
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'completed',
                'total_records': data_file.total_records,
                'processed_records': result['processed_count'],
                'failed_records': result['failed_count'],
                'message': message,
                'engagement_id': data_file.engagement.engagement_id if data_file.engagement else None,
                'client_name': data_file.engagement.client.client_name if data_file.engagement and data_file.engagement.client else None
            }
            
        except Exception as e:
            # Update data file with error
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.error(f"Error processing {file_type} file {data_file.file_name}: {e}")
            raise e

    def _cleanup_engagement_temp_files(self, engagement):
        """
        Clean up all temporary files for a specific engagement after GL processing completion
        
        This function removes:
        1. Files in temp_uploads/ directory that belong to this engagement
        2. Any orphaned temporary files older than 1 hour
        
        Args:
            engagement: Engagement object to clean up temp files for
        """
        if not engagement:
            logger.warning("No engagement provided for temp file cleanup")
            return
            
        logger.info(f"🧹 Starting temp file cleanup for engagement: {engagement.engagement_id}")
        
        try:
            from django.conf import settings
            import glob
            from datetime import datetime, timedelta
            
            temp_dir = getattr(settings, 'FILE_UPLOAD_TEMP_DIR', os.path.join(settings.BASE_DIR, 'temp_uploads'))
            
            if not os.path.exists(temp_dir):
                logger.info(f"Temp directory {temp_dir} does not exist, nothing to clean")
                return
            
            cleaned_count = 0
            total_size = 0
            
            # Get all data files for this engagement
            engagement_files = engagement.data_files.all()
            engagement_file_names = [df.file_name for df in engagement_files]
            
            logger.info(f"📋 Found {len(engagement_file_names)} files for engagement {engagement.engagement_id}")
            
            # Clean up files in temp_uploads directory
            temp_files = glob.glob(os.path.join(temp_dir, "*"))
            
            for temp_file_path in temp_files:
                try:
                    if not os.path.isfile(temp_file_path):
                        continue
                        
                    file_name = os.path.basename(temp_file_path)
                    file_size = os.path.getsize(temp_file_path)
                    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(temp_file_path))
                    
                    should_delete = False
                    reason = ""
                    
                    # Check if this file belongs to the current engagement
                    # Look for engagement-related patterns or older files
                    if any(eng_name in file_name for eng_name in engagement_file_names):
                        should_delete = True
                        reason = f"belongs to engagement {engagement.engagement_id}"
                    elif file_age > timedelta(hours=1):
                        should_delete = True
                        reason = f"older than 1 hour (age: {file_age})"
                    elif file_name.startswith('tmp') and file_name.endswith(('.xlsx', '.csv', '.xls')):
                        # Clean up temporary upload files that are old
                        if file_age > timedelta(minutes=30):
                            should_delete = True
                            reason = f"temporary upload file older than 30 minutes"
                    
                    if should_delete:
                        os.unlink(temp_file_path)
                        cleaned_count += 1
                        total_size += file_size
                        logger.info(f"🗑️  Deleted temp file: {file_name} ({file_size} bytes) - {reason}")
                        
                except Exception as file_error:
                    logger.error(f"❌ Error deleting temp file {temp_file_path}: {file_error}")
                    continue
            
            # Summary log
            if cleaned_count > 0:
                logger.info(f"✅ Temp cleanup completed for engagement {engagement.engagement_id}")
                logger.info(f"📊 Cleaned {cleaned_count} files, freed {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
            else:
                logger.info(f"🔍 No temp files to clean for engagement {engagement.engagement_id}")
                
        except Exception as e:
            logger.error(f"❌ Error during temp file cleanup for engagement {engagement.engagement_id}: {e}")


# ============================================================================
# COMPLETENESS TEST API VIEWS
# ============================================================================

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
        queryset = CompletenessTestResult.objects.filter(engagement__engagement_id=engagement_id)
        
        if not queryset.exists():
            return Response({
                'engagement_id': engagement_id,
                'total_tests': 0,
                'results': [],
                'message': f'No completeness test results found for engagement {engagement_id}'
            }, status=status.HTTP_200_OK)
        
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
        
        # Remove account_verifications from the serialized data to reduce response size
        serialized_data = serializer.data
        for result in serialized_data:
            if 'step2_gl_tb_reconciliation' in result and isinstance(result['step2_gl_tb_reconciliation'], dict):
                if 'account_verifications' in result['step2_gl_tb_reconciliation']:
                    # Remove the detailed account_verifications list but keep summary info
                    step2_data = result['step2_gl_tb_reconciliation'].copy()
                    account_verifications_count = len(step2_data.get('account_verifications', []))
                    del step2_data['account_verifications']
                    step2_data['account_verifications_count'] = account_verifications_count
                    step2_data['account_verifications_note'] = f"Use /api/account-verifications/engagement/{engagement_id}/ to get detailed account verification data"
                    result['step2_gl_tb_reconciliation'] = step2_data
        
        # Prepare response data
        response_data = {
            'engagement_id': engagement_id,
            'total_tests': queryset.count() if not latest_only else 1,
            'results': serialized_data
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


@api_view(['POST'])
@permission_classes([AllowAny])
def trigger_completeness_analysis(request, gl_file_id):
    """
    Trigger 2-step GL completeness analysis using Celery task
    
    POST /api/completeness-test/trigger/{gl_file_id}/
    
    Query Parameters:
    - clear_previous: boolean (default: false) - Clear previous test results for this engagement
    
    Response:
    {
        "task_id": "uuid",
        "status": "submitted",
        "message": "2-step completeness analysis task submitted",
        "gl_file_id": "uuid",
        "engagement_id": "ENG-XXX",
        "monitor_url": "/api/completeness-test/task-status/{task_id}/",
        "estimated_duration": "2-5 minutes"
    }
    """
    try:
        from .tasks import run_gl_completeness_analysis
        from .models import DataFile, CompletenessTestResult
        from uuid import UUID
        
        # Validate GL file ID format
        try:
            gl_file_uuid = UUID(gl_file_id)
        except ValueError:
            return Response({
                'error': 'Invalid GL file ID format',
                'details': f'Expected UUID format, got: {gl_file_id}'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Verify GL file exists
        try:
            gl_file = DataFile.objects.get(id=gl_file_uuid, file_type='GL')
        except DataFile.DoesNotExist:
            return Response({
                'error': 'GL file not found',
                'details': f'No GL file found with ID: {gl_file_id}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get query parameters
        clear_previous = request.query_params.get('clear_previous', 'false').lower() == 'true'
        
        # Clear previous results if requested
        if clear_previous:
            previous_results = CompletenessTestResult.objects.filter(engagement=gl_file.engagement)
            if previous_results.exists():
                count = previous_results.count()
                previous_results.delete()
                logger.info(f"Cleared {count} previous completeness test results for engagement {gl_file.engagement.engagement_id}")
        
        # Submit Celery task
        task_result = run_gl_completeness_analysis.delay(gl_file_id)
        
        # Prepare response
        response_data = {
            'task_id': task_result.id,
            'status': 'submitted',
            'message': '2-step completeness analysis task submitted successfully',
            'gl_file_id': gl_file_id,
            'gl_file_name': gl_file.file_name,
            'engagement_id': gl_file.engagement.engagement_id,
            'engagement_name': gl_file.engagement.engagement_name,
            'client_name': gl_file.engagement.client_name,
            'monitor_url': f'/api/completeness-test/task-status/{task_result.id}/',
            'results_url': f'/api/completeness-test/engagement/{gl_file.engagement.engagement_id}/',
            'estimated_duration': '2-5 minutes for large datasets',
            'task_details': {
                'celery_task_name': 'run_gl_completeness_analysis',
                'task_state': task_result.state,
                'flower_url': f'http://localhost:5555/task/{task_result.id}' if 'localhost' in request.get_host() else None
            }
        }
        
        logger.info(f"Submitted 2-step completeness analysis task {task_result.id} for GL file {gl_file_id} (engagement: {gl_file.engagement.engagement_id})")
        
        return Response(response_data, status=status.HTTP_202_ACCEPTED)
        
    except Exception as e:
        logger.error(f"Error triggering completeness analysis for GL file {gl_file_id}: {e}")
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


# ============================================================================
# ACCOUNT VERIFICATIONS API VIEW
# ============================================================================

class AccountVerificationsPagination(PageNumberPagination):
    """Custom pagination for account verifications"""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 500


@api_view(['GET'])
@permission_classes([AllowAny])
def get_account_verifications_by_engagement(request, engagement_id):
    """
    Get account verifications for a specific engagement with pagination
    
    GET /api/account-verifications/engagement/{engagement_id}/
    
    Query Parameters:
    - page: Page number for pagination
    - page_size: Number of results per page (max 500)
    - failed_only: boolean (default: false) - true=failed accounts, false=passed accounts
    - latest: boolean (default: true) - Return only from latest test result
    - account_code: Filter by specific account code
    - min_variance: Filter by minimum balance variance
    - sort_by: Sort field (account_code, balance_variance, gl_debit_total, gl_credit_total)
    - sort_order: Sort order (asc, desc) - default: desc for variance, asc for account_code
    """
    try:
        # Get query parameters
        failed_only = request.query_params.get('failed_only', 'false').lower() == 'true'
        latest_only = request.query_params.get('latest', 'true').lower() == 'true'
        account_code_filter = request.query_params.get('account_code')
        min_variance = request.query_params.get('min_variance')
        sort_by = request.query_params.get('sort_by', 'balance_variance')
        sort_order = request.query_params.get('sort_order', 'desc' if sort_by == 'balance_variance' else 'asc')
        
        # Query completeness test results for the engagement
        queryset = CompletenessTestResult.objects.filter(engagement__engagement_id=engagement_id)
        
        if not queryset.exists():
            return Response({
                'engagement_id': engagement_id,
                'total_verifications': 0,
                'results': [],
                'message': f'No completeness test results found for engagement {engagement_id}'
            }, status=status.HTTP_200_OK)
        
        # Get latest result if requested
        if latest_only:
            test_result = queryset.order_by('-test_timestamp').first()
        else:
            test_result = queryset.order_by('-test_timestamp').first()  # For now, always use latest
        
        # Extract account verifications from comprehensive_statistics
        account_verifications = []
        if (test_result.step2_gl_tb_reconciliation and 
            isinstance(test_result.step2_gl_tb_reconciliation, dict) and
            'account_verifications' in test_result.step2_gl_tb_reconciliation):
            
            account_verifications = test_result.step2_gl_tb_reconciliation['account_verifications']
        
        if not account_verifications:
            return Response({
                'engagement_id': engagement_id,
                'test_id': str(test_result.id),
                'test_timestamp': test_result.test_timestamp,
                'total_verifications': 0,
                'results': [],
                'message': 'No account verifications found in the latest test result'
            }, status=status.HTTP_200_OK)
        
        # Apply filters
        filtered_verifications = account_verifications
        
        # Filter by status: failed_only=true shows failed, failed_only=false shows passed
        if failed_only:
            filtered_verifications = [v for v in filtered_verifications if not v.get('account_passed', True)]
        else:
            filtered_verifications = [v for v in filtered_verifications if v.get('account_passed', True)]
        
        # Filter by account code
        if account_code_filter:
            filtered_verifications = [v for v in filtered_verifications 
                                    if account_code_filter.upper() in str(v.get('account_code', '')).upper()]
        
        # Filter by minimum variance
        if min_variance:
            try:
                min_var = float(min_variance)
                filtered_verifications = [v for v in filtered_verifications 
                                        if abs(float(v.get('balance_variance', 0))) >= min_var]
            except ValueError:
                pass  # Ignore invalid min_variance
        
        # Sort results
        sort_reverse = sort_order.lower() == 'desc'
        def get_audit_variance(x):
            """Calculate audit variance for sorting: closing_balance - (gl_debit - gl_credit + opening_balance)"""
            gl_debit = float(x.get('gl_debit_total', 0) or 0)
            gl_credit = float(x.get('gl_credit_total', 0) or 0)
            opening_balance = float(x.get('opening_balance', 0) or 0)
            closing_balance = float(x.get('closing_balance', 0) or 0)
            calculated_closing = gl_debit - gl_credit + opening_balance
            audit_variance = closing_balance - calculated_closing
            return abs(audit_variance)
        
        sort_key_mapping = {
            'account_code': lambda x: str(x.get('account_code', '')),
            'balance_variance': get_audit_variance,
            'gl_debit_total': lambda x: float(x.get('gl_debit_total', 0)),
            'gl_credit_total': lambda x: float(x.get('gl_credit_total', 0)),
            'tb_debit': lambda x: float(x.get('tb_debit', 0)),
            'tb_credit': lambda x: float(x.get('tb_credit', 0)),
            'opening_balance': lambda x: float(x.get('opening_balance', 0)),
            'closing_balance': lambda x: float(x.get('closing_balance', 0))
        }
        
        if sort_by in sort_key_mapping:
            try:
                filtered_verifications.sort(key=sort_key_mapping[sort_by], reverse=sort_reverse)
            except (ValueError, TypeError):
                # If sorting fails, keep original order
                pass
        
        # Add computed fields for better display
        for verification in filtered_verifications:
            # Handle balance_variance - ensure we get the actual value
            balance_variance = verification.get('balance_variance', 0)
            try:
                balance_variance_float = float(balance_variance) if balance_variance is not None else 0.0
            except (ValueError, TypeError):
                balance_variance_float = 0.0
            
            # Calculate variance using proper audit formula: 
            # closing_balance - (gl_debit - gl_credit + opening_balance)
            gl_debit = float(verification.get('gl_debit_total', 0) or 0)
            gl_credit = float(verification.get('gl_credit_total', 0) or 0)
            opening_balance = float(verification.get('opening_balance', 0) or 0)
            closing_balance = float(verification.get('closing_balance', 0) or 0)
            
            # Proper variance formula
            calculated_closing = gl_debit - gl_credit + opening_balance
            audit_variance = closing_balance - calculated_closing
            
            # Also calculate movement variances for additional context
            gl_debit_var = abs(float(verification.get('gl_vs_tb_debit_variance', 0) or 0))
            gl_credit_var = abs(float(verification.get('gl_vs_tb_credit_variance', 0) or 0))
            total_movement_variance = gl_debit_var + gl_credit_var
            
            # Use the audit variance as the primary variance
            display_variance = abs(audit_variance)
            
            verification['balance_variance_abs'] = abs(balance_variance_float)
            verification['audit_variance'] = audit_variance
            verification['audit_variance_abs'] = abs(audit_variance)
            verification['calculated_closing_balance'] = calculated_closing
            verification['movement_variance_total'] = total_movement_variance
            verification['variance_formatted'] = f"{display_variance:,.2f}"
            verification['variance_type'] = 'audit_variance'
            verification['variance_note'] = 'Closing - (GL_Debit - GL_Credit + Opening)'
            
            # Format other fields with error handling
            verification['gl_debit_formatted'] = f"{float(verification.get('gl_debit_total', 0) or 0):,.2f}"
            verification['gl_credit_formatted'] = f"{float(verification.get('gl_credit_total', 0) or 0):,.2f}"
            verification['tb_debit_formatted'] = f"{float(verification.get('tb_debit', 0) or 0):,.2f}"
            verification['tb_credit_formatted'] = f"{float(verification.get('tb_credit', 0) or 0):,.2f}"
            verification['opening_balance_formatted'] = f"{float(verification.get('opening_balance', 0) or 0):,.2f}"
            verification['closing_balance_formatted'] = f"{float(verification.get('closing_balance', 0) or 0):,.2f}"
            verification['calculated_closing_formatted'] = f"{verification['calculated_closing_balance']:,.2f}"
            verification['audit_variance_formatted'] = f"{verification['audit_variance']:,.2f}"
            
            # Status and additional verification details
            verification['status'] = 'PASS' if verification.get('account_passed', False) else 'FAIL'
            verification['status_icon'] = '✅' if verification.get('account_passed', False) else '❌'
            verification['balance_equation_correct'] = verification.get('balance_equation_correct', False)
            verification['gl_tb_movements_match'] = verification.get('gl_tb_movements_match', False)
            
            # Add failure reasons for failed accounts
            if not verification.get('account_passed', False):
                failure_reasons = []
                if not verification.get('balance_equation_correct', False):
                    failure_reasons.append('Balance equation incorrect')
                if not verification.get('gl_tb_movements_match', False):
                    failure_reasons.append('GL vs TB movements mismatch')
                verification['failure_reasons'] = failure_reasons
        
        # Apply pagination
        paginator = AccountVerificationsPagination()
        
        # Convert to a format that can be paginated
        class ListWrapper:
            def __init__(self, items):
                self.items = items
            
            def __getitem__(self, key):
                return self.items[key]
            
            def __len__(self):
                return len(self.items)
        
        wrapped_data = ListWrapper(filtered_verifications)
        page = paginator.paginate_queryset(wrapped_data, request)
        
        if page is not None:
            response_data = {
                'engagement_id': engagement_id,
                'test_id': str(test_result.id),
                'test_timestamp': test_result.test_timestamp,
                'total_verifications': len(account_verifications),
                'filtered_verifications': len(filtered_verifications),
                'filters_applied': {
                    'failed_only': failed_only,
                    'account_code': account_code_filter,
                    'min_variance': min_variance,
                    'sort_by': sort_by,
                    'sort_order': sort_order
                },
                'results': page
            }
            return paginator.get_paginated_response(response_data)
        
        # If no pagination, return all results
        return Response({
            'engagement_id': engagement_id,
            'test_id': str(test_result.id),
            'test_timestamp': test_result.test_timestamp,
            'total_verifications': len(account_verifications),
            'filtered_verifications': len(filtered_verifications),
            'filters_applied': {
                'failed_only': failed_only,
                'account_code': account_code_filter,
                'min_variance': min_variance,
                'sort_by': sort_by,
                'sort_order': sort_order
            },
            'results': filtered_verifications
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving account verifications for engagement {engagement_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



