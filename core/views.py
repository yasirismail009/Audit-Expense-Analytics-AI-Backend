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
import pandas as pd
from uuid import uuid4
from datetime import datetime
from typing import Optional, Dict

from .models import DataFile, Client, Engagement, CompletenessTestResult, SAPGLPosting, ProfitCenter
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
            'description': request.data.get('description', ''),
            # Version control fields
            'version': request.data.get('version', '1.0'),
            'version_notes': request.data.get('version_notes', '')
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
        
        # Get or create engagement (one engagement, multiple test versions)
        version = metadata.get('version', '1.0')
        version_notes = metadata.get('version_notes', '')
        
        # Get or create the main engagement (unique per client + fiscal year)
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
        
        # Extract version information from metadata
        version = metadata.get('version', '1.0')
        version_notes = metadata.get('version_notes', '')
        
        # Create new DataFile record with version (multiple versions allowed per engagement)
        data_file = DataFile.objects.create(
            file_name=file_obj.name,
            file_size=file_obj.size,
            file_hash=file_hash.hexdigest(),
            engagement=engagement,
            file_type=file_type,
            version=version,  # Each file version is tracked separately
            version_notes=version_notes,
            status='PENDING',
            is_validated=False,
            validation_errors=[]
        )
        
        logger.info(f"Created DataFile record: {data_file.id} for engagement {engagement.engagement_id} (version: {version})")
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
        
        # Create data file records and save files locally first
        data_files = {}
        try:
            for file_type, file_obj in files.items():
                if file_type in ['chart_of_accounts', 'gl_accounts']:
                    # Create data file record first
                    new_file_type = 'COA' if file_type == 'chart_of_accounts' else 'GL'
                    data_file = self._create_data_file_record(file_obj, engagement, new_file_type, metadata)
                    
                    # Save file locally in organized folder structure
                    self._save_file_locally(data_file, file_obj, new_file_type)
                    
                    # Store the data file for background processing
                    data_files[file_type] = data_file
                    
                    logger.info(f"💾 Saved {file_type} file locally: {data_file.local_file_path}")
            
            # Start single background thread for all processing
            if data_files:  # Only start thread if there are files to process
                logger.info(f"🚀 Starting background processing for {len(data_files)} files")
                
                thread = threading.Thread(
                    target=self._process_data_files_from_local,
                    args=(data_files, engagement, metadata, results)
                )
                thread.daemon = True
                thread.start()
                
                logger.info("🚀 Single background thread started for sequential file processing")
            else:
                logger.info("ℹ️ No files to process in background")
                
        except Exception as e:
            logger.error(f"❌ Error setting up background processing: {e}")
            raise e
    
    def _process_data_files_from_local(self, data_files, engagement, metadata, results):
        """
        Process data files from local paths in background thread:
        - If COA file exists: COA first, then GL only if COA succeeds
        - If NO COA file: GL processes directly
        """
        coa_success = False
        
        try:
            # Step 1: Process COA files if they exist
            if 'chart_of_accounts' in data_files:
                logger.info("🎯" + "="*60)
                logger.info("🎯 STEP 1: PROCESSING CHART OF ACCOUNTS 🎯")
                logger.info("🎯" + "="*60)
                
                data_file = data_files['chart_of_accounts']
                
                try:
                    # Process from local file
                    result = self._process_file_from_local_path(data_file, 'COA')
                    coa_success = result.get('status') == 'completed'
                    
                    logger.info(f"✅ COA processing completed: {result.get('message', 'Success')}")
                    
                except Exception as e:
                    logger.error(f"❌ COA processing failed: {e}")
                    coa_success = False
            
            # Step 2: Process GL files (only if COA succeeded or no COA file)
            if 'gl_accounts' in data_files and (coa_success or 'chart_of_accounts' not in data_files):
                logger.info("🎯" + "="*60)
                logger.info("🎯 STEP 2: PROCESSING GL ACCOUNTS 🎯")
                logger.info("🎯" + "="*60)
                
                data_file = data_files['gl_accounts']
                
                try:
                    # Process from local file
                    result = self._process_file_from_local_path(data_file, 'GL')
                    
                    logger.info(f"✅ GL processing completed: {result.get('message', 'Success')}")
                    
                except Exception as e:
                    logger.error(f"❌ GL processing failed: {e}")
            else:
                logger.info("⏭️ Skipping GL processing - COA processing failed or no GL file")
            
            logger.info("🎯" + "="*60)
            logger.info("🎯 SINGLE THREAD PROCESSING COMPLETED! 🎯")
            logger.info("🎯" + "="*60)
            
        except Exception as e:
            logger.error(f"❌ Background processing failed: {e}")
    
    def _process_files_with_local_saving(self, files, engagement, metadata, results):
        """
        Process files in single background thread using local file saving:
        - If COA file exists: COA first, then GL only if COA succeeds
        - If NO COA file: GL processes directly
        """
        coa_success = False
        
        try:
            # Step 1: Process COA files if they exist
            if 'chart_of_accounts' in files:
                logger.info("🎯" + "="*60)
                logger.info("🎯 STEP 1: PROCESSING CHART OF ACCOUNTS 🎯")
                logger.info("🎯" + "="*60)
                
                file_obj = files['chart_of_accounts']
                
                try:
                    # Create data file record
                    data_file = self._create_data_file_record(file_obj, engagement, 'COA', metadata)
                    
                    # Save file locally
                    self._save_file_locally(data_file, file_obj, 'COA')
                    
                    # Process from local file (don't pass file_obj as it might be closed)
                    result = self._process_file_from_local_path(data_file, 'COA')
                    coa_success = result.get('status') == 'completed'
                    
                    logger.info(f"✅ COA processing completed: {result.get('message', 'Success')}")
                    
                except Exception as e:
                    logger.error(f"❌ COA processing failed: {e}")
                    coa_success = False
            
            # Step 2: Process GL files (only if COA succeeded or no COA file)
            if 'gl_accounts' in files and (coa_success or 'chart_of_accounts' not in files):
                logger.info("🎯" + "="*60)
                logger.info("🎯 STEP 2: PROCESSING GL ACCOUNTS 🎯")
                logger.info("🎯" + "="*60)
                
                file_obj = files['gl_accounts']
                
                try:
                    # Create data file record
                    data_file = self._create_data_file_record(file_obj, engagement, 'GL', metadata)
                    
                    # Save file locally
                    self._save_file_locally(data_file, file_obj, 'GL')
                    
                    # Process from local file (don't pass file_obj as it might be closed)
                    result = self._process_file_from_local_path(data_file, 'GL')
                    
                    logger.info(f"✅ GL processing completed: {result.get('message', 'Success')}")
                    
                except Exception as e:
                    logger.error(f"❌ GL processing failed: {e}")
            else:
                logger.info("⏭️ Skipping GL processing - COA processing failed or no GL file")
            
            logger.info("🎯" + "="*60)
            logger.info("🎯 SINGLE THREAD PROCESSING COMPLETED! 🎯")
            logger.info("🎯" + "="*60)
            
        except Exception as e:
            logger.error(f"❌ Background processing failed: {e}")
    
    def _process_file_from_local_path(self, data_file: DataFile, file_type: str):
        """
        Process file from local path (for background processing)
        """
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Read from the saved local file
            file_reader = FileReader()
            df = file_reader.read_file_from_path(data_file.local_file_path)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Process the data
            processor = DataProcessor(data_file)
            
            if file_type == 'COA':
                # Use bulk loading for COA files to handle field length issues
                result = self._process_coa_with_bulk_loading(data_file, df)
                message = f"Chart of Accounts processed: {result['processed_count']} records"
            elif file_type == 'GL':
                # Use chunked processing for GL files to handle large datasets
                result = processor.process_gl_data_chunked(data_file.local_file_path, chunk_size=25000)
                message = f"General Ledger processed: {result['processed_count']} records"
            else:
                raise Exception(f"Unsupported file type: {file_type}")
            
            # Update data file with results
            data_file.status = 'COMPLETED'
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.processed_at = timezone.now()
            data_file.save()
            
            logger.info(f"Successfully processed {file_type} file {data_file.file_name} from local path: {result}")
            
            return {
                'file_type': file_type,
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'file_size': data_file.file_size,
                'status': 'completed',
                'total_records': data_file.total_records,
                'processed_records': data_file.processed_records,
                'failed_records': data_file.failed_records,
                'message': message
            }
            
        except Exception as e:
            # Update data file status to failed
            data_file.status = 'FAILED'
            data_file.save()
            
            logger.error(f"Error processing {file_type} file from local path: {e}")
            
            return {
                'file_type': file_type,
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def _process_coa_with_bulk_loading(self, data_file: DataFile, df: pd.DataFrame) -> Dict[str, int]:
        """
        Process COA data using bulk loading to handle field length issues
        """
        try:
            from .bulk_loading_utils import SQLAlchemyBulkLoader
            
            # Create bulk loader
            bulk_loader = SQLAlchemyBulkLoader()
            
            # Use bulk loading for COA data
            result = bulk_loader.bulk_load_coa_with_pandas(df, str(data_file.id))
            
            if result['success']:
                return {
                    'processed_count': result['records_loaded'],
                    'failed_count': 0
                }
            else:
                logger.error(f"❌ COA bulk loading failed: {result.get('error', 'Unknown error')}")
                return {
                    'processed_count': 0,
                    'failed_count': len(df)
                }
                
        except Exception as e:
            logger.error(f"❌ Error in COA bulk loading: {e}")
            return {
                'processed_count': 0,
                'failed_count': len(df)
            }
    
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
            # Files are now saved locally and not cleaned up
            logger.info(f"💾 GL file saved locally: {data_file.local_file_path}")
    
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
            # Files are now saved locally and not cleaned up
            logger.info(f"💾 File saved locally and preserved")
    
    def _process_gl_file_sync(self, data_file: DataFile, file_obj):
        """Process GL Listing file synchronously for better reliability"""
        try:
            # Update data file status to processing
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Save file locally first, then read from local file
            self._save_file_locally(data_file, file_obj, 'GL')
            
            # Read from the saved local file
            file_reader = FileReader()
            df = file_reader.read_file_from_path(data_file.local_file_path)
            
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
    
    def _save_file_locally(self, data_file: DataFile, file_obj, file_type: str):
        """
        Save uploaded file locally in organized folder structure:
        engagement_name/version/files/
        """
        import os
        from django.conf import settings
        
        try:
            # Create folder structure: engagement_name/version/
            engagement_name = data_file.engagement.client.client_name.replace(' ', '_').replace('/', '_')
            version = data_file.version
            local_base_path = os.path.join(settings.MEDIA_ROOT, 'local_files', engagement_name, version)
            
            # Create directories if they don't exist
            os.makedirs(local_base_path, exist_ok=True)
            
            # Generate safe filename
            safe_filename = file_obj.name.replace(' ', '_').replace('/', '_')
            
            # Save file to local folder
            local_file_path = os.path.join(local_base_path, safe_filename)
            
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Write file content
            with open(local_file_path, 'wb') as local_file:
                for chunk in file_obj.chunks():
                    local_file.write(chunk)
            
            # Store local file path in data_file
            data_file.local_file_path = local_file_path
            data_file.save()
            
            logger.info(f"💾 File saved locally: {local_file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save file locally: {e}")
            # Don't raise exception - continue with processing even if local save fails
    
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
            
            # Save file locally first, then read from local file
            self._save_file_locally(data_file, file_obj, file_type)
            
            # Read from the saved local file
            file_reader = FileReader()
            df = file_reader.read_file_from_path(data_file.local_file_path)
            
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

    # File cleanup method removed - files are now saved locally and preserved


# ============================================================================
# COMPLETENESS TEST API VIEWS
# ============================================================================

class CompletenessTestPagination(PageNumberPagination):
    """Custom pagination for completeness test results"""
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 100


def _clean_comprehensive_statistics(comprehensive_stats):
    """
    Clean comprehensive statistics to remove detailed arrays and keep only summary data
    """
    if not comprehensive_stats:
        return None
    
    cleaned_stats = {}
    
    # Keep chart data with full arrays for charts
    if 'chart_data' in comprehensive_stats:
        chart_data = comprehensive_stats['chart_data']
        cleaned_chart_data = {}
        
        # Keep top accounts data with full arrays for charting
        if 'top_accounts' in chart_data:
            top_accounts = chart_data['top_accounts']
            cleaned_chart_data['top_accounts'] = {
                'labels': top_accounts.get('labels', []),
                'debit_amounts': top_accounts.get('debit_amounts', []),
                'credit_amounts': top_accounts.get('credit_amounts', []),
                'net_movements': top_accounts.get('net_movements', []),
                # Add summary stats for quick reference
                'total_accounts': len(top_accounts.get('labels', [])) if isinstance(top_accounts.get('labels'), list) else len(top_accounts.get('labels', '').split()) if isinstance(top_accounts.get('labels'), str) else 0,
                'total_debit_amount': sum(top_accounts.get('debit_amounts', [])) if isinstance(top_accounts.get('debit_amounts'), list) else sum([float(x) for x in top_accounts.get('debit_amounts', '').split()]) if isinstance(top_accounts.get('debit_amounts'), str) else 0,
                'total_credit_amount': sum(top_accounts.get('credit_amounts', [])) if isinstance(top_accounts.get('credit_amounts'), list) else sum([float(x) for x in top_accounts.get('credit_amounts', '').split()]) if isinstance(top_accounts.get('credit_amounts'), str) else 0,
                'net_balance': sum(top_accounts.get('net_movements', [])) if isinstance(top_accounts.get('net_movements'), list) else sum([float(x) for x in top_accounts.get('net_movements', '').split()]) if isinstance(top_accounts.get('net_movements'), str) else 0
            }
        
        # Keep chart metadata
        if 'chart_metadata' in chart_data:
            cleaned_chart_data['chart_metadata'] = chart_data['chart_metadata']
        
        # Keep monthly trends with full arrays for charting
        if 'monthly_trends' in chart_data:
            monthly_trends = chart_data['monthly_trends']
            cleaned_chart_data['monthly_trends'] = {
                'labels': monthly_trends.get('labels', []),
                'amounts': monthly_trends.get('amounts', []),
                'transaction_counts': monthly_trends.get('transaction_counts', []),
                # Add summary stats for quick reference
                'total_months': len(monthly_trends.get('labels', [])),
                'total_amount': sum(monthly_trends.get('amounts', [])),
                'total_transactions': sum(monthly_trends.get('transaction_counts', []))
            }
        
        cleaned_stats['chart_data'] = cleaned_chart_data
    
    # Keep summary statistics
    if 'summary_statistics' in comprehensive_stats:
        cleaned_stats['summary_statistics'] = comprehensive_stats['summary_statistics']
    
    # Clean document statistics - remove account codes arrays
    if 'document_statistics' in comprehensive_stats:
        doc_stats = comprehensive_stats['document_statistics']
        cleaned_stats['document_statistics'] = {
            'gl_debit_total': doc_stats.get('gl_debit_total', 0),
            'gl_credit_total': doc_stats.get('gl_credit_total', 0),
            'gl_net_balance': doc_stats.get('gl_net_balance', 0),
            'unique_accounts': doc_stats.get('unique_accounts', 0),
            'total_gl_records': doc_stats.get('total_gl_records', 0),
            'total_tb_records': doc_stats.get('total_tb_records', 0)
        }
    
    # Clean enhanced statistics - remove detailed arrays
    if 'enhanced_statistics' in comprehensive_stats:
        enhanced_stats = comprehensive_stats['enhanced_statistics']
        cleaned_enhanced = {}
        
        # Keep basic totals
        if 'basic_totals' in enhanced_stats:
            cleaned_enhanced['basic_totals'] = enhanced_stats['basic_totals']
        
        # Keep amount statistics
        if 'amount_statistics' in enhanced_stats:
            cleaned_enhanced['amount_statistics'] = enhanced_stats['amount_statistics']
        
        # Keep data quality metrics
        if 'data_quality_metrics' in enhanced_stats:
            cleaned_enhanced['data_quality_metrics'] = enhanced_stats['data_quality_metrics']
        
        # Keep per account statistics
        if 'per_account_statistics' in enhanced_stats:
            cleaned_enhanced['per_account_statistics'] = enhanced_stats['per_account_statistics']
        
        # Keep enhanced chart data with full arrays for charting
        if 'chart_data_enhanced' in enhanced_stats:
            chart_data_enhanced = enhanced_stats['chart_data_enhanced']
            cleaned_enhanced['chart_data_enhanced'] = {}
            
            # Convert string data to arrays for each chart type
            for chart_type, chart_data in chart_data_enhanced.items():
                if isinstance(chart_data, dict):
                    cleaned_chart = {}
                    for key, value in chart_data.items():
                        if isinstance(value, str) and value.strip():
                            # Convert space-separated string to array
                            string_array = value.split()
                            # Try to convert to numbers if possible
                            try:
                                if key in ['total_amounts', 'transaction_counts', 'net_amounts', 'debit_totals', 'credit_totals']:
                                    # Convert to float for numeric arrays
                                    cleaned_chart[key] = [float(x) for x in string_array]
                                else:
                                    # Keep as strings for labels
                                    cleaned_chart[key] = string_array
                            except ValueError:
                                # If conversion fails, keep as strings
                                cleaned_chart[key] = string_array
                        else:
                            cleaned_chart[key] = value
                    cleaned_enhanced['chart_data_enhanced'][chart_type] = cleaned_chart
                else:
                    cleaned_enhanced['chart_data_enhanced'][chart_type] = chart_data
        
        # Clean audit calculation statistics - remove detailed arrays
        if 'audit_calculation_statistics' in enhanced_stats:
            audit_stats = enhanced_stats['audit_calculation_statistics']
            cleaned_audit = {}
            
            # Keep only summary data, remove detailed arrays
            for key, value in audit_stats.items():
                if isinstance(value, str) and value:  # Keep string summaries
                    cleaned_audit[key] = value
                elif isinstance(value, dict):  # Keep dict summaries
                    cleaned_audit[key] = value
            
            cleaned_enhanced['audit_calculation_statistics'] = cleaned_audit
        
        cleaned_stats['enhanced_statistics'] = cleaned_enhanced
    
    return cleaned_stats


@api_view(['GET'])
@permission_classes([AllowAny])
def get_completeness_test_by_engagement(request, engagement_id):
    """
    Get completeness test results for a specific engagement - SINGLE RESULT WITH ENGAGEMENT DETAILS
    
    GET /api/completeness-test/engagement/{engagement_id}/
    
    Query Parameters:
    - summary: boolean (default: false) - Return summary only
    - latest: boolean (default: true) - Return only the latest test result
    
    Returns:
    - Single test result (not array)
    - Engagement details (name, client, fiscal year, status)
    - Statistics and summary data only (no detailed verification listings)
    - Step summaries with proper data extraction
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
        
        # Convert queryset to list to avoid QuerySet serialization issues
        queryset_list = list(queryset)
        
        # Create cleaned results with only statistics
        cleaned_results = []
        for result in queryset_list:
            # Extract only statistics and summary data
            cleaned_result = {
                'id': str(result.id),
                'engagement_id': result.engagement.engagement_id,
                'engagement_name': result.engagement.engagement_name,
                'client_name': result.engagement.client.client_name,
                'test_timestamp': result.test_timestamp,
                'test_version': result.test_version,
                
                # File information (names only)
                'gl_file_name': result.gl_file.file_name if result.gl_file else None,
                'tb_file_name': result.tb_file.file_name if result.tb_file else None,
                'coa_file_name': result.coa_file.file_name if result.coa_file else None,
                
                # Test results
                'overall_status': result.overall_status,
                'overall_explanation': result.overall_explanation,
                'completeness_score': result.completeness_score,
                
                # Statistics only
                'total_gl_records': result.total_gl_records,
                'total_tb_records': result.total_tb_records,
                'total_coa_records': result.total_coa_records,
                'total_accounts_unified': result.total_accounts_unified,
                'tests_passed': result.tests_passed,
                'total_tests': result.total_tests,
                'critical_issues_count': result.critical_issues_count,
                'processing_duration': result.processing_duration,
                
                # Step summaries (statistics only) - using actual data structure
                'step1_summary': {
                    'status': 'PASS' if result.step1_file_completeness.get('passed', False) else 'FAIL' if result.step1_file_completeness else 'UNKNOWN',
                    'passed': result.step1_file_completeness.get('passed', False) if result.step1_file_completeness else False,
                    'description': result.step1_file_completeness.get('description', '') if result.step1_file_completeness else '',
                    'explanation': result.step1_file_completeness.get('explanation', '') if result.step1_file_completeness else '',
                    'gl_balance_status': 'PASS' if result.step1_file_completeness.get('gl_is_balanced', False) else 'FAIL' if result.step1_file_completeness else 'UNKNOWN',
                    'data_volume_status': 'PASS' if result.step1_file_completeness.get('volume_check', False) else 'FAIL' if result.step1_file_completeness else 'UNKNOWN',
                    'total_debit': result.step1_file_completeness.get('gl_debit_total', 0) if result.step1_file_completeness else 0,
                    'total_credit': result.step1_file_completeness.get('gl_credit_total', 0) if result.step1_file_completeness else 0,
                    'balance_difference': result.step1_file_completeness.get('gl_net_balance', 0) if result.step1_file_completeness else 0,
                    'transaction_count': result.step1_file_completeness.get('transaction_count', 0) if result.step1_file_completeness else 0,
                    'account_count': result.step1_file_completeness.get('account_count', 0) if result.step1_file_completeness else 0,
                    'has_tb': result.step1_file_completeness.get('has_tb', False) if result.step1_file_completeness else False,
                } if result.step1_file_completeness else None,
                
                'step2_summary': {
                    'status': 'PASS' if result.step2_gl_tb_reconciliation.get('passed', False) else 'FAIL' if result.step2_gl_tb_reconciliation else 'UNKNOWN',
                    'passed': result.step2_gl_tb_reconciliation.get('passed', False) if result.step2_gl_tb_reconciliation else False,
                    'description': result.step2_gl_tb_reconciliation.get('description', '') if result.step2_gl_tb_reconciliation else '',
                    'explanation': result.step2_gl_tb_reconciliation.get('explanation', '') if result.step2_gl_tb_reconciliation else '',
                    'accounts_verified': result.step2_gl_tb_reconciliation.get('total_accounts_verified', 0) if result.step2_gl_tb_reconciliation else 0,
                    'accounts_passed': result.step2_gl_tb_reconciliation.get('accounts_passed', 0) if result.step2_gl_tb_reconciliation else 0,
                    'accounts_failed': result.step2_gl_tb_reconciliation.get('accounts_failed', 0) if result.step2_gl_tb_reconciliation else 0,
                    'pass_rate': result.step2_gl_tb_reconciliation.get('pass_rate', 0) if result.step2_gl_tb_reconciliation else 0,
                    'total_variance': result.step2_gl_tb_reconciliation.get('total_variance', 0) if result.step2_gl_tb_reconciliation else 0,
                    'account_verifications_count': len(result.step2_gl_tb_reconciliation.get('account_verifications', [])) if result.step2_gl_tb_reconciliation else 0,
                    'failed_accounts_count': len(result.step2_gl_tb_reconciliation.get('failed_accounts', [])) if result.step2_gl_tb_reconciliation else 0,
                } if result.step2_gl_tb_reconciliation else None,
                
                # Comprehensive statistics (cleaned - statistics only)
                'comprehensive_statistics': _clean_comprehensive_statistics(result.comprehensive_statistics) if result.comprehensive_statistics else None,
                
                'created_at': result.created_at,
                'updated_at': result.updated_at
            }
            
            # Add summary only if not summary_only
            if not summary_only:
                cleaned_result['test_summary'] = result.get_summary() if hasattr(result, 'get_summary') else None
                cleaned_result['engagement_completeness_status'] = result.get_engagement_completeness_status() if hasattr(result, 'get_engagement_completeness_status') else None
                cleaned_result['failed_tests'] = result.get_failed_tests() if hasattr(result, 'get_failed_tests') else None
            
            cleaned_results.append(cleaned_result)
        
        # Get the latest result (single result, not array)
        latest_result = queryset_list[0] if queryset_list else None
        
        if not latest_result:
            return Response({
                'engagement_id': engagement_id,
                'message': f'No completeness test results found for engagement {engagement_id}'
            }, status=status.HTTP_200_OK)
        
        # Get engagement details
        engagement = latest_result.engagement
        
        # Prepare single result response with engagement details
        response_data = {
            'engagement_id': engagement_id,
            'engagement_name': engagement.engagement_name,
            'client_name': engagement.client.client_name,
            'client_id': engagement.client.id,
            'fiscal_year': engagement.fiscal_year,
            'engagement_status': engagement.status,
            'engagement_created_at': engagement.created_at,
            'engagement_updated_at': engagement.updated_at,
            
            # Test result (single object, not array)
            'test_result': cleaned_results[0] if cleaned_results else None,
            
            # Summary statistics
            'summary': {
                'test_timestamp': latest_result.test_timestamp,
                'overall_status': latest_result.overall_status,
                'completeness_score': latest_result.completeness_score,
                'tests_passed': latest_result.tests_passed,
                'total_tests': latest_result.total_tests,
                'critical_issues': latest_result.critical_issues_count,
                'total_gl_records': latest_result.total_gl_records,
                'total_tb_records': latest_result.total_tb_records,
                'total_coa_records': latest_result.total_coa_records,
                'processing_duration': latest_result.processing_duration
            }
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


class DocumentVerificationsPagination(PageNumberPagination):
    """Custom pagination for document verifications"""
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 500


class ProfitCenterDataPagination(PageNumberPagination):
    """Custom pagination for profit center data"""
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000


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





# ============================================================================
# DOCUMENT VERIFICATIONS API VIEW
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_document_verifications_by_engagement(request, engagement_id):
    """
    Get document verifications for a specific engagement with enhanced pagination
    
    Returns both successful (balanced) and failed (unbalanced) document verifications
    from CompletenessTestResult data with comprehensive statistics, filtering options, 
    and robust pagination.
    
    GET /api/document-verifications/engagement/{engagement_id}/
    
    Data Source:
    - Uses document verification data stored in CompletenessTestResult.comprehensive_statistics
    - Falls back to summary statistics from CompletenessTestResult fields if detailed data unavailable
    - No direct SAPGLPosting queries for better performance and consistency
    
    Query Parameters:
    - page: Page number for pagination (default: 1)
    - page_size: Number of results per page (default: 50, max: 500)
    - unbalanced_only: boolean (default: false) - true=unbalanced documents only, false=all documents
    - latest: boolean (default: true) - Return only from latest test result
    - document_number: Filter by specific document number
    - min_variance: Filter by minimum balance variance
    - sort_by: Sort field (document_number, net_balance, transaction_count, account_count, balance_variance)
    - sort_order: Sort order (asc, desc) - default: desc for variance, asc for document_number
    
    Response includes:
    - summary_statistics: Overall document verification statistics from CompletenessTestResult
    - document_verification_summary: Separate counts for successful and failed verifications
    - filtered_results: Information about applied filters
    - pagination: Comprehensive pagination metadata with navigation info
    - results: Array of document verification details with status indicators
    
    Performance Benefits:
    - Uses pre-calculated CompletenessTestResult data (no real-time SAPGLPosting queries)
    - Faster response times for large datasets
    - Consistent data with completeness test results
    - Automatic page size validation (1-500 range)
    - Page number validation and bounds checking
    """
    try:
        # Get query parameters
        unbalanced_only = request.query_params.get('unbalanced_only', 'false').lower() == 'true'
        latest_only = request.query_params.get('latest', 'true').lower() == 'true'
        document_number_filter = request.query_params.get('document_number', '').strip()
        min_variance = request.query_params.get('min_variance')
        sort_by = request.query_params.get('sort_by', 'transaction_count')
        sort_order = request.query_params.get('sort_order', 'desc')
        
        # Convert min_variance to float if provided
        if min_variance:
            try:
                min_variance = float(min_variance)
            except ValueError:
                min_variance = None
        
        # Get the latest completeness test result for the engagement
        test_result = CompletenessTestResult.objects.filter(
            engagement__engagement_id=engagement_id
        ).order_by('-test_timestamp').first()
        
        if not test_result:
            return Response({
                'engagement_id': engagement_id,
                'message': f'No completeness test results found for engagement {engagement_id}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Extract document verification data from CompletenessTestResult
        logger.info(f"Document verifications debug for {engagement_id}:")
        logger.info(f"  Using document verification data from CompletenessTestResult")
        logger.info(f"  Total Documents: {test_result.total_documents}")
        logger.info(f"  Balanced Documents: {test_result.balanced_documents}")
        logger.info(f"  Unbalanced Documents: {test_result.unbalanced_documents}")
        
        # Extract document verification results from comprehensive_statistics
        document_verifications = []
        comprehensive_stats = test_result.comprehensive_statistics or {}
        
        # Check for document verification data in the correct locations based on our analysis
        document_verifications = []
        
        # First, check if document_verification_results is directly in comprehensive_statistics
        if 'document_verification_results' in comprehensive_stats:
            document_verifications = comprehensive_stats['document_verification_results']
            logger.info(f"  Found {len(document_verifications)} document verifications in comprehensive_statistics")
        else:
            # Check enhanced_statistics.audit_calculation_statistics.document_balance_verification
            enhanced_stats = comprehensive_stats.get('enhanced_statistics', {})
            audit_stats = enhanced_stats.get('audit_calculation_statistics', {})
            doc_balance_verification = audit_stats.get('document_balance_verification', {})
            
            if doc_balance_verification:
                logger.info(f"  Found document balance verification data in enhanced_statistics")
                logger.info(f"  Document verification data: {doc_balance_verification}")
                
                # Create a more informative response since we have verification data but no detailed document list
            return Response({
                'engagement_id': engagement_id,
                    'test_id': str(test_result.id),
                    'test_timestamp': test_result.test_timestamp,
                    'message': 'Document verification summary available, but detailed document list not stored in completeness test results',
                    'summary_statistics': {
                        'total_documents': test_result.total_documents,
                        'balanced_documents': test_result.balanced_documents,
                        'unbalanced_documents': test_result.unbalanced_documents,
                        'balance_rate_percentage': round(test_result.document_balance_rate * 100, 2),
                        'total_variance': float(test_result.total_document_variance),
                        'average_variance': float(test_result.average_document_variance)
                    },
                    'document_verification_summary': {
                        'successful_verifications': {
                            'count': test_result.balanced_documents,
                            'percentage': round(test_result.document_balance_rate * 100, 2),
                            'status': 'PASSED'
                        },
                        'failed_verifications': {
                            'count': test_result.unbalanced_documents,
                            'percentage': round((1 - test_result.document_balance_rate) * 100, 2),
                            'status': 'FAILED',
                            'total_variance': float(test_result.total_document_variance),
                            'average_variance': float(test_result.average_document_variance)
                        }
                    },
                    'verification_details': {
                        'verification_rule': doc_balance_verification.get('verification_rule', ''),
                        'verification_passed': doc_balance_verification.get('verification_passed', False),
                        'critical_issues': doc_balance_verification.get('critical_issues', 0),
                        'recommendation': doc_balance_verification.get('recommendation', '')
                    },
                    'data_availability': {
                        'detailed_document_list': False,
                        'summary_statistics': True,
                        'verification_results': True,
                        'note': 'Detailed document verification list is not stored in CompletenessTestResult. Only summary statistics are available.'
                    },
                    'results': []
                }, status=status.HTTP_200_OK)
        
        # If no detailed document verification data is available, return summary only
        if not document_verifications:
            logger.warning(f"  No detailed document verification data found in CompletenessTestResult")
            logger.info(f"  Available comprehensive_statistics keys: {list(comprehensive_stats.keys())}")
            
            return Response({
                'engagement_id': engagement_id,
                'test_id': str(test_result.id),
                'test_timestamp': test_result.test_timestamp,
                'message': 'Document verification data not available in completeness test results',
                'summary_statistics': {
                    'total_documents': test_result.total_documents,
                    'balanced_documents': test_result.balanced_documents,
                    'unbalanced_documents': test_result.unbalanced_documents,
                    'balance_rate_percentage': round(test_result.document_balance_rate * 100, 2),
                    'total_variance': float(test_result.total_document_variance),
                    'average_variance': float(test_result.average_document_variance)
                },
                'document_verification_summary': {
                    'successful_verifications': {
                        'count': test_result.balanced_documents,
                        'percentage': round(test_result.document_balance_rate * 100, 2),
                        'status': 'PASSED'
                    },
                    'failed_verifications': {
                        'count': test_result.unbalanced_documents,
                        'percentage': round((1 - test_result.document_balance_rate) * 100, 2),
                        'status': 'FAILED',
                        'total_variance': float(test_result.total_document_variance),
                        'average_variance': float(test_result.average_document_variance)
                    }
                },
                'results': []
            }, status=status.HTTP_200_OK)
        
        # Ensure document_verifications is a list
        if not isinstance(document_verifications, list):
            document_verifications = []
        
        # Separate balanced and unbalanced documents
        balanced_documents = [v for v in document_verifications if v.get('is_balanced', True)]
        unbalanced_documents = [v for v in document_verifications if not v.get('is_balanced', True)]
        
        # Apply filters
        filtered_verifications = document_verifications.copy()
        
        if unbalanced_only:
            # Return only unbalanced documents
            filtered_verifications = unbalanced_documents.copy()
        elif unbalanced_only is False:
            # Return only balanced/successful documents
            filtered_verifications = balanced_documents.copy()
        
        if document_number_filter:
            filtered_verifications = [v for v in filtered_verifications 
                                    if document_number_filter.lower() in v.get('document_number', '').lower()]
        
        if min_variance is not None:
            filtered_verifications = [v for v in filtered_verifications 
                                    if v.get('balance_variance', 0) >= min_variance]
        
        # Apply sorting
        if sort_by in ['document_number', 'net_balance', 'transaction_count', 'account_count', 'balance_variance']:
            try:
                reverse = sort_order.lower() == 'desc'
                if sort_by == 'document_number':
                    filtered_verifications.sort(key=lambda x: x.get(sort_by, ''), reverse=reverse)
                else:
                    filtered_verifications.sort(key=lambda x: float(x.get(sort_by, 0)), reverse=reverse)
            except (ValueError, TypeError):
                # If sorting fails, keep original order
                pass
        
        # Add computed fields for better display
        for verification in filtered_verifications:
            verification['balance_variance_abs'] = abs(verification.get('balance_variance', 0))
            verification['variance_formatted'] = f"{verification.get('balance_variance_abs', 0):,.2f}"
            verification['status'] = 'BALANCED' if verification.get('is_balanced', True) else 'UNBALANCED'
        
        # Use statistics from CompletenessTestResult (more accurate than recalculating)
        total_documents = test_result.total_documents
        total_balanced = test_result.balanced_documents
        total_unbalanced = test_result.unbalanced_documents
        balance_rate = test_result.document_balance_rate * 100  # Convert to percentage
        total_variance = float(test_result.total_document_variance)
        average_variance = float(test_result.average_document_variance)
        
        # If we have detailed document verification data, use it for filtering and sorting
        if document_verifications:
            # Recalculate from detailed data for consistency
            calculated_balanced = len([v for v in document_verifications if v.get('is_balanced', True)])
            calculated_unbalanced = len([v for v in document_verifications if not v.get('is_balanced', True)])
            
            # Use calculated values if they differ significantly from stored values
            if abs(calculated_balanced - total_balanced) > 5 or abs(calculated_unbalanced - total_unbalanced) > 5:
                logger.warning(f"Document count mismatch: stored({total_balanced}/{total_unbalanced}) vs calculated({calculated_balanced}/{calculated_unbalanced})")
                total_balanced = calculated_balanced
                total_unbalanced = calculated_unbalanced
                total_documents = total_balanced + total_unbalanced
                balance_rate = (total_balanced / total_documents * 100) if total_documents > 0 else 0
        
        # Apply pagination
        paginator = DocumentVerificationsPagination()
        
        # Get pagination parameters from request
        page_number = request.query_params.get('page', 1)
        page_size = request.query_params.get('page_size', paginator.page_size)
        
        # Ensure page_size is within limits
        try:
            page_size = int(page_size)
            if page_size > paginator.max_page_size:
                page_size = paginator.max_page_size
            elif page_size < 1:
                page_size = paginator.page_size
        except (ValueError, TypeError):
            page_size = paginator.page_size
        
        # Calculate pagination manually for better control
        total_items = len(filtered_verifications)
        total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
        
        try:
            page_number = int(page_number)
            if page_number < 1:
                page_number = 1
            elif page_number > total_pages and total_pages > 0:
                page_number = total_pages
        except (ValueError, TypeError):
            page_number = 1
        
        # Calculate start and end indices
        start_index = (page_number - 1) * page_size
        end_index = start_index + page_size
        
        # Get the page data
        page_data = filtered_verifications[start_index:end_index]
        
        # Create pagination metadata
        pagination_info = {
            'current_page': page_number,
            'page_size': page_size,
            'total_pages': total_pages,
            'total_items': total_items,
            'has_next': page_number < total_pages,
            'has_previous': page_number > 1,
            'next_page': page_number + 1 if page_number < total_pages else None,
            'previous_page': page_number - 1 if page_number > 1 else None,
            'start_index': start_index + 1 if total_items > 0 else 0,
            'end_index': min(end_index, total_items)
        }
        
        # Always return paginated response with comprehensive metadata
        response_data = {
            'engagement_id': engagement_id,
            'test_id': str(test_result.id),
            'test_timestamp': test_result.test_timestamp,
        'summary_statistics': {
            'total_documents': total_documents,
            'balanced_documents': total_balanced,
            'unbalanced_documents': total_unbalanced,
            'balance_rate_percentage': round(balance_rate, 2),
            'total_variance': round(total_variance, 2),
            'average_variance': round(average_variance, 2)
        },
        'document_verification_summary': {
            'successful_verifications': {
                'count': total_balanced,
                'percentage': round(balance_rate, 2),
                'status': 'PASSED'
            },
            'failed_verifications': {
                'count': total_unbalanced,
                'percentage': round(100 - balance_rate, 2),
                'status': 'FAILED',
                'total_variance': round(total_variance, 2),
                'average_variance': round(average_variance, 2)
            }
        },
        'filtered_results': {
            'total_filtered': len(filtered_verifications),
        'filters_applied': {
            'unbalanced_only': unbalanced_only,
            'document_number': document_number_filter,
            'min_variance': min_variance,
            'sort_by': sort_by,
            'sort_order': sort_order
            }
        },
        'pagination': pagination_info,
        'results': page_data
        }
        
        return Response(response_data, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving document verifications for engagement {engagement_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ============================================================================
# PROFIT CENTER DATA API VIEW
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_profit_center_data_by_engagement(request, engagement_id):
    """
    Get profit center data for a specific engagement with pagination
    
    GET /api/profit-center-data/engagement/{engagement_id}/
    
    Query Parameters:
    - page: Page number for pagination
    - page_size: Number of results per page (max 1000)
    - profit_center_code: Filter by specific profit center code
    - profit_center_type: Filter by profit center type
    - revenue_center: boolean - Filter revenue centers only
    - cost_center: boolean - Filter cost centers only
    - status: Filter by status (ACTIVE, INACTIVE, BLOCKED)
    - sort_by: Sort field (profit_center_code, profit_center_name, total_amount, transaction_count)
    - sort_order: Sort order (asc, desc) - default: asc for code, desc for amounts
    """
    try:
        # Get query parameters
        profit_center_code_filter = request.query_params.get('profit_center_code', '').strip()
        profit_center_type_filter = request.query_params.get('profit_center_type', '').strip()
        revenue_center_only = request.query_params.get('revenue_center', 'false').lower() == 'true'
        cost_center_only = request.query_params.get('cost_center', 'false').lower() == 'true'
        status_filter = request.query_params.get('status', '').strip()
        sort_by = request.query_params.get('sort_by', 'profit_center_code')
        sort_order = request.query_params.get('sort_order', 'asc')
        
        # Get the engagement
        try:
            engagement = Engagement.objects.get(engagement_id=engagement_id)
        except Engagement.DoesNotExist:
            return Response({
                'engagement_id': engagement_id,
                'message': f'Engagement {engagement_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get GL postings for this engagement to calculate profit center statistics
        gl_postings = SAPGLPosting.objects.filter(
            data_file__engagement=engagement
        ).exclude(profit_center='').exclude(profit_center__isnull=True)
        
        # Group by profit center
        profit_center_analysis = {}
        for posting in gl_postings:
            pc_code = posting.profit_center
            if pc_code not in profit_center_analysis:
                profit_center_analysis[pc_code] = {
                    'debit_total': 0,
                    'credit_total': 0,
                    'transaction_count': 0,
                    'accounts': set(),
                    'users': set(),
                    'document_count': set(),
                    'posting_dates': set()
                }
            
            # Add amounts (positive=debit, negative=credit)
            amount = float(posting.amount_local_currency or 0)
            if amount > 0:
                profit_center_analysis[pc_code]['debit_total'] += amount
            else:
                profit_center_analysis[pc_code]['credit_total'] += abs(amount)
            
            profit_center_analysis[pc_code]['transaction_count'] += 1
            profit_center_analysis[pc_code]['accounts'].add(posting.gl_account)
            profit_center_analysis[pc_code]['users'].add(posting.user_name)
            if posting.document_number:
                profit_center_analysis[pc_code]['document_count'].add(posting.document_number)
            if posting.posting_date:
                profit_center_analysis[pc_code]['posting_dates'].add(str(posting.posting_date))
        
        # Get profit center master data
        profit_centers = ProfitCenter.objects.all()
        
        # Apply filters
        if profit_center_code_filter:
            profit_centers = profit_centers.filter(profit_center_code__icontains=profit_center_code_filter)
        
        if profit_center_type_filter:
            profit_centers = profit_centers.filter(profit_center_type__icontains=profit_center_type_filter)
        
        if revenue_center_only:
            profit_centers = profit_centers.filter(revenue_center=True)
        
        if cost_center_only:
            profit_centers = profit_centers.filter(cost_center=True)
        
        if status_filter:
            profit_centers = profit_centers.filter(profit_center_status=status_filter)
        
        # Create profit center data with statistics
        profit_center_data = []
        for pc in profit_centers:
            pc_stats = profit_center_analysis.get(pc.profit_center_code, {
                'debit_total': 0,
                'credit_total': 0,
                'transaction_count': 0,
                'accounts': set(),
                'users': set(),
                'document_count': set(),
                'posting_dates': set()
            })
            
            net_amount = pc_stats['debit_total'] - pc_stats['credit_total']
            
            pc_data = {
                'id': str(pc.id),
                'profit_center_code': pc.profit_center_code,
                'profit_center_name': pc.profit_center_name,
                'profit_center_short_text': pc.profit_center_short_text,
                'profit_center_type': pc.profit_center_type,
                'profit_center_group': pc.profit_center_group,
                'revenue_center': pc.revenue_center,
                'cost_center': pc.cost_center,
                'profit_center_currency': pc.profit_center_currency,
                'profit_center_status': pc.profit_center_status,
                'company_code': pc.company_code,
                'business_area': pc.business_area,
                'segment': pc.segment,
                'responsible_person': pc.responsible_person,
                'department': pc.department,
                'parent_profit_center': pc.parent_profit_center.profit_center_code if pc.parent_profit_center else None,
                'profit_center_level': pc.profit_center_level,
                'created_at': pc.created_at,
                'updated_at': pc.updated_at,
                
                # Statistics from GL postings
                'debit_total': round(pc_stats['debit_total'], 2),
                'credit_total': round(pc_stats['credit_total'], 2),
                'net_amount': round(net_amount, 2),
                'transaction_count': pc_stats['transaction_count'],
                'account_count': len(pc_stats['accounts']),
                'accounts': list(pc_stats['accounts']),
                'user_count': len(pc_stats['users']),
                'users': list(pc_stats['users']),
                'document_count': len(pc_stats['document_count']),
                'posting_dates': list(pc_stats['posting_dates']),
                'has_activity': pc_stats['transaction_count'] > 0
            }
            
            profit_center_data.append(pc_data)
        
        # Apply sorting
        if sort_by in ['profit_center_code', 'profit_center_name', 'debit_total', 'credit_total', 'net_amount', 'transaction_count']:
            try:
                reverse = sort_order.lower() == 'desc'
                if sort_by in ['profit_center_code', 'profit_center_name']:
                    profit_center_data.sort(key=lambda x: x.get(sort_by, ''), reverse=reverse)
                else:
                    profit_center_data.sort(key=lambda x: float(x.get(sort_by, 0)), reverse=reverse)
            except (ValueError, TypeError):
                # If sorting fails, keep original order
                pass
        
        # Apply pagination
        paginator = ProfitCenterDataPagination()
        page = paginator.paginate_queryset(profit_center_data, request)
        
        if page is not None:
            response_data = {
                'engagement_id': engagement_id,
                'engagement_name': engagement.engagement_name,
                'total_profit_centers': len(profit_center_data),
                'filters_applied': {
                    'profit_center_code': profit_center_code_filter,
                    'profit_center_type': profit_center_type_filter,
                    'revenue_center_only': revenue_center_only,
                    'cost_center_only': cost_center_only,
                    'status': status_filter,
                    'sort_by': sort_by,
                    'sort_order': sort_order
                },
                'results': page
            }
            return paginator.get_paginated_response(response_data)
        
        # If no pagination, return all results
        return Response({
            'engagement_id': engagement_id,
            'engagement_name': engagement.engagement_name,
            'total_profit_centers': len(profit_center_data),
            'filters_applied': {
                'profit_center_code': profit_center_code_filter,
                'profit_center_type': profit_center_type_filter,
                'revenue_center_only': revenue_center_only,
                'cost_center_only': cost_center_only,
                'status': status_filter,
                'sort_by': sort_by,
                'sort_order': sort_order
            },
            'results': profit_center_data
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving profit center data for engagement {engagement_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_profit_center_data_by_account(request, account_id):
    """
    Get profit center data for a specific GL account with pagination
    
    GET /api/profit-center-data/account/{account_id}/
    
    Returns profit centers that have transactions with the specified GL account,
    including GL account type, sub type, and sub sub type information.
    
    Query Parameters:
    - page: Page number for pagination
    - page_size: Number of results per page (max 1000)
    - profit_center_code: Filter by specific profit center code
    - profit_center_type: Filter by profit center type
    - revenue_center: boolean - Filter revenue centers only
    - cost_center: boolean - Filter cost centers only
    - status: Filter by status (ACTIVE, INACTIVE, BLOCKED)
    - sort_by: Sort field (profit_center_code, profit_center_name, total_amount, transaction_count)
    - sort_order: Sort order (asc, desc) - default: asc for code, desc for amounts
    
    Response includes:
    - GL account information (type, sub type, sub sub type)
    - Profit centers with activity for this GL account
    - Transaction statistics specific to this GL account
    """
    try:
        # Get query parameters
        profit_center_code_filter = request.query_params.get('profit_center_code', '').strip()
        profit_center_type_filter = request.query_params.get('profit_center_type', '').strip()
        revenue_center_only = request.query_params.get('revenue_center', 'false').lower() == 'true'
        cost_center_only = request.query_params.get('cost_center', 'false').lower() == 'true'
        status_filter = request.query_params.get('status', '').strip()
        sort_by = request.query_params.get('sort_by', 'profit_center_code')
        sort_order = request.query_params.get('sort_order', 'asc')
        
        # Get GL postings for this specific account
        gl_postings = SAPGLPosting.objects.filter(
            gl_account=account_id
        ).exclude(profit_center='').exclude(profit_center__isnull=True)
        
        # Get GL account information
        try:
            from core.models import GLAccount, ChartOfAccount
            gl_account = GLAccount.objects.get(account_code=account_id)
            
            # Get type information from GL account
            account_type = gl_account.account_type
            sub_type = gl_account.sub_type
            sub_sub_type = gl_account.sub_sub_type
            
            # If type information is empty, try to get it from Chart of Accounts
            if not account_type or not sub_type or not sub_sub_type:
                try:
                    coa_account = ChartOfAccount.objects.filter(
                        account=account_id
                    ).first()
                    
                    if coa_account:
                        account_type = account_type or coa_account.type
                        sub_type = sub_type or coa_account.sub_type
                        sub_sub_type = sub_sub_type or coa_account.sub_sub_type
                except Exception as e:
                    logger.warning(f"Could not fetch Chart of Accounts data for account {account_id}: {e}")
            
            gl_account_info = {
                'account_code': gl_account.account_code,
                'account_name': gl_account.account_name,
                'account_type': account_type,
                'sub_type': sub_type,
                'sub_sub_type': sub_sub_type,
                'financial_statement_category': gl_account.financial_statement_category,
                'balance_sheet_category': gl_account.balance_sheet_category,
                'income_statement_category': gl_account.income_statement_category,
                'currency': gl_account.currency,
                'is_active': gl_account.is_active,
                'opening_balance': float(gl_account.opening_balance) if gl_account.opening_balance else None,
                'closing_balance': float(gl_account.closing_balance) if gl_account.closing_balance else None
            }
        except GLAccount.DoesNotExist:
            # Try to get basic info from Chart of Accounts if GL account doesn't exist
            try:
                from core.models import ChartOfAccount
                coa_account = ChartOfAccount.objects.filter(account=account_id).first()
                if coa_account:
                    gl_account_info = {
                        'account_code': coa_account.account,
                        'account_name': coa_account.gl_account_long_text or f'Account {coa_account.account}',
                        'account_type': coa_account.type,
                        'sub_type': coa_account.sub_type,
                        'sub_sub_type': coa_account.sub_sub_type,
                        'financial_statement_category': None,
                        'balance_sheet_category': None,
                        'income_statement_category': None,
                        'currency': 'SAR',
                        'is_active': True,
                        'opening_balance': None,
                        'closing_balance': None
                    }
                else:
                    gl_account_info = {
                        'account_code': account_id,
                        'account_name': f'Account {account_id}',
                        'account_type': 'Unknown',
                        'sub_type': None,
                        'sub_sub_type': None,
                        'financial_statement_category': None,
                        'balance_sheet_category': None,
                        'income_statement_category': None,
                        'currency': 'SAR',
                        'is_active': True,
                        'opening_balance': None,
                        'closing_balance': None
                    }
            except Exception as e:
                logger.warning(f"Could not fetch Chart of Accounts data for account {account_id}: {e}")
                gl_account_info = {
                    'account_code': account_id,
                    'account_name': f'Account {account_id}',
                    'account_type': 'Unknown',
                    'sub_type': None,
                    'sub_sub_type': None,
                    'financial_statement_category': None,
                    'balance_sheet_category': None,
                    'income_statement_category': None,
                    'currency': 'SAR',
                    'is_active': True,
                    'opening_balance': None,
                    'closing_balance': None
                }
        
        if not gl_postings.exists():
            return Response({
                'account_id': account_id,
                'message': f'No profit center data found for account {account_id}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Group by profit center for this account
        profit_center_analysis = {}
        for posting in gl_postings:
            pc_code = posting.profit_center
            if pc_code not in profit_center_analysis:
                profit_center_analysis[pc_code] = {
                    'debit_total': 0,
                    'credit_total': 0,
                    'transaction_count': 0,
                    'users': set(),
                    'document_count': set(),
                    'posting_dates': set()
                }
            
            # Add amounts (positive=debit, negative=credit)
            amount = float(posting.amount_local_currency or 0)
            if amount > 0:
                profit_center_analysis[pc_code]['debit_total'] += amount
            else:
                profit_center_analysis[pc_code]['credit_total'] += abs(amount)
            
            profit_center_analysis[pc_code]['transaction_count'] += 1
            profit_center_analysis[pc_code]['users'].add(posting.user_name)
            if posting.document_number:
                profit_center_analysis[pc_code]['document_count'].add(posting.document_number)
            if posting.posting_date:
                profit_center_analysis[pc_code]['posting_dates'].add(str(posting.posting_date))
        
        # Get profit center master data for the profit centers that have activity with this account
        profit_center_codes = list(profit_center_analysis.keys())
        profit_centers = ProfitCenter.objects.filter(profit_center_code__in=profit_center_codes)
        
        # Apply filters
        if profit_center_code_filter:
            profit_centers = profit_centers.filter(profit_center_code__icontains=profit_center_code_filter)
        
        if profit_center_type_filter:
            profit_centers = profit_centers.filter(profit_center_type__icontains=profit_center_type_filter)
        
        if revenue_center_only:
            profit_centers = profit_centers.filter(revenue_center=True)
        
        if cost_center_only:
            profit_centers = profit_centers.filter(cost_center=True)
        
        if status_filter:
            profit_centers = profit_centers.filter(profit_center_status=status_filter)
        
        # Create profit center data with statistics
        profit_center_data = []
        for pc in profit_centers:
            pc_stats = profit_center_analysis.get(pc.profit_center_code, {
                'debit_total': 0,
                'credit_total': 0,
                'transaction_count': 0,
                'users': set(),
                'document_count': set(),
                'posting_dates': set()
            })
            
            net_amount = pc_stats['debit_total'] - pc_stats['credit_total']
            
            pc_data = {
                'id': str(pc.id),
                'profit_center_code': pc.profit_center_code,
                'profit_center_name': pc.profit_center_name,
                'profit_center_short_text': pc.profit_center_short_text,
                'profit_center_type': pc.profit_center_type,
                'profit_center_group': pc.profit_center_group,
                'revenue_center': pc.revenue_center,
                'cost_center': pc.cost_center,
                'profit_center_currency': pc.profit_center_currency,
                'profit_center_status': pc.profit_center_status,
                'company_code': pc.company_code,
                'business_area': pc.business_area,
                'segment': pc.segment,
                'responsible_person': pc.responsible_person,
                'department': pc.department,
                'parent_profit_center': pc.parent_profit_center.profit_center_code if pc.parent_profit_center else None,
                'profit_center_level': pc.profit_center_level,
                'created_at': pc.created_at,
                'updated_at': pc.updated_at,
                
                # Statistics from GL postings for this account
                'debit_total': round(pc_stats['debit_total'], 2),
                'credit_total': round(pc_stats['credit_total'], 2),
                'net_amount': round(net_amount, 2),
                'transaction_count': pc_stats['transaction_count'],
                'user_count': len(pc_stats['users']),
                'users': list(pc_stats['users']),
                'document_count': len(pc_stats['document_count']),
                'posting_dates': list(pc_stats['posting_dates']),
                'has_activity': pc_stats['transaction_count'] > 0
            }
            
            profit_center_data.append(pc_data)
        
        # Apply sorting
        if sort_by in ['profit_center_code', 'profit_center_name', 'debit_total', 'credit_total', 'net_amount', 'transaction_count']:
            try:
                reverse = sort_order.lower() == 'desc'
                if sort_by in ['profit_center_code', 'profit_center_name']:
                    profit_center_data.sort(key=lambda x: x.get(sort_by, ''), reverse=reverse)
                else:
                    profit_center_data.sort(key=lambda x: float(x.get(sort_by, 0)), reverse=reverse)
            except (ValueError, TypeError):
                # If sorting fails, keep original order
                pass
        
        # Apply pagination
        paginator = ProfitCenterDataPagination()
        page = paginator.paginate_queryset(profit_center_data, request)
        
        if page is not None:
            response_data = {
                'account_id': account_id,
                'gl_account_info': gl_account_info,
                'total_profit_centers': len(profit_center_data),
                'filters_applied': {
                    'profit_center_code': profit_center_code_filter,
                    'profit_center_type': profit_center_type_filter,
                    'revenue_center_only': revenue_center_only,
                    'cost_center_only': cost_center_only,
                    'status': status_filter,
                    'sort_by': sort_by,
                    'sort_order': sort_order
                },
                'results': page
            }
            return paginator.get_paginated_response(response_data)
        
        # If no pagination, return all results
        return Response({
            'account_id': account_id,
            'gl_account_info': gl_account_info,
            'total_profit_centers': len(profit_center_data),
            'filters_applied': {
                'profit_center_code': profit_center_code_filter,
                'profit_center_type': profit_center_type_filter,
                'revenue_center_only': revenue_center_only,
                'cost_center_only': cost_center_only,
                'status': status_filter,
                'sort_by': sort_by,
                'sort_order': sort_order
            },
            'results': profit_center_data
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"Error retrieving profit center data for account {account_id}: {e}")
        return Response({
            'error': 'Internal server error',
            'details': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ============================================================================
# VERSION MANAGEMENT API VIEWS
# ============================================================================

@api_view(['GET'])
@permission_classes([AllowAny])
def get_engagement_versions(request, engagement_id):
    """
    Get all versions for a specific engagement
    
    GET /api/engagement/{engagement_id}/versions/
    """
    try:
        # Get the engagement
        engagement = Engagement.objects.get(engagement_id=engagement_id)
        
        # Get all data files for this engagement grouped by version
        data_files = DataFile.objects.filter(engagement=engagement).order_by('-version', '-uploaded_at')
        
        # Group files by version
        versions = {}
        for file in data_files:
            version = file.version
            if version not in versions:
                versions[version] = {
                    'version': version,
                    'version_notes': file.version_notes,
                    'uploaded_at': file.uploaded_at,
                    'files': []
                }
            versions[version]['files'].append({
                'id': str(file.id),
                'file_name': file.file_name,
                'file_type': file.file_type,
                'status': file.status,
                'total_records': file.total_records,
                'processed_records': file.processed_records,
                'failed_records': file.failed_records
            })
        
        # Get completeness tests for this engagement
        completeness_tests = CompletenessTestResult.objects.filter(engagement=engagement).order_by('-test_timestamp')
        
        return Response({
            'engagement_id': engagement_id,
            'engagement_name': engagement.engagement_name,
            'total_versions': len(versions),
            'versions': list(versions.values()),
            'completeness_tests': [
                {
                    'id': str(test.id),
                    'data_version': test.data_version,
                    'version_notes': test.version_notes,
                    'test_timestamp': test.test_timestamp,
                    'overall_status': test.overall_status,
                    'completeness_score': test.completeness_score
                }
                for test in completeness_tests
            ]
        }, status=status.HTTP_200_OK)
        
    except Engagement.DoesNotExist:
        return Response({
            'engagement_id': engagement_id,
            'message': f'Engagement {engagement_id} not found'
        }, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting engagement versions: {e}")
        return Response({
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_files_by_version(request, engagement_id, version):
    """
    Get all files for a specific engagement version
    
    GET /api/engagement/{engagement_id}/version/{version}/files/
    """
    try:
        # Get the engagement
        engagement = Engagement.objects.get(engagement_id=engagement_id)
        
        # Get all data files for this engagement and version
        data_files = DataFile.objects.filter(
            engagement=engagement,
            version=version
        ).order_by('file_type', 'uploaded_at')
        
        if not data_files.exists():
            return Response({
                'engagement_id': engagement_id,
                'version': version,
                'message': f'No files found for version {version}'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get completeness test for this version
        completeness_test = CompletenessTestResult.objects.filter(
            engagement=engagement,
            data_version=version
        ).order_by('-test_timestamp').first()
        
        files_data = []
        for file in data_files:
            files_data.append({
                'id': str(file.id),
                'file_name': file.file_name,
                'file_type': file.file_type,
                'file_size': file.file_size,
                'status': file.status,
                'total_records': file.total_records,
                'processed_records': file.processed_records,
                'failed_records': file.failed_records,
                'uploaded_at': file.uploaded_at,
                'processed_at': file.processed_at,
                'error_message': file.error_message
            })
        
        return Response({
            'engagement_id': engagement_id,
            'version': version,
            'version_notes': data_files.first().version_notes,
            'total_files': len(files_data),
            'files': files_data,
            'completeness_test': {
                'id': str(completeness_test.id),
                'test_timestamp': completeness_test.test_timestamp,
                'overall_status': completeness_test.overall_status,
                'completeness_score': completeness_test.completeness_score,
                'version_notes': completeness_test.version_notes
            } if completeness_test else None
        }, status=status.HTTP_200_OK)
        
    except Engagement.DoesNotExist:
        return Response({
            'engagement_id': engagement_id,
            'message': f'Engagement {engagement_id} not found'
        }, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting files by version: {e}")
        return Response({
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
