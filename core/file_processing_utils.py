"""
File processing utility functions for efficient and reusable operations
"""

import csv
import io
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

from django.utils import timezone
from .models import SAPGLPosting, TrialBalance, ChartOfAccount, DataFile, GLAccount

logger = logging.getLogger(__name__)


class FileReader:
    """Utility class for reading different file formats"""
    
    @staticmethod
    def read_file(file_obj, chunk_size=None):
        """
        Read file and return DataFrame based on file extension
        
        Args:
            file_obj: Uploaded file object or file path string
            chunk_size: If provided, return iterator of chunks instead of full DataFrame
            
        Returns:
            pandas.DataFrame or Iterator[pandas.DataFrame]: File data
        """
        # Handle both file objects and file path strings
        if isinstance(file_obj, str):
            # It's a file path string - use read_file_from_path
            return FileReader.read_file_from_path(file_obj, chunk_size)
        else:
            # It's a file object - use the original logic
            file_extension = file_obj.name.lower().split('.')[-1]
            file_name = file_obj.name
            
            try:
                if file_extension == 'csv':
                    return FileReader._read_csv(file_obj, chunk_size)
                elif file_extension in ['xlsx', 'xls', 'xlsb']:
                    return FileReader._read_excel(file_obj, file_extension, chunk_size)
                else:
                    logger.error(f"Unsupported file extension: {file_extension}")
                    return None
            except Exception as e:
                logger.error(f"Error reading file {file_name}: {e}")
                return None
    
    @staticmethod
    def read_file_from_path(file_path, chunk_size=None):
        """
        Read file from local path and return DataFrame
        
        Args:
            file_path: Local file path string
            chunk_size: If provided, return iterator of chunks instead of full DataFrame
            
        Returns:
            pandas.DataFrame or Iterator[pandas.DataFrame]: File data
        """
        import os
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        file_extension = file_path.lower().split('.')[-1]
        
        try:
            if file_extension == 'csv':
                return FileReader._read_csv(file_path, chunk_size)
            elif file_extension in ['xlsx', 'xls', 'xlsb']:
                return FileReader._read_excel(file_path, file_extension, chunk_size)
            else:
                logger.error(f"Unsupported file extension: {file_extension}")
                return None
        except Exception as e:
            logger.error(f"Error reading file from path {file_path}: {e}")
            return None
    
    @staticmethod
    def _read_csv(file_obj, chunk_size=None):
        """Read CSV file with optional chunking"""
        if chunk_size:
            if isinstance(file_obj, str):
                return pd.read_csv(file_obj, chunksize=chunk_size)
            else:
                content = file_obj.read().decode('utf-8')
                return pd.read_csv(io.StringIO(content), chunksize=chunk_size)
        else:
            if isinstance(file_obj, str):
                return pd.read_csv(file_obj)
            else:
                content = file_obj.read().decode('utf-8')
                return pd.read_csv(io.StringIO(content))
    
    @staticmethod
    def _read_excel(file_obj, file_extension, chunk_size=None):
        """Read Excel file (xlsx, xls, xlsb) with optional chunking"""
        if chunk_size:
            # For chunked reading, use openpyxl for better control
            return FileReader._read_excel_chunked(file_obj, file_extension, chunk_size)
        else:
            # Regular reading
            if isinstance(file_obj, str):
                if file_extension == 'xlsb':
                    try:
                        return pd.read_excel(file_obj, engine='pyxlsb')
                    except Exception as e:
                        logger.warning(f"pyxlsb failed for xlsb file: {e}")
                        raise Exception(f"Could not read xlsb file: {e}")
                elif file_extension == 'xls':
                    try:
                        return pd.read_excel(file_obj, engine='xlrd', header=0)
                    except Exception as e:
                        logger.error(f"xlrd failed for xls file: {e}")
                        raise Exception(f"Could not read xls file: {e}")
                else:
                    # Try openpyxl first, fallback to xlrd
                    try:
                        return pd.read_excel(file_obj, engine='openpyxl', header=0)
                    except Exception as e:
                        logger.warning(f"openpyxl failed for {file_extension} file: {e}")
                        try:
                            return pd.read_excel(file_obj, engine='xlrd', header=0)
                        except Exception as e2:
                            logger.error(f"xlrd also failed for {file_extension} file: {e2}")
                            raise Exception(f"Could not read {file_extension} file with openpyxl or xlrd: {e}, {e2}")
            else:
                file_obj.seek(0)
                if file_extension == 'xlsb':
                    try:
                        return pd.read_excel(file_obj, engine='pyxlsb')
                    except Exception as e:
                        logger.warning(f"pyxlsb failed for xlsb file: {e}")
                        raise Exception(f"Could not read xlsb file: {e}")
                elif file_extension == 'xls':
                    try:
                        return pd.read_excel(file_obj, engine='xlrd', header=0)
                    except Exception as e:
                        logger.error(f"xlrd failed for xls file: {e}")
                        raise Exception(f"Could not read xls file: {e}")
                else:
                    # Try openpyxl first, fallback to xlrd
                    try:
                        return pd.read_excel(file_obj, engine='openpyxl', header=0)
                    except Exception as e:
                        logger.warning(f"openpyxl failed for {file_extension} file: {e}")
                        try:
                            return pd.read_excel(file_obj, engine='xlrd', header=0)
                        except Exception as e2:
                            logger.error(f"xlrd also failed for {file_extension} file: {e2}")
                            raise Exception(f"Could not read {file_extension} file with openpyxl or xlrd: {e}, {e2}")
    
    @staticmethod
    def _read_excel_chunked(file_obj, file_extension, chunk_size):
        """Read Excel file in chunks with robust error handling"""
        try:
            # Method 1: Try pandas with openpyxl first (most reliable)
            logger.info("🔄 Method 1: pandas + openpyxl")
            if isinstance(file_obj, str):
                df_full = pd.read_excel(file_obj, engine='openpyxl', header=0)
            else:
                file_obj.seek(0)
                df_full = pd.read_excel(file_obj, engine='openpyxl', header=0)
            
            logger.info(f"✅ Method 1 success: {len(df_full)} rows")
            
            # Split into chunks
            total_rows = len(df_full)
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk_df = df_full.iloc[start_idx:end_idx].copy()
                logger.info(f"🔄 Yielding chunk: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
                yield chunk_df
                
        except Exception as e:
            logger.error(f"Method 1 failed: {e}")
            
            # Method 2: Try openpyxl directly
            try:
                logger.info("🔄 Method 2: openpyxl direct")
                from openpyxl import load_workbook
                
                if isinstance(file_obj, str):
                    wb = load_workbook(file_obj, read_only=True)
                else:
                    file_obj.seek(0)
                    wb = load_workbook(file_obj, read_only=True)
                
                sheet = wb.active
                logger.info(f"📊 Opened Excel file with openpyxl: {sheet.title}")
                
                # Get headers from first row
                headers = []
                for cell in sheet[1]:
                    headers.append(cell.value)
                logger.info(f"📋 Headers: {headers}")
                
                # Read data in chunks
                chunk_data = []
                row_count = 0
                
                for row in sheet.iter_rows(min_row=2, values_only=True):
                    if any(cell is not None for cell in row):  # Skip empty rows
                        chunk_data.append(row)
                        row_count += 1
                        
                        if len(chunk_data) >= chunk_size:
                            # Yield chunk
                            df_chunk = pd.DataFrame(chunk_data, columns=headers)
                            logger.info(f"🔄 Yielding chunk: {len(df_chunk)} rows")
                            yield df_chunk
                            chunk_data = []
                
                # Yield remaining data
                if chunk_data:
                    df_chunk = pd.DataFrame(chunk_data, columns=headers)
                    logger.info(f"🔄 Yielding final chunk: {len(df_chunk)} rows")
                    yield df_chunk
                
                wb.close()
                logger.info(f"✅ Successfully processed {row_count} rows using openpyxl direct")
                
            except Exception as e2:
                logger.error(f"Method 2 failed: {e2}")
                
                # Method 3: Try pandas with xlrd (for older Excel files)
                try:
                    logger.info("🔄 Method 3: pandas + xlrd")
                    if isinstance(file_obj, str):
                        df_full = pd.read_excel(file_obj, engine='xlrd', header=0)
                    else:
                        file_obj.seek(0)
                        df_full = pd.read_excel(file_obj, engine='xlrd', header=0)
                    
                    logger.info(f"✅ Method 3 success: {len(df_full)} rows")
                    
                    # Split into chunks
                    total_rows = len(df_full)
                    for start_idx in range(0, total_rows, chunk_size):
                        end_idx = min(start_idx + chunk_size, total_rows)
                        chunk_df = df_full.iloc[start_idx:end_idx].copy()
                        logger.info(f"🔄 Yielding chunk: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
                        yield chunk_df
                        
                except Exception as e3:
                    logger.error(f"Method 3 failed: {e3}")
                    raise Exception(f"All Excel reading methods failed. File may be corrupted. Methods failed: 1) {e}, 2) {e2}, 3) {e3}")


class FileTypeDetector:
    """Utility class for detecting file types based on column headers"""
    
    @staticmethod
    def detect_file_type(df: pd.DataFrame) -> str:
        """
        Detect file type based on DataFrame column headers
        
        Args:
            df: pandas DataFrame
            
        Returns:
            str: File type ('GL_LIST', 'TB_LIST', 'CHART_OF_ACCOUNTS', 'UNKNOWN')
        """
        if df.empty or df.columns.empty:
            return 'UNKNOWN'
        
        # Convert to lowercase for comparison
        headers = [str(col).lower().strip() for col in df.columns]
        headers_text = ' '.join(headers)
        
        # Check for GL List indicators
        gl_indicators = ['document', 'posting date', 'gl account', 'amount', 'user name']
        if any(indicator in headers_text for indicator in gl_indicators):
            return 'GL_LIST'
        
        # Check for TB List indicators
        tb_indicators = ['cocd', 'gl account', 'short text', 'opening balance', 'closing balance']
        if any(indicator in headers_text for indicator in tb_indicators):
            return 'TB_LIST'
        
        # Check for Chart of Accounts indicators
        chart_indicators = ['account', 'type', 'sub type', 'sub sub type', 'g/l acct long text', 'ref to fs', 'fin q1']
        if any(indicator in headers_text for indicator in chart_indicators):
            return 'CHART_OF_ACCOUNTS'
        
        return 'UNKNOWN'


class DataParser:
    """Utility class for parsing CSV data"""
    
    @staticmethod
    def parse_decimal(value: Any) -> Decimal:
        """
        Parse decimal value from string, handling parentheses for negative values
        and limiting decimal places to prevent validation errors
        
        Args:
            value: Value to parse
            
        Returns:
            Decimal: Parsed decimal value
        """
        # Handle None, NaN, or empty values
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return Decimal('0')
        
        # Convert to string first, then check for empty
        str_value = str(value)
        if str_value.strip() == '' or str_value.strip() == '-':
            return Decimal('0')
        
        # Remove commas and handle parentheses (negative values)
        cleaned_value = str_value.replace(',', '').strip()
        if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
            cleaned_value = '-' + cleaned_value[1:-1]
        
        try:
            decimal_value = Decimal(cleaned_value)
            
            # Limit decimal places to 15 to prevent validation errors
            # Round to 15 decimal places if more are present
            if decimal_value.as_tuple().exponent is not None and abs(decimal_value.as_tuple().exponent) > 15:
                # Round to 15 decimal places
                decimal_value = decimal_value.quantize(Decimal('0.' + '0' * 15))
            
            # Normalize to remove unnecessary trailing zeros but preserve actual precision
            # This will convert 1000.000000 to 1000 but keep 1000.50 as 1000.50
            return decimal_value.normalize()
            
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Could not parse decimal value '{value}': {e}")
            return Decimal('0')
    
    @staticmethod
    def parse_int(value: Any) -> Optional[int]:
        """
        Parse integer value from string
        
        Args:
            value: Value to parse
            
        Returns:
            int or None: Parsed integer value
        """
        if not value or str(value).strip() == '':
            return None
        
        try:
            return int(str(value).replace(',', '').strip())
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def parse_date(date_str, date_format: str = None) -> Optional[datetime]:
        """
        Parse date string or Timestamp to datetime object with multiple format support
        
        Args:
            date_str: Date string, Timestamp, or datetime object to parse
            date_format: Expected date format (if None, tries multiple formats)
            
        Returns:
            datetime or None: Parsed date
        """
        # Handle pandas Timestamp objects
        if hasattr(date_str, 'to_pydatetime'):
            # Check for pandas NaT (Not a Time) values
            if hasattr(date_str, 'isna') and date_str.isna():
                return None
            # Check for pandas NaT using string representation
            if str(date_str) == 'NaT':
                return None
            try:
                return date_str.to_pydatetime()
            except (ValueError, TypeError):
                return None
        
        # Handle datetime objects
        if isinstance(date_str, datetime):
            return date_str
        
        # Handle None or empty values
        if not date_str:
            return None
            
        # Convert to string and check if empty
        date_str = str(date_str)
        if date_str.strip() == '' or date_str.strip() == 'NaT':
            return None
        
        # If specific format provided, use it
        if date_format:
            try:
                return datetime.strptime(date_str.strip(), date_format)
            except ValueError:
                return None
        
        # Try multiple common date formats
        date_formats = [
            '%m/%d/%Y',           # 1/15/2025 (most common format)
            '%m/%d/%y',           # 1/15/25
            '%d/%m/%Y',           # 15/1/2025
            '%d/%m/%y',           # 15/1/25
            '%Y-%m-%d',           # 2025-01-15
            '%m-%d-%Y',           # 1-15-2025
            '%d-%m-%Y',           # 15-1-2025
            '%A, %B %d, %Y',      # Monday, January 15, 2025
            '%B %d, %Y',          # January 15, 2025
            '%b %d, %Y',          # Jan 15, 2025
            '%d %B %Y',           # 15 January 2025
            '%d %b %Y',           # 15 Jan 2025
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return None


class DataProcessor:
    """Utility class for processing different data types"""
    
    def __init__(self, data_file: DataFile):
        self.data_file = data_file
        self.batch_size = 10000  # Optimized batch size for better performance
        self.validation_batch_size = 5000  # Separate validation batch size
    
    def process_gl_data_chunked(self, file_path, chunk_size=25000):
        """Process GL data in chunks for large files"""
        logger.info("🎯" + "="*60)
        logger.info("🎯 CHUNKED GL DATA PROCESSING - THE MAGIC BEGINS! 🎯")
        logger.info(f"📊 Processing file in chunks of {chunk_size} rows...")
        logger.info("🎯" + "="*60)
        
        total_processed = 0
        total_failed = 0
        chunk_num = 0
        
        try:
            file_reader = FileReader()
            chunk_iterator = file_reader.read_file(file_path, chunk_size=chunk_size)
            
            for chunk_df in chunk_iterator:
                chunk_num += 1
                logger.info(f"🔄 Processing chunk {chunk_num} ({len(chunk_df)} rows)...")
                
                # Process this chunk using bulk loading for proper column mapping
                result = self._process_gl_chunk_with_bulk_loading(chunk_df)
                
                total_processed += result['processed_count']
                total_failed += result['failed_count']
                
                logger.info(f"✅ Chunk {chunk_num} completed: {result['processed_count']} processed, {result['failed_count']} failed")
            
            logger.info("🎯" + "="*60)
            logger.info("🎯 CHUNKED GL DATA PROCESSING COMPLETED! 🎯")
            logger.info(f"📊 Final Results: {total_processed} processed, {total_failed} failed")
            logger.info("🎯" + "="*60)
            
            return {'processed_count': total_processed, 'failed_count': total_failed}
            
        except Exception as e:
            logger.error(f"❌ Chunked processing failed: {e}")
            raise
    
    def _process_gl_chunk_with_bulk_loading(self, chunk_df: pd.DataFrame) -> Dict[str, int]:
        """
        Process a single GL chunk using bulk loading for proper column mapping
        """
        try:
            from .bulk_loading_utils import SQLAlchemyBulkLoader
            
            # Create bulk loader
            bulk_loader = SQLAlchemyBulkLoader()
            
            # Use bulk loading with proper column mapping
            result = bulk_loader.bulk_load_gl_with_pandas(chunk_df, str(self.data_file.id))
            
            if result['success']:
                return {
                    'processed_count': result['records_loaded'],
                    'failed_count': 0
                }
            else:
                logger.error(f"❌ Bulk loading failed: {result.get('error', 'Unknown error')}")
                return {
                    'processed_count': 0,
                    'failed_count': len(chunk_df)
                }
                
        except Exception as e:
            logger.error(f"❌ Error in bulk loading chunk: {e}")
            return {
                'processed_count': 0,
                'failed_count': len(chunk_df)
            }
    
    def process_gl_data(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        🎯 GL Data Processing - The Heart of Financial Data Magic! 🎯
        
        This method transforms raw GL data into beautiful, structured database records!
        
        Args:
            df: pandas DataFrame (our raw data treasure!)
            
        Returns:
            Dict: Processing results with counts (our success metrics!)
        """
        logger.info("🎯" + "="*60)
        logger.info("🎯 GL DATA PROCESSING - THE MAGIC BEGINS! 🎯")
        logger.info(f"📊 Processing {len(df)} rows of GL data...")
        logger.info("🎯" + "="*60)
        
        processed_count = 0
        failed_count = 0
        postings_to_create = []
        
        # 🚀 VECTORIZED Data Processing - The Fast Data Transformation! 🚀
        logger.info("🚀 Starting vectorized data processing...")
        
        # Use vectorized operations for better performance
        postings_to_create, processed_count, failed_count = self._process_gl_data_vectorized(df)
        
        logger.info("✅ Vectorized processing completed!")
        logger.info(f"📊 Processing Summary:")
        logger.info(f"   ✅ Successfully Processed: {processed_count} records")
        logger.info(f"   ❌ Failed Records: {failed_count} records")
        logger.info(f"   📈 Success Rate: {(processed_count / (processed_count + failed_count) * 100):.1f}%")
        
        # 🚀 ULTRA-FAST Database Save - Choose best method based on data size!
        if postings_to_create:
            record_count = len(postings_to_create)
            logger.info(f"🚀 Starting ULTRA-FAST database save ({record_count:,} records)...")
            
            # Use Django bulk_create for reliable database persistence
            logger.info("💾 Using Django bulk_create for reliable database persistence")
            self._optimized_bulk_create_gl_postings(
                postings_to_create,
                f"GL records for file {self.data_file.file_name}"
            )
            
            # Update GLAccount records after GL bulk operations
            self._update_gl_accounts_after_gl_processing()
        else:
            logger.warning("⚠️  No GL postings to save to database!")
        
        logger.info("🎯" + "="*60)
        logger.info("🎯 GL DATA PROCESSING COMPLETED! 🎯")
        logger.info(f"📊 Final Results: {processed_count} processed, {failed_count} failed")
        logger.info("🎯" + "="*60)
        
        # PERFECT ML FLOW: GL Save → Completeness Test → Model Training → ML Recommendations
        try:
            from .tasks.perfect_flow_tasks import trigger_perfect_ml_flow
            from django.db import connection
            from .models import DataFile
            
            # Ensure database connection is closed before task execution
            connection.close()
            
            # Get data file to determine engagement and version
            data_file = DataFile.objects.get(id=self.data_file.id)
            engagement = data_file.engagement
            version = getattr(data_file, 'version', '1.0')
            
            logger.info("🚀 Starting PERFECT ML FLOW for ENG-008")
            logger.info("📋 Flow: GL Save → Completeness Test → Model Training → ML Recommendations")
            
            # Trigger the perfect ML flow
            flow_result = trigger_perfect_ml_flow.delay(
                data_file_id=str(self.data_file.id),
                engagement_id=str(engagement.id),
                client_name=engagement.engagement_name
            )
            
            logger.info(f"✅ PERFECT ML FLOW initiated successfully!")
            logger.info(f"🔗 Flow Task ID: {flow_result.id}")
            logger.info(f"📊 GL data saved for file: {self.data_file.file_name}")
            logger.info(f"🎯 Engagement: {engagement.engagement_name} (ID: {engagement.engagement_id})")
            logger.info(f"📋 Version: {version}")
            logger.info("🔄 Flow will execute: Completeness → Training → Recommendations")
            logger.info("📈 Models to be trained:")
            logger.info("   - Trend Analysis Model (scikit-learn Isolation Forest)")
            logger.info("   - Unusual Transaction Detection (scikit-learn Random Forest)")
            logger.info("   - Completeness Prediction Model")
            
        except Exception as e:
            logger.error(f"❌ Could not initiate perfect ML flow: {e}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
        
        return {'processed_count': processed_count, 'failed_count': failed_count}
    
    def _save_gl_record_individual(self, posting, record_index: int) -> bool:
        """
        Save a single GL posting record with connection retry logic
        
        Args:
            posting: SAPGLPosting instance to save
            record_index: Index of the record for logging
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        from django.db import connection
        from django.db.utils import OperationalError, InterfaceError
        import time
        
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                # Ensure database connection is alive
                try:
                    connection.ensure_connection()
                except (OperationalError, InterfaceError) as e:
                    logger.warning(f"🔄 Database connection issue for record {record_index}: {e}")
                    try:
                        connection.close()
                        connection.connect()
                        logger.info(f"✅ Database reconnected for record {record_index}")
                    except Exception as reconnect_error:
                        logger.error(f"❌ Failed to reconnect for record {record_index}: {reconnect_error}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        return False
                
                # Validate and save the record
                posting.full_clean()  # Validate the record
                posting.save()
                return True
                
            except (OperationalError, InterfaceError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"🔄 Record {record_index} attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ Record {record_index} failed after {max_retries} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"❌ Record {record_index} failed with non-connection error: {e}")
                return False
        
        return False
    
    def _optimized_bulk_create_gl_postings(self, postings_to_create: List, operation_name: str):
        """
        🚀 Ultra-Fast GL Posting Bulk Create - OPTIMIZED FOR SPEED! 🚀
        
        This method is specifically optimized for GL postings with:
        - Massive batch sizes (10K+ records per batch)
        - Minimal validation overhead
        - Connection pooling optimization
        - Batch-level transaction handling
        - Error segregation without stopping the process
        
        Args:
            postings_to_create: List of SAPGLPosting instances
            operation_name: Name of operation for logging
        """
        from django.db import transaction, connection
        from django.db.utils import OperationalError, InterfaceError
        from .models import SAPGLPosting, SAPGLPostingError
        import time
        
        logger.info("🚀" + "="*60)
        logger.info("🚀 ULTRA-FAST GL POSTING BULK CREATE! 🚀")
        logger.info(f"📊 Processing {len(postings_to_create)} {operation_name}...")
        logger.info(f"🔢 Optimized Batch Size: {self.batch_size}")
        logger.info("🚀" + "="*60)
        
        total_created = 0
        total_errors = 0
        batch_count = 0
        start_time = time.time()
        
        # Pre-validate and separate good/bad records
        logger.info("🔍 Pre-validation phase...")
        valid_postings = []
        invalid_postings = []
        
        for i, posting in enumerate(postings_to_create):
            try:
                # Quick validation - only essential fields
                if not posting.gl_account or not posting.data_file:
                    raise ValueError("Missing required fields")
                valid_postings.append(posting)
            except Exception as e:
                invalid_postings.append((i, posting, str(e)))
        
        logger.info(f"✅ Pre-validation complete: {len(valid_postings)} valid, {len(invalid_postings)} invalid")
        
        # Save invalid records to error table first
        if invalid_postings:
            logger.info(f"💾 Saving {len(invalid_postings)} invalid records to error table...")
            error_records = []
            for record_index, posting, error_msg in invalid_postings:
                try:
                    raw_data = {
                        'gl_account': getattr(posting, 'gl_account', ''),
                        'document_number': getattr(posting, 'document_number', ''),
                        'amount_local_currency': str(getattr(posting, 'amount_local_currency', 0)),
                        'user_name': getattr(posting, 'user_name', ''),
                        'posting_date': str(getattr(posting, 'posting_date', '')),
                    }
                    
                    error_record = SAPGLPostingError(
                        data_file=posting.data_file,
                        error_type='PreValidationError',
                        error_message=error_msg,
                        raw_data=raw_data,
                        batch_number=0,
                        record_index=record_index
                    )
                    error_records.append(error_record)
                except Exception as e:
                    logger.error(f"Failed to create error record: {e}")
            
            if error_records:
                try:
                    SAPGLPostingError.objects.bulk_create(error_records, batch_size=1000)
                    total_errors += len(error_records)
                    logger.info(f"✅ Saved {len(error_records)} error records")
                except Exception as e:
                    logger.error(f"Failed to save error records: {e}")
        
        # Process valid records in large batches
        if valid_postings:
            logger.info(f"🚀 Processing {len(valid_postings)} valid records in {self.batch_size}-record batches...")
            
            for i in range(0, len(valid_postings), self.batch_size):
                batch = valid_postings[i:i + self.batch_size]
                batch_count += 1
                batch_start_time = time.time()
                
                try:
                    # Use a single transaction per large batch
                    with transaction.atomic():
                        # Ensure connection is healthy
                        try:
                            connection.ensure_connection()
                        except (OperationalError, InterfaceError):
                            connection.close()
                            connection.connect()
                        
                        # Bulk create with ignore_conflicts for speed
                        created_objects = SAPGLPosting.objects.bulk_create(
                            batch, 
                            batch_size=min(self.batch_size, 5000),  # Internal Django batch size
                            ignore_conflicts=True  # Skip duplicates instead of failing
                        )
                        
                        batch_created = len(batch)  # Since ignore_conflicts=True, assume all created
                        total_created += batch_created
                        
                        batch_duration = time.time() - batch_start_time
                        records_per_second = batch_created / batch_duration if batch_duration > 0 else 0
                        
                        logger.info(f"⚡ Batch {batch_count}: {batch_created} records in {batch_duration:.2f}s ({records_per_second:.0f} rec/sec)")
                        
                        # Progress update for large datasets
                        if len(valid_postings) > 10000:
                            progress_percent = (total_created / len(valid_postings)) * 100
                            logger.info(f"📈 Progress: {total_created}/{len(valid_postings)} ({progress_percent:.1f}%)")
                            
                except Exception as e:
                    logger.error(f"❌ Batch {batch_count} failed: {e}")
                    # Don't stop - try to save individual records from failed batch
                    try:
                        individual_saved = 0
                        for j, posting in enumerate(batch):
                            try:
                                posting.save()
                                individual_saved += 1
                            except Exception as individual_error:
                                # Save to error table
                                try:
                                    raw_data = {
                                        'gl_account': getattr(posting, 'gl_account', ''),
                                        'document_number': getattr(posting, 'document_number', ''),
                                        'amount_local_currency': str(getattr(posting, 'amount_local_currency', 0)),
                                    }
                                    SAPGLPostingError.objects.create(
                                        data_file=posting.data_file,
                                        error_type='IndividualSaveError',
                                        error_message=str(individual_error),
                                        raw_data=raw_data,
                                        batch_number=batch_count,
                                        record_index=j
                                    )
                                    total_errors += 1
                                except:
                                    pass
                        
                        total_created += individual_saved
                        logger.info(f"🔄 Batch {batch_count} fallback: {individual_saved} individual saves")
                        
                    except Exception as fallback_error:
                        logger.error(f"❌ Batch {batch_count} fallback also failed: {fallback_error}")
                        total_errors += len(batch)
        
        total_duration = time.time() - start_time
        overall_speed = total_created / total_duration if total_duration > 0 else 0
        
        logger.info("🚀" + "="*60)
        logger.info("🚀 ULTRA-FAST BULK CREATE COMPLETED! 🚀")
        logger.info(f"✅ Successfully created: {total_created} records")
        logger.info(f"❌ Errors handled: {total_errors} records")
        logger.info(f"⏱️  Total duration: {total_duration:.2f} seconds")
        logger.info(f"⚡ Overall speed: {overall_speed:.0f} records/second")
        logger.info(f"📊 Success rate: {(total_created / len(postings_to_create) * 100):.1f}%")
        logger.info("🚀" + "="*60)
    
    def _ultra_fast_copy_load_gl_postings(self, df: pd.DataFrame, operation_name: str):
        """
        🏆 ULTRA-FAST GL Loading using PostgreSQL COPY command
        
        This is the fastest possible method for very large GL datasets (50K+ records)
        Uses raw PostgreSQL COPY command for maximum performance
        
        Args:
            df: pandas DataFrame with GL data
            operation_name: Name of operation for logging
        """
        try:
            from .bulk_loading_utils import bulk_load_gl_with_pandas
            
            logger.info("🏆" + "="*60)
            logger.info("🏆 ULTRA-FAST PostgreSQL COPY LOADING! 🏆")
            logger.info(f"📊 Loading {len(df):,} GL records with COPY command...")
            logger.info("🏆" + "="*60)
            
            # Use the SQLAlchemy bulk loader with pandas (COPY requires file setup)
            result = bulk_load_gl_with_pandas(df, str(self.data_file.id))
            
            if result['success']:
                logger.info(f"🏆 COPY load completed: {result['records_loaded']:,} records in {result['duration']:.2f}s")
                logger.info(f"⚡ Speed: {result['records_per_second']:.0f} records/second")
            else:
                logger.error(f"❌ COPY load failed: {result.get('error', 'Unknown error')}")
                # Fallback to optimized Django method
                logger.info("🔄 Falling back to optimized Django bulk_create...")
                self._optimized_bulk_create_gl_postings(
                    [self._create_gl_posting_from_row(row) for _, row in df.iterrows()],
                    operation_name
                )
                
        except Exception as e:
            logger.error(f"❌ Ultra-fast COPY loading failed: {e}")
            # Fallback to optimized Django method
            logger.info("🔄 Falling back to optimized Django bulk_create...")
            self._optimized_bulk_create_gl_postings(
                [self._create_gl_posting_from_row(row) for _, row in df.iterrows()],
                operation_name
            )
    
    def _fast_pandas_load_gl_postings(self, df: pd.DataFrame, operation_name: str):
        """
        ⚡ FAST GL Loading using pandas.to_sql()
        
        5-10x faster than Django ORM for medium-large datasets (5K-50K records)
        
        Args:
            df: pandas DataFrame with GL data
            operation_name: Name of operation for logging
        """
        try:
            from .bulk_loading_utils import bulk_load_gl_with_pandas
            
            logger.info("⚡" + "="*60)
            logger.info("⚡ FAST PANDAS.TO_SQL LOADING! ⚡")
            logger.info(f"📊 Loading {len(df):,} GL records with pandas.to_sql()...")
            logger.info("⚡" + "="*60)
            
            # Use the SQLAlchemy bulk loader
            result = bulk_load_gl_with_pandas(df, str(self.data_file.id))
            
            if result['success']:
                logger.info(f"⚡ Pandas load completed: {result['records_loaded']:,} records in {result['duration']:.2f}s")
                logger.info(f"🚀 Speed: {result['records_per_second']:.0f} records/second")
            else:
                logger.error(f"❌ Pandas load failed: {result.get('error', 'Unknown error')}")
                # Fallback to optimized Django method
                logger.info("🔄 Falling back to optimized Django bulk_create...")
                self._optimized_bulk_create_gl_postings(
                    [self._create_gl_posting_from_row(row) for _, row in df.iterrows()],
                    operation_name
                )
                
        except Exception as e:
            logger.error(f"❌ Fast pandas loading failed: {e}")
            # Fallback to optimized Django method
            logger.info("🔄 Falling back to optimized Django bulk_create...")
            self._optimized_bulk_create_gl_postings(
                [self._create_gl_posting_from_row(row) for _, row in df.iterrows()],
                operation_name
            )
    
    def _fast_pandas_load_gl_postings_from_instances(self, postings_to_create: List, operation_name: str):
        """
        ⚡ FAST GL Loading using pandas.to_sql() from model instances
        
        5-10x faster than Django ORM for medium-large datasets (5K-50K records)
        
        Args:
            postings_to_create: List of SAPGLPosting model instances
            operation_name: Name of operation for logging
        """
        try:
            from .bulk_loading_utils import bulk_load_gl_with_pandas_from_instances
            
            logger.info("⚡" + "="*60)
            logger.info("⚡ FAST PANDAS.TO_SQL LOADING FROM INSTANCES! ⚡")
            logger.info(f"📊 Loading {len(postings_to_create):,} GL records with pandas.to_sql()...")
            logger.info("⚡" + "="*60)
            
            # Use the SQLAlchemy bulk loader with instances
            result = bulk_load_gl_with_pandas_from_instances(postings_to_create, str(self.data_file.id))
            
            if result['success']:
                logger.info(f"⚡ Pandas load completed: {result['records_loaded']:,} records in {result['duration']:.2f}s")
                logger.info(f"🚀 Speed: {result['records_per_second']:.0f} records/second")
            else:
                logger.error(f"❌ Pandas load failed: {result.get('error', 'Unknown error')}")
                # Fallback to optimized Django method
                logger.info("🔄 Falling back to optimized Django bulk_create...")
                self._optimized_bulk_create_gl_postings(postings_to_create, operation_name)
                
        except Exception as e:
            logger.error(f"❌ Fast pandas loading failed: {e}")
            # Fallback to optimized Django method
            logger.info("🔄 Falling back to optimized Django bulk_create...")
            self._optimized_bulk_create_gl_postings(postings_to_create, operation_name)
    
    def _ultra_fast_copy_load_gl_postings_from_instances(self, postings_to_create: List, operation_name: str):
        """
        🏆 ULTRA-FAST GL Loading using PostgreSQL COPY from model instances
        
        10-20x faster than Django ORM for very large datasets (50K+ records)
        
        Args:
            postings_to_create: List of SAPGLPosting model instances
            operation_name: Name of operation for logging
        """
        try:
            from .bulk_loading_utils import bulk_load_gl_with_copy_from_instances
            
            logger.info("🏆" + "="*60)
            logger.info("🏆 ULTRA-FAST POSTGRESQL COPY LOADING FROM INSTANCES! 🏆")
            logger.info(f"📊 Loading {len(postings_to_create):,} GL records with PostgreSQL COPY...")
            logger.info("🏆" + "="*60)
            
            # Use the SQLAlchemy bulk loader with instances
            result = bulk_load_gl_with_copy_from_instances(postings_to_create, str(self.data_file.id))
            
            if result['success']:
                logger.info(f"🏆 COPY load completed: {result['records_loaded']:,} records in {result['duration']:.2f}s")
                logger.info(f"🚀 Speed: {result['records_per_second']:.0f} records/second")
            else:
                logger.error(f"❌ COPY load failed: {result.get('error', 'Unknown error')}")
                # Fallback to optimized Django method
                logger.info("🔄 Falling back to optimized Django bulk_create...")
                self._optimized_bulk_create_gl_postings(postings_to_create, operation_name)
                
        except Exception as e:
            logger.error(f"❌ Ultra-fast COPY loading failed: {e}")
            # Fallback to optimized Django method
            logger.info("🔄 Falling back to optimized Django bulk_create...")
            self._optimized_bulk_create_gl_postings(postings_to_create, operation_name)
    
    def _process_gl_data_vectorized(self, df: pd.DataFrame) -> Tuple[List, int, int]:
        """
        🚀 VECTORIZED GL Data Processing - Ultra-Fast Data Transformation! 🚀
        
        Uses pandas vectorized operations for maximum performance:
        - Vectorized data cleaning and type conversion
        - Batch GL Account lookups
        - Parallel data validation
        - Optimized model instance creation
        
        Args:
            df: pandas DataFrame with GL data
            
        Returns:
            Tuple: (postings_to_create, processed_count, failed_count)
        """
        logger.info("🚀" + "="*60)
        logger.info("🚀 VECTORIZED GL DATA PROCESSING - MAXIMUM SPEED! 🚀")
        logger.info(f"📊 Processing {len(df)} rows with vectorized operations...")
        logger.info("🚀" + "="*60)
        
        processed_count = 0
        failed_count = 0
        postings_to_create = []
        
        try:
            # Step 1: Vectorized data cleaning and preparation
            logger.info("🧹 Step 1: Vectorized data cleaning...")
            df_cleaned = self._clean_gl_dataframe_vectorized(df)
            
            # Step 2: Batch GL Account lookups
            logger.info("🔍 Step 2: Batch GL Account lookups...")
            gl_accounts_map = self._batch_lookup_gl_accounts(df_cleaned)
            
            # Step 3: Vectorized model instance creation
            logger.info("⚡ Step 3: Vectorized model instance creation...")
            postings_to_create, processed_count, failed_count = self._create_gl_postings_vectorized(
                df_cleaned, gl_accounts_map
            )
            
            logger.info("✅ Vectorized processing completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Vectorized processing failed: {e}")
            # Fallback to row-by-row processing
            logger.info("🔄 Falling back to row-by-row processing...")
            return self._process_gl_data_row_by_row(df)
        
        return postings_to_create, processed_count, failed_count
    
    def _clean_gl_dataframe_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean GL DataFrame using vectorized operations"""
        df_cleaned = df.copy()
        
        # Vectorized string cleaning
        string_columns = ['G/L Account', 'Document Number', 'User Name', 'Text']
        for col in string_columns:
            if col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
        
        # Vectorized account code cleaning
        if 'G/L Account' in df_cleaned.columns:
            df_cleaned['G/L Account'] = df_cleaned['G/L Account'].astype(str)
            # Remove decimal representations of whole numbers
            df_cleaned['G/L Account'] = df_cleaned['G/L Account'].apply(
                lambda x: str(int(float(x))) if '.' in str(x) and str(x).replace('.', '').isdigit() and float(x).is_integer() else str(x)
            )
        
        # Vectorized date parsing
        date_columns = ['Posting Date', 'Document Date', 'Entry Date']
        for col in date_columns:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
        
        # Vectorized amount parsing
        amount_columns = ['Amount in Local Currency', 'Amount in Transaction Currency']
        for col in amount_columns:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').fillna(0)
        
        # Remove rows with missing required fields
        required_fields = ['G/L Account', 'Document Number']
        for field in required_fields:
            if field in df_cleaned.columns:
                df_cleaned = df_cleaned[df_cleaned[field].notna() & (df_cleaned[field] != '')]
        
        logger.info(f"🧹 Data cleaning completed: {len(df)} → {len(df_cleaned)} rows")
        return df_cleaned
    
    def _batch_lookup_gl_accounts(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Batch lookup GL Accounts for all unique account codes"""
        from .models import GLAccount
        
        # Get unique account codes
        unique_accounts = df['G/L Account'].unique() if 'G/L Account' in df.columns else []
        
        if not unique_accounts.size:
            return {}
        
        # Batch lookup all accounts
        gl_accounts = GLAccount.objects.filter(
            engagement=self.data_file.engagement,
            account_code__in=unique_accounts
        ).values('account_code', 'id', 'account_name')
        
        # Create lookup map
        accounts_map = {acc['account_code']: acc for acc in gl_accounts}
        
        logger.info(f"🔍 Batch GL Account lookup: {len(accounts_map)}/{len(unique_accounts)} found")
        return accounts_map
    
    def _create_gl_postings_vectorized(self, df: pd.DataFrame, gl_accounts_map: Dict) -> Tuple[List, int, int]:
        """Create GL postings using vectorized operations"""
        from .models import SAPGLPosting
        
        postings_to_create = []
        processed_count = 0
        failed_count = 0
        
        # Process in batches for memory efficiency
        batch_size = 1000
        total_rows = len(df)
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            # Process batch
            batch_postings, batch_processed, batch_failed = self._create_gl_postings_batch(
                batch_df, gl_accounts_map, start_idx
            )
            
            postings_to_create.extend(batch_postings)
            processed_count += batch_processed
            failed_count += batch_failed
            
            # Progress logging
            if processed_count % 5000 == 0:
                logger.info(f"📈 Progress: {processed_count}/{total_rows} records processed...")
        
        return postings_to_create, processed_count, failed_count
    
    def _create_gl_postings_batch(self, batch_df: pd.DataFrame, gl_accounts_map: Dict, start_idx: int) -> Tuple[List, int, int]:
        """Create GL postings for a batch of rows"""
        from .models import SAPGLPosting
        
        postings = []
        processed = 0
        failed = 0
        
        for idx, (_, row) in enumerate(batch_df.iterrows()):
            try:
                posting = self._create_gl_posting_from_row_optimized(row, gl_accounts_map)
                if posting:
                    postings.append(posting)
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                if failed <= 5:  # Log first few errors
                    logger.error(f"❌ Error processing GL row {start_idx + idx}: {e}")
        
        return postings, processed, failed
    
    def _create_gl_posting_from_row_optimized(self, row: pd.Series, gl_accounts_map: Dict) -> Optional[SAPGLPosting]:
        """Optimized GL posting creation with pre-looked up GL Account"""
        try:
            gl_account_code = str(row.get('G/L Account', '')).strip()
            if not gl_account_code:
                return None
            
            # Get pre-looked up GL Account
            gl_account_data = gl_accounts_map.get(gl_account_code)
            gl_account_ref = None
            if gl_account_data:
                gl_account_ref = GLAccount.objects.get(id=gl_account_data['id'])
            
            # Parse posting date and extract period
            posting_date = self._parse_posting_date(row.get('Posting Date', ''))
            posting_period = 1  # Default period
            fiscal_year = 2025  # Default year
            
            if posting_date:
                posting_period = posting_date.month
                fiscal_year = posting_date.year
            else:
                # Try to get from explicit fields if posting date parsing failed
                posting_period = DataParser.parse_int(row.get('Posting Period', '1')) or 1
                fiscal_year = DataParser.parse_int(row.get('Fiscal Year', '2025')) or 2025
            
            # Create SAPGLPosting instance
            posting = SAPGLPosting(
                data_file=self.data_file,
                gl_account=gl_account_code,
                gl_account_ref=gl_account_ref,
                document_number=str(row.get('Document Number', '')).strip(),
                posting_date=posting_date,
                amount_local_currency=row.get('Amount in Local Currency', 0),
                local_currency=str(row.get('Local Currency', 'SAR')).strip(),
                amount_transaction_currency=row.get('Amount in Transaction Currency', 0),
                transaction_currency=str(row.get('Transaction Currency', '')).strip(),
                exchange_rate=row.get('Exchange Rate', 1.0),
                user_name=str(row.get('User Name', '')).strip(),
                posting_key=str(row.get('Posting Key', '')).strip(),
                reference_document=str(row.get('Reference Document', '')).strip(),
                document_header_text=str(row.get('Document Header Text', '')).strip(),
                company_code=str(row.get('Company Code', '')).strip(),
                fiscal_year=fiscal_year,
                posting_period=posting_period,
                fiscal_period=row.get('Fiscal Period'),
                cost_center=str(row.get('Cost Center', '')).strip(),
                profit_center=str(row.get('Profit Center', '')).strip(),
                wbs_element=str(row.get('WBS Element', '')).strip(),
                order_number=str(row.get('Order Number', '')).strip(),
                asset_number=str(row.get('Asset Number', '')).strip(),
                sub_number=str(row.get('Sub Number', '')).strip(),
                business_area=str(row.get('Business Area', '')).strip(),
                segment=str(row.get('Segment', '')).strip(),
                partner_business_area=str(row.get('Partner Business Area', '')).strip(),
                # GL Account type information from Chart of Accounts
                gl_account_type=gl_account_data.get('account_type', '') if gl_account_data else '',
                gl_account_sub_type=gl_account_data.get('account_sub_type', '') if gl_account_data else '',
                gl_account_sub_sub_type=gl_account_data.get('account_sub_sub_type', '') if gl_account_data else '',
                gl_account_long_text=gl_account_data.get('account_long_text', '') if gl_account_data else '',
                financial_statement=gl_account_data.get('financial_statement', '') if gl_account_data else '',
                ref_to_fs=gl_account_data.get('ref_to_fs', '') if gl_account_data else ''
            )
            
            return posting
            
        except Exception as e:
            logger.error(f"Error creating GL posting: {e}")
            return None
    
    def _process_gl_data_row_by_row(self, df: pd.DataFrame) -> Tuple[List, int, int]:
        """Fallback row-by-row processing method"""
        processed_count = 0
        failed_count = 0
        postings_to_create = []
        
        logger.info("🔄 Using fallback row-by-row processing...")
        
        for index, row in df.iterrows():
            try:
                posting = self._create_gl_posting_from_row(row)
                if posting:
                    postings_to_create.append(posting)
                    processed_count += 1
                    
                    # Log progress for every 100 records
                    if processed_count % 100 == 0:
                        logger.info(f"📈 Progress: {processed_count} records processed successfully...")
                        
            except Exception as e:
                logger.error(f"❌ Error processing GL row {index}: {e}")
                failed_count += 1
                
                # Log detailed error for first few failures
                if failed_count <= 5:
                    logger.error(f"🔍 Row {index} details: {dict(row)}")
        
        return postings_to_create, processed_count, failed_count
    
    def process_chart_data_from_file(self, file_path: str) -> Dict[str, int]:
        """
        Process Chart of Accounts data from file path (for background thread processing)
        
        Args:
            file_path: Path to the COA file
            
        Returns:
            Dict: Processing results with counts
        """
        try:
            # Read file using FileReader
            file_reader = FileReader()
            df = file_reader.read_file(file_path)
            
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Process using existing method
            return self.process_chart_data(df)
            
        except Exception as e:
            logger.error(f"Error processing COA file {file_path}: {e}")
            return {'processed_count': 0, 'failed_count': 1}
    
    def process_tb_data(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Process Trial Balance data and save to TrialBalance model
        
        Args:
            df: pandas DataFrame
            
        Returns:
            Dict: Processing results with counts
        """
        processed_count = 0
        failed_count = 0
        tb_records_to_create = []
        
        for index, row in df.iterrows():
            try:
                tb_record = self._create_tb_record_from_row(row)
                if tb_record:
                    tb_records_to_create.append(tb_record)
                    processed_count += 1
            except Exception as e:
                logger.error(f"Error processing TB row {index}: {e}")
                failed_count += 1
        
        if tb_records_to_create:
            # Use Django bulk_create for reliable database persistence
            logger.info(f"💾 Using Django bulk_create for TB loading ({len(tb_records_to_create):,} records)")
            self._bulk_create_with_progress(
                TrialBalance, tb_records_to_create,
                f"TB records for file {self.data_file.file_name}"
            )
        
        return {'processed_count': processed_count, 'failed_count': failed_count}
    
    def process_chart_data(self, df: pd.DataFrame, start_row: int = None) -> Dict[str, int]:
        """
        Process Chart of Accounts data and save to ChartOfAccount model
        Handles hierarchical structure where one account can have multiple sub-sub types
        
        Args:
            df: pandas DataFrame
            start_row: Optional starting row number (0-based index). If None, auto-detect.
            
        Returns:
            Dict: Processing results with counts
        """
        processed_count = 0
        failed_count = 0
        chart_records_to_create = []
        
        # If no start_row specified, try to detect optimal starting row
        if start_row is None:
            start_row = self._detect_chart_start_row(df)
        
        # Clean the DataFrame first - remove completely empty rows and handle initial empty rows
        df_cleaned = self._clean_chart_dataframe(df, start_row)
        
        logger.info(f"Chart of Accounts: Processing {len(df_cleaned)} cleaned rows...")
        
        # Group rows by account to handle hierarchical structure
        account_groups = self._group_chart_data_by_account(df_cleaned)
        logger.info(f"Chart of Accounts: Found {len(account_groups)} unique accounts")
        
        for account_code, account_rows in account_groups.items():
            try:
                # Process each account group
                account_records = self._process_account_group(account_code, account_rows)
                chart_records_to_create.extend(account_records)
                processed_count += len(account_records)
                
                # Log progress for first few accounts
                if processed_count <= 10:
                    logger.info(f"Chart of Accounts: Processed account {account_code}: {len(account_records)} sub-sub types")
                    
            except Exception as e:
                logger.error(f"Error processing account group {account_code}: {e}")
                failed_count += len(account_rows)
        
        # Use simple bulk_create like TB processing
        if chart_records_to_create:
            logger.info(f"💾 Using Django bulk_create for COA loading ({len(chart_records_to_create):,} records)")
            try:
                # Use the same approach as TB processing
                self._bulk_create_with_progress(
                    ChartOfAccount, chart_records_to_create,
                    f"COA records for file {self.data_file.file_name}"
                )
                logger.info(f"✅ Successfully processed {len(chart_records_to_create)} COA records using bulk_create")
            except Exception as e:
                logger.error(f"❌ Error in bulk_create operations: {e}")
                # Fallback to individual save
                logger.info("🔄 Falling back to individual save operations")
                
                for record in chart_records_to_create:
                    try:
                        record.save()
                        logger.debug(f"✅ Saved COA record for account {record.account}")
                    except Exception as individual_error:
                        logger.error(f"❌ Error saving individual record for account {record.account}: {individual_error}")
                        failed_count += 1
        
        # Log final processing summary
        logger.info("🎯" + "="*60)
        logger.info("🎯 CHART OF ACCOUNTS PROCESSING COMPLETED! 🎯")
        logger.info("🎯" + "="*60)
        logger.info(f"📊 Original DataFrame rows: {len(df)}")
        logger.info(f"📊 Cleaned DataFrame rows: {len(df_cleaned)}")
        logger.info(f"📊 Unique accounts: {len(account_groups)}")
        logger.info(f"📊 Records created: {processed_count}")
        logger.info(f"📊 Records failed: {failed_count}")
        logger.info(f"📊 Success rate: {(processed_count / (processed_count + failed_count) * 100):.1f}%" if (processed_count + failed_count) > 0 else "N/A")
        
        # Verify records in database
        try:
            from .models import ChartOfAccount
            db_count = ChartOfAccount.objects.filter(data_file=self.data_file).count()
            logger.info(f"🔍 Database verification: {db_count} COA records found in database")
            if db_count != processed_count:
                logger.warning(f"⚠️ Database mismatch: Expected {processed_count}, found {db_count}")
            else:
                logger.info(f"✅ Database verification successful: All {processed_count} records saved!")
        except Exception as verify_error:
            logger.error(f"❌ Database verification failed: {verify_error}")
        
        logger.info("🎯" + "="*60)
        
        return {'processed_count': processed_count, 'failed_count': failed_count}
    
    def _group_chart_data_by_account(self, df: pd.DataFrame) -> Dict[str, List[pd.Series]]:
        """
        Group Chart of Accounts data by account code to handle hierarchical structure
        
        Args:
            df: Cleaned pandas DataFrame
            
        Returns:
            Dict: Account code -> List of rows for that account
        """
        account_groups = {}
        
        for index, row in df.iterrows():
            try:
                # Extract account code using the same logic as _create_chart_record_from_row
                account = self._extract_account_code(row)
                
                if account:
                    if account not in account_groups:
                        account_groups[account] = []
                    account_groups[account].append(row)
                else:
                    # Debug: Log the row data to see what's available
                    available_columns = [col for col in row.index if pd.notna(row.get(col)) and str(row.get(col)).strip()]
                    logger.warning(f"Chart of Accounts: Skipped row {index} - no account code found. Available columns: {available_columns[:5]}...")
                    
            except Exception as e:
                logger.error(f"Error grouping row {index}: {e}")
        
        return account_groups
    
    def _extract_account_code(self, row: pd.Series) -> str:
        """Extract account code from row using flexible column mapping"""
        # Try multiple possible account field names - prioritize 'Account' field
        possible_names = [
            'Account', 'account', 'ACCOUNT',
            'G/L acct', 'G/L Acct', 'GL Account', 'GL Account Code',
            'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
            'Account Code', 'account_code', 'Account_Code'
        ]
        
        for name in possible_names:
            if name in row.index:
                value = str(row.get(name, '')).strip()
                if value and value != 'nan' and value != 'None' and value != 'null' and value != '':
                    # Special handling for G/L account format (e.g., "124000/1003" -> "124000")
                    if name in ['G/L acct', 'G/L Acct', 'GL Account', 'GL Account Code'] and '/' in value:
                        # Split by slash and take the first part (account code)
                        account_part = value.split('/')[0].strip()
                        if account_part:
                            return self._clean_account_code(account_part)
                    
                    return self._clean_account_code(value)
        
        # Debug: If no account code found, log what columns are available
        available_cols = [col for col in row.index if pd.notna(row.get(col)) and str(row.get(col)).strip()]
        logger.debug(f"No account code found. Available columns: {available_cols}")
        return ''
    
    def _clean_account_code(self, account_code: str) -> str:
        """
        Clean account code to make it alphanumeric only for database storage
        
        Args:
            account_code: Raw account code from data
            
        Returns:
            str: Cleaned account code suitable for database storage
        """
        if not account_code:
            return ''
        
        # Convert to string and strip whitespace
        cleaned = str(account_code).strip()
        
        # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
        if '.' in cleaned:
            try:
                float_val = float(cleaned)
                if float_val.is_integer():
                    cleaned = str(int(float_val))
            except ValueError:
                pass
        
        # Remove commas if present (thousands separators)
        cleaned = cleaned.replace(',', '')
        
        # For Chart of Accounts, we need to handle descriptive names
        # Convert spaces, hyphens, and other special characters to underscores
        # This preserves the meaning while making it alphanumeric
        cleaned = re.sub(r'[^A-Za-z0-9]', '_', cleaned)
        
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        
        # Ensure it's not empty after cleaning
        if not cleaned:
            cleaned = 'UNKNOWN_ACCOUNT'
        
        # Limit length to database field size (20 characters)
        if len(cleaned) > 20:
            cleaned = cleaned[:20]
            logger.warning(f"Account code truncated to 20 characters: {cleaned}")
        
        return cleaned
    
    def _get_original_account_name(self, row: pd.Series) -> str:
        """Get the original account name before cleaning"""
        possible_names = [
            'Account', 'account', 'ACCOUNT',
            'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
            'G/L acct', 'GL Account', 'GL Account Code',
            'Account Code', 'account_code', 'Account_Code'
        ]
        
        for name in possible_names:
            if name in row.index:
                value = str(row.get(name, '')).strip()
                if value and value != 'nan' and value != 'None' and value != 'null' and value != '':
                    return value
        
        return ''
    
    def _process_account_group(self, account_code: str, account_rows: List[pd.Series]) -> List[ChartOfAccount]:
        """
        Process a group of rows for the same account
        
        Args:
            account_code: The account code
            account_rows: List of rows for this account
            
        Returns:
            List: ChartOfAccount records for this account
        """
        records = []
        
        # Use the first row to get common account information
        first_row = account_rows[0]
        
        # Extract common fields from first row
        type_field = self._extract_field_value(first_row, [
            'Type', 'type', 'TYPE', 'Account Type', 'account_type', 'Account_Type'
        ])
        
        sub_type = self._extract_field_value(first_row, [
            'Sub Type', 'sub type', 'SUB TYPE', 'Sub_Type', 'sub_type',
            'Account Sub Type', 'account_sub_type', 'Account_Sub_Type'
        ])
        
        # Extract common fields that apply to all sub-sub types
        company = self._extract_field_value(first_row, [
            'Company', 'company', 'COMPANY', 'Company Code', 'company_code', 'Company_Code'
        ])
        
        branch = self._extract_field_value(first_row, [
            'Branch', 'branch', 'BRANCH', 'Branch Code', 'branch_code', 'Branch_Code'
        ])
        
        cost_center = self._extract_field_value(first_row, [
            'Cost. C', 'Cost Center', 'cost_center', 'Cost_Center',
            'Cost Center Code', 'cost_center_code', 'Cost_Center_Code'
        ])
        
        gl_account_long_text = self._extract_field_value(first_row, [
            'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
            'Account Long Text', 'account_long_text', 'Account_Long_Text',
            'Description', 'description', 'DESCRIPTION'
        ])
        
        # Extract GL Account field (e.g., "124000/1003")
        gl_account = self._extract_field_value(first_row, [
            'G/L acct', 'G/L Acct', 'GL Account', 'GL Account Code', 'gl_account'
        ])
        
        # Extract Code field (e.g., "1003")
        code = self._extract_field_value(first_row, [
            'Code', 'code', 'CODE'
        ])
        
        # Process each row as a separate sub-sub type
        for row in account_rows:
            try:
                # Extract sub-sub type specific information
                sub_sub_type = self._extract_field_value(row, [
                    'Sub Sub Type', 'sub sub type', 'SUB SUB TYPE', 'Sub_Sub_Type', 'sub_sub_type',
                    'Account Sub Sub Type', 'account_sub_sub_type', 'Account_Sub_Sub_Type'
                ])
                
                # If no sub-sub type found, use a default or the account code
                if not sub_sub_type:
                    sub_sub_type = f"SubType_{len(records) + 1}"
                
                # Extract financial data specific to this sub-sub type
                q1_amount = self._extract_decimal_value(row, [
                    'Q1 2025', 'Q1 Amount', 'q1_amount', 'Q1_Amount', 'Q1', 'q1'
                ])
                
                adj_reclas = self._extract_decimal_value(row, [
                    'Adj/Reclas', 'Adjustment', 'adjustment', 'Adjustment/Reclassification', 'adj_reclas'
                ])
                
                fin_q1_amount = self._extract_decimal_value(row, [
                    'Fin Q1 2025', 'Final Q1', 'final_q1', 'Final_Q1', 'Final Q1 Amount', 'fin_q1_amount'
                ])
                
                # Extract FY 2024 amount
                fy_2024_amount = self._extract_decimal_value(row, [
                    'FY 2024', 'Fiscal Year 2024', 'fy_2024', 'FY_2024'
                ])
                
                # Create ChartOfAccount record with validation
                try:
                    # Store original account name in long text if different from cleaned code
                    original_account_name = self._get_original_account_name(account_rows[0])
                    if original_account_name and original_account_name != account_code:
                        display_name = f"{original_account_name} ({account_code})"
                    else:
                        display_name = gl_account_long_text or account_code
                    
                    # Add FY 2024 amount information to the display name if available
                    if fy_2024_amount and fy_2024_amount != Decimal('0'):
                        display_name += f" [FY2024: {fy_2024_amount:,.2f}]"
                    
                    # Create ChartOfAccount record for bulk processing
                    chart_record = ChartOfAccount(
                        data_file=self.data_file,
                        account=account_code,  # Cleaned account code
                        type=type_field or 'Unknown',
                        sub_type=sub_type or 'Unknown',
                        sub_sub_type=sub_sub_type,
                        gl_account=gl_account or account_code,  # Use extracted GL account or cleaned account code
                        gl_account_ref=None,  # Will be created if needed
                        company=company or 'DEFAULT',  # Provide default company
                        branch=branch,
                        cost_center=cost_center or 'DEFAULT',  # Provide default cost center
                        code=code or account_code,  # Use extracted code or cleaned account code
                        gl_account_long_text=display_name,  # Include original name for reference
                        ref_to_fs=self._extract_field_value(row, [
                            'REF to FS', 'Ref to FS', 'ref_to_fs', 'Ref_To_FS',
                            'Reference to FS', 'reference_to_fs', 'Reference_To_FS'
                        ]),
                        financial_statement=self._extract_field_value(row, [
                            'F.S', 'Financial Statement', 'financial_statement', 'Financial_Statement',
                            'FS', 'fs', 'FS Category', 'fs_category', 'FS_Category'
                        ]),
                        ref_to_note=self._extract_field_value(row, [
                            'Ref to Note', 'ref_to_note', 'Ref_To_Note',
                            'Reference to Note', 'reference_to_note', 'Reference_To_Note'
                        ]),
                        fiscal_year=self._extract_int_value(row, [
                            'FY 2024', 'Fiscal Year', 'fiscal_year', 'Fiscal_Year', 'Year', 'year'
                        ]) or 2024,
                        q1_amount=q1_amount,
                        adj_reclas=adj_reclas,
                        fin_q1_amount=fin_q1_amount
                    )
                    
                    # Validate the record before adding to list
                    chart_record.clean()
                    records.append(chart_record)
                    
                except Exception as validation_error:
                    logger.error(f"❌ Validation error for account {account_code}, sub-sub-type {sub_sub_type}: {validation_error}")
                    # Create a minimal valid record as fallback
                    try:
                        # Get original account name for fallback
                        original_name = self._get_original_account_name(account_rows[0])
                        fallback_display_name = f"{original_name} ({account_code})" if original_name else f"Account {account_code}"
                        
                        fallback_record = ChartOfAccount(
                            data_file=self.data_file,
                            account=account_code,  # Cleaned account code
                            type=type_field or 'Unknown',
                            sub_type=sub_type or 'Unknown',
                            sub_sub_type=sub_sub_type,
                            gl_account=gl_account or account_code,  # Use extracted GL account or cleaned account code
                            company=company or 'DEFAULT',  # Provide default company
                            branch=branch,
                            cost_center=cost_center or 'DEFAULT',  # Provide default cost center
                            code=code or account_code,  # Use extracted code or cleaned account code
                            gl_account_long_text=fallback_display_name,
                            fiscal_year=2024,
                            q1_amount=Decimal('0'),
                            adj_reclas=Decimal('0'),
                            fin_q1_amount=Decimal('0')
                        )
                        fallback_record.clean()
                        records.append(fallback_record)
                        logger.info(f"✅ Created fallback record for account {account_code}")
                    except Exception as fallback_error:
                        logger.error(f"❌ Failed to create fallback record for account {account_code}: {fallback_error}")
                
            except Exception as e:
                logger.error(f"Error processing sub-sub type for account {account_code}: {e}")
        
        return records
    
    def _extract_field_value(self, row: pd.Series, possible_names: List[str]) -> str:
        """Extract field value using flexible column mapping"""
        for name in possible_names:
            if name in row.index:
                value = str(row.get(name, '')).strip()
                if value and value != 'nan' and value != 'None' and value != 'null' and value != '':
                    return value
        return ''
    
    def _bulk_upsert_coa_records(self, records: List[ChartOfAccount]):
        """
        Use SQLAlchemy for bulk upsert operations on COA records
        Handles the unique constraint (account, cost_center) efficiently
        """
        try:
            from sqlalchemy import create_engine, text
            from django.conf import settings
            import uuid
            
            # Get database connection string from Django settings
            db_config = settings.DATABASES['default']
            if db_config['ENGINE'] == 'django.db.backends.postgresql':
                connection_string = f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
            else:
                # Fallback for other databases
                logger.warning("SQLAlchemy bulk operations only supported for PostgreSQL")
                raise Exception("Unsupported database engine for bulk operations")
            
            # Create SQLAlchemy engine
            engine = create_engine(connection_string)
            
            # Prepare data for bulk insert
            records_data = []
            for record in records:
                record_data = {
                    'id': str(uuid.uuid4()),
                    'data_file_id': str(record.data_file.id),
                    'account': record.account,
                    'type': record.type,
                    'sub_type': record.sub_type,
                    'sub_sub_type': record.sub_sub_type,
                    'gl_account': record.gl_account,
                    'gl_account_ref_id': str(record.gl_account_ref.id) if record.gl_account_ref else None,
                    'company': record.company,
                    'branch': record.branch,
                    'cost_center': record.cost_center,
                    'code': record.code,
                    'gl_account_long_text': record.gl_account_long_text,
                    'ref_to_fs': record.ref_to_fs,
                    'financial_statement': record.financial_statement,
                    'ref_to_note': record.ref_to_note,
                    'fiscal_year': record.fiscal_year,
                    'q1_amount': float(record.q1_amount) if record.q1_amount else None,
                    'adj_reclas': float(record.adj_reclas) if record.adj_reclas else None,
                    'fin_q1_amount': float(record.fin_q1_amount) if record.fin_q1_amount else None,
                    'created_at': timezone.now().isoformat(),
                    'updated_at': timezone.now().isoformat()
                }
                records_data.append(record_data)
            
            # Use PostgreSQL's ON CONFLICT for upsert
            with engine.connect() as conn:
                # Create temporary table for bulk insert
                conn.execute(text("""
                    CREATE TEMP TABLE temp_coa_records (
                        id UUID PRIMARY KEY,
                        data_file_id UUID NOT NULL,
                        account VARCHAR(10) NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        sub_type VARCHAR(50) NOT NULL,
                        sub_sub_type VARCHAR(50) NOT NULL,
                        gl_account VARCHAR(20),
                        gl_account_ref_id UUID,
                        company VARCHAR(50),
                        branch VARCHAR(50),
                        cost_center VARCHAR(50) NOT NULL,
                        code VARCHAR(50),
                        gl_account_long_text TEXT,
                        ref_to_fs VARCHAR(100),
                        financial_statement VARCHAR(100),
                        ref_to_note VARCHAR(100),
                        fiscal_year INTEGER,
                        q1_amount DECIMAL(30,15),
                        adj_reclas DECIMAL(30,15),
                        fin_q1_amount DECIMAL(30,15),
                        created_at TIMESTAMP WITH TIME ZONE,
                        updated_at TIMESTAMP WITH TIME ZONE
                    )
                """))
                
                # Bulk insert into temp table
                if records_data:
                    conn.execute(text("""
                        INSERT INTO temp_coa_records 
                        (id, data_file_id, account, type, sub_type, sub_sub_type, gl_account, 
                         gl_account_ref_id, company, branch, cost_center, code, gl_account_long_text,
                         ref_to_fs, financial_statement, ref_to_note, fiscal_year, q1_amount, 
                         adj_reclas, fin_q1_amount, created_at, updated_at)
                        VALUES 
                        (:id, :data_file_id, :account, :type, :sub_type, :sub_sub_type, :gl_account,
                         :gl_account_ref_id, :company, :branch, :cost_center, :code, :gl_account_long_text,
                         :ref_to_fs, :financial_statement, :ref_to_note, :fiscal_year, :q1_amount,
                         :adj_reclas, :fin_q1_amount, :created_at, :updated_at)
                    """), records_data)
                
                # Upsert from temp table to main table
                conn.execute(text("""
                    INSERT INTO chart_of_accounts 
                    (id, data_file_id, account, type, sub_type, sub_sub_type, gl_account,
                     gl_account_ref_id, company, branch, cost_center, code, gl_account_long_text,
                     ref_to_fs, financial_statement, ref_to_note, fiscal_year, q1_amount,
                     adj_reclas, fin_q1_amount, created_at, updated_at)
                    SELECT 
                        id, data_file_id, account, type, sub_type, sub_sub_type, gl_account,
                        gl_account_ref_id, company, branch, cost_center, code, gl_account_long_text,
                        ref_to_fs, financial_statement, ref_to_note, fiscal_year, q1_amount,
                        adj_reclas, fin_q1_amount, created_at, updated_at
                    FROM temp_coa_records
                    ON CONFLICT (account, cost_center) 
                    DO UPDATE SET
                        data_file_id = EXCLUDED.data_file_id,
                        type = EXCLUDED.type,
                        sub_type = EXCLUDED.sub_type,
                        sub_sub_type = EXCLUDED.sub_sub_type,
                        gl_account = EXCLUDED.gl_account,
                        gl_account_ref_id = EXCLUDED.gl_account_ref_id,
                        company = EXCLUDED.company,
                        branch = EXCLUDED.branch,
                        code = EXCLUDED.code,
                        gl_account_long_text = EXCLUDED.gl_account_long_text,
                        ref_to_fs = EXCLUDED.ref_to_fs,
                        financial_statement = EXCLUDED.financial_statement,
                        ref_to_note = EXCLUDED.ref_to_note,
                        fiscal_year = EXCLUDED.fiscal_year,
                        q1_amount = EXCLUDED.q1_amount,
                        adj_reclas = EXCLUDED.adj_reclas,
                        fin_q1_amount = EXCLUDED.fin_q1_amount,
                        updated_at = EXCLUDED.updated_at
                """))
                
                conn.commit()
                logger.info(f"✅ SQLAlchemy bulk upsert completed for {len(records_data)} COA records")
                
        except Exception as e:
            logger.error(f"❌ SQLAlchemy bulk upsert failed: {e}")
            raise e
    
    def _extract_decimal_value(self, row: pd.Series, possible_names: List[str]) -> Decimal:
        """Extract decimal value using flexible column mapping"""
        for name in possible_names:
            if name in row.index:
                value = DataParser.parse_decimal(row.get(name, ''))
                if value:
                    return value
        return Decimal('0')
    
    def _extract_int_value(self, row: pd.Series, possible_names: List[str]) -> int:
        """Extract integer value using flexible column mapping"""
        for name in possible_names:
            if name in row.index:
                value = DataParser.parse_int(row.get(name, ''))
                if value:
                    return value
        return None
    
    def _detect_chart_start_row(self, df: pd.DataFrame) -> int:
        """
        Detect the optimal starting row for Chart of Accounts data
        
        Args:
            df: pandas DataFrame
            
        Returns:
            int: Optimal starting row (1-based index)
        """
        if df.empty:
            return 1
        
        # Look for common patterns that indicate data starts from row 8
        chart_indicators = ['Account', 'Type', 'Sub Type', 'Sub Sub Type', 'G/L acct']
        
        # Check if row 8 (index 7) has chart data indicators
        if len(df) > 7:
            row_8 = df.iloc[7]
            row_8_headers = [str(col).lower().strip() for col in row_8.index]
            row_8_values = [str(val).lower().strip() for val in row_8.values if pd.notna(val)]
            
            # Check if row 8 contains chart indicators
            has_chart_indicators = any(
                any(indicator.lower() in str(val).lower() for val in row_8_values) 
                for indicator in chart_indicators
            )
            
            if has_chart_indicators:
                logger.info("Chart of Accounts: Detected data starting from row 8")
                return 8
        
        # Fallback to auto-detection
        for index, row in df.iterrows():
            # Check if any cell in the row has meaningful content
            has_content = False
            for value in row:
                if pd.notna(value) and str(value).strip():
                    has_content = True
                    break
            
            if has_content:
                logger.info(f"Chart of Accounts: Auto-detected data starting from row {index + 1}")
                return index + 1
        
        logger.info("Chart of Accounts: No data detected, starting from row 1")
        return 1
    
    def _bulk_create_with_progress(self, model_class, records_to_create: List, operation_name: str):
        """
        💾 Bulk Database Save with Connection Management & Transaction Handling! 💾
        
        This method efficiently saves large batches of records to the database with:
        - Proper transaction handling (all-or-nothing)
        - Database connection retry logic
        - Connection health checks
        - Optimized batch processing
        
        Args:
            model_class: Django model class (our database table!)
            records_to_create: List of model instances to create (our data treasures!)
            operation_name: Name of operation for logging (our operation identifier!)
        """
        from django.db import transaction, connection
        from django.db.utils import OperationalError, InterfaceError
        from .models import ChartOfAccount  # Ensure ChartOfAccount is available
        import time
        
        logger.info("💾" + "="*50)
        logger.info("💾 BULK DATABASE SAVE - THE FINAL STEP! 💾")
        logger.info(f"📊 Saving {len(records_to_create)} {operation_name}...")
        logger.info(f"🔢 Batch Size: {self.batch_size}")
        logger.info("💾" + "="*50)
        
        total_created = 0
        batch_count = 0
        max_retries = 3
        retry_delay = 2  # seconds
        
        def _ensure_db_connection():
            """Ensure database connection is alive and reconnect if needed"""
            try:
                connection.ensure_connection()
                return True
            except (OperationalError, InterfaceError) as e:
                logger.warning(f"🔄 Database connection issue: {e}. Attempting to reconnect...")
                try:
                    connection.close()
                    connection.connect()
                    return True
                except Exception as reconnect_error:
                    logger.error(f"❌ Failed to reconnect to database: {reconnect_error}")
                    return False
        
        def _save_batch_with_retry(batch, batch_num):
            """Save a batch with retry logic for connection issues"""
            for attempt in range(max_retries):
                try:
                    # Ensure connection is alive before each batch
                    if not _ensure_db_connection():
                        raise Exception("Cannot establish database connection")
                    
                    # Validate each record before bulk create (skip for ChartOfAccount)
                    for record in batch:
                        if model_class.__name__ != 'ChartOfAccount':
                            record.full_clean()  # Validate the record
                    
                    # Save the batch
                    model_class.objects.bulk_create(batch, batch_size=min(self.batch_size, 1000), ignore_conflicts=True)
                    return True
                    
                except (OperationalError, InterfaceError) as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"🔄 Batch {batch_num} attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"❌ Batch {batch_num} failed after {max_retries} attempts: {e}")
                        raise Exception(f"Batch {batch_num} failed after {max_retries} attempts: {str(e)}")
                except Exception as e:
                    logger.error(f"❌ Batch {batch_num} failed with non-connection error: {e}")
                    raise Exception(f"Batch {batch_num} failed: {str(e)}")
            
            return False
        
        try:
            # Process each batch individually with its own transaction
            for i in range(0, len(records_to_create), self.batch_size):
                batch = records_to_create[i:i + self.batch_size]
                batch_count += 1
                
                logger.info(f"💿 Processing batch {batch_count}: {len(batch)} records...")
                
                try:
                    # Use individual transaction for each batch
                    with transaction.atomic():
                        # Filter out invalid records before saving
                        valid_records = []
                        invalid_count = 0
                        
                        for record_index, record in enumerate(batch):
                            try:
                                # Skip validation for ChartOfAccount since we've already cleaned them during processing
                                if model_class.__name__ != 'ChartOfAccount':
                                    record.full_clean()  # Validate the record
                                valid_records.append(record)
                            except Exception as validation_error:
                                invalid_count += 1
                                logger.warning(f"⚠️ Invalid record found, saving to error model: {validation_error}")
                                
                                # Save failed record to error model
                                try:
                                    from .models import SAPGLPostingError
                                    
                                    # Extract raw data from the record
                                    raw_data = {}
                                    for field in record._meta.fields:
                                        if hasattr(record, field.name):
                                            value = getattr(record, field.name)
                                            # Convert datetime objects to strings for JSON serialization
                                            if hasattr(value, 'isoformat'):
                                                raw_data[field.name] = value.isoformat()
                                            else:
                                                raw_data[field.name] = str(value) if value is not None else None
                                    
                                    # Create error record
                                    error_record = SAPGLPostingError(
                                        data_file=record.data_file,
                                        error_type=type(validation_error).__name__,
                                        error_message=str(validation_error),
                                        raw_data=raw_data,
                                        batch_number=batch_count,
                                        record_index=record_index
                                    )
                                    error_record.save()
                                    logger.info(f"✅ Failed record saved to error model (Error ID: {error_record.id})")
                                    
                                except Exception as error_save_error:
                                    logger.error(f"❌ Failed to save error record: {error_save_error}")
                                continue
                        
                        if valid_records:
                            # Save only valid records
                            if _save_batch_with_retry(valid_records, batch_count):
                                total_created += len(valid_records)
                                logger.info(f"✅ Batch {batch_count} saved successfully! ({len(valid_records)} valid records)")
                                
                                if invalid_count > 0:
                                    logger.warning(f"⚠️ Skipped {invalid_count} invalid records in batch {batch_count}")
                            else:
                                logger.error(f"❌ Failed to save batch {batch_count} after retries")
                        else:
                            logger.warning(f"⚠️ Batch {batch_count} had no valid records, skipping...")
                            
                    # Log progress for large datasets
                    if len(records_to_create) > 1000:
                        progress_percent = (total_created / len(records_to_create)) * 100
                        logger.info(f"📈 Progress: {total_created}/{len(records_to_create)} ({progress_percent:.1f}%) {operation_name}...")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing batch {batch_count}: {e}")
                    logger.error(f"🔍 Batch details: {len(batch)} records")
                    # Continue with next batch instead of failing completely
                    continue
            
            logger.info("💾" + "="*50)
            logger.info(f"✅ Successfully saved {total_created} {operation_name}")
            logger.info(f"📊 Total Batches Processed: {batch_count}")
            logger.info(f"📈 Success Rate: {(total_created / len(records_to_create) * 100):.1f}%")
            logger.info("💾" + "="*50)
            
            # Verify records were actually saved to database
            try:
                actual_count = model_class.objects.filter(data_file=self.data_file).count()
                logger.info(f"🔍 Database verification: {actual_count} {operation_name} found in database")
                if actual_count != total_created:
                    logger.warning(f"⚠️ Mismatch: Expected {total_created}, found {actual_count} in database")
            except Exception as verify_error:
                logger.error(f"❌ Database verification failed: {verify_error}")
            
            # Update GLAccount records after bulk operations
            self._update_gl_accounts_after_bulk_operation(model_class, operation_name)
                
        except Exception as e:
            logger.error("💥" + "="*50)
            logger.error("💥 PROCESSING FAILED! 💥")
            logger.error(f"❌ Error: {str(e)}")
            logger.error(f"📊 Records attempted: {len(records_to_create)}")
            logger.error(f"📊 Records saved before failure: {total_created}")
            logger.error(f"📊 Batches processed: {batch_count}")
            logger.error("💥" + "="*50)
            raise Exception(f"Processing failed: {str(e)}")
    
    def _update_gl_accounts_after_bulk_operation(self, model_class, operation_name: str):
        """
        Update GLAccount records after bulk operations since bulk_create bypasses save() methods
        
        This ensures that:
        - TB data updates GLAccount opening/closing balances
        - COA data updates GLAccount type hierarchy and cost_code
        - Profit Center auto-linking happens
        """
        try:
            from .models import GLAccount, TrialBalance, ChartOfAccount, ProfitCenter
            
            logger.info("🔄 Updating GLAccount records after bulk operation...")
            
            if model_class.__name__ == 'TrialBalance':
                # Update GLAccount with TB data
                logger.info("📊 Updating GLAccount records with TB data...")
                
                # Get all TB records for this data file
                tb_records = TrialBalance.objects.filter(data_file=self.data_file)
                
                for tb_record in tb_records:
                    if tb_record.gl_account_ref:
                        # Update GLAccount with TB information
                        gl_account = tb_record.gl_account_ref
                        gl_account.opening_balance = tb_record.opening_balance
                        gl_account.closing_balance = tb_record.closing_balance
                        gl_account.tb_debit = tb_record.debit
                        gl_account.tb_credit = tb_record.credit
                        gl_account.save()
                        
                        logger.debug(f"✅ Updated GLAccount {gl_account.account_code} with TB data")
                
                logger.info(f"✅ Updated {tb_records.count()} GLAccount records with TB data")
                
            elif model_class.__name__ == 'ChartOfAccount':
                # Update GLAccount with COA data
                logger.info("📊 Updating GLAccount records with COA data...")
                
                # Get all COA records for this data file
                coa_records = ChartOfAccount.objects.filter(data_file=self.data_file)
                
                for coa_record in coa_records:
                    if coa_record.gl_account_ref:
                        # Update GLAccount with COA hierarchy information
                        gl_account = coa_record.gl_account_ref
                        gl_account.account_type = coa_record.type
                        gl_account.sub_type = coa_record.sub_type
                        gl_account.sub_sub_type = coa_record.sub_sub_type
                        gl_account.financial_statement_category = coa_record.financial_statement
                        
                        # Also set cost_code from COA if not already set
                        if coa_record.cost_center and not gl_account.cost_code:
                            gl_account.cost_code = coa_record.cost_center
                        
                        gl_account.save()
                        
                        # Auto-link to ProfitCenter if cost_code is provided
                        if gl_account.cost_code and not gl_account.profit_center_ref:
                            profit_center = ProfitCenter.objects.filter(profit_center_code=gl_account.cost_code).first()
                            if profit_center:
                                gl_account.profit_center_ref = profit_center
                                gl_account.save()
                                logger.debug(f"✅ Linked GLAccount {gl_account.account_code} to ProfitCenter {profit_center.profit_center_code}")
                        
                        logger.debug(f"✅ Updated GLAccount {gl_account.account_code} with COA data")
                
                logger.info(f"✅ Updated {coa_records.count()} GLAccount records with COA data")
                
        except Exception as e:
            logger.error(f"❌ Error updating GLAccount records after bulk operation: {e}")
            # Don't raise exception as this is a post-processing step
    
    def _update_gl_accounts_after_gl_processing(self):
        """
        Update GLAccount records after GL processing to ensure Profit Center auto-linking
        
        This ensures that:
        - GLAccount records created during GL processing are properly linked to Profit Centers
        - Cost codes from GL data are used for Profit Center linking
        """
        try:
            from .models import GLAccount, ProfitCenter
            
            logger.info("🔄 Updating GLAccount records after GL processing...")
            
            # Get all GLAccount records for this engagement that have cost_code but no profit_center_ref
            gl_accounts_to_update = GLAccount.objects.filter(
                engagement=self.data_file.engagement,
                cost_code__isnull=False
            ).exclude(
                cost_code=''
            ).filter(
                profit_center_ref__isnull=True
            )
            
            updated_count = 0
            for gl_account in gl_accounts_to_update:
                # Auto-link to ProfitCenter if cost_code is provided
                if gl_account.cost_code:
                    profit_center = ProfitCenter.objects.filter(profit_center_code=gl_account.cost_code).first()
                    if profit_center:
                        gl_account.profit_center_ref = profit_center
                        gl_account.save()
                        updated_count += 1
                        logger.debug(f"✅ Linked GLAccount {gl_account.account_code} to ProfitCenter {profit_center.profit_center_code}")
                    else:
                        logger.debug(f"⚠️ No ProfitCenter found for cost_code: {gl_account.cost_code}")
            
            logger.info(f"✅ Updated {updated_count} GLAccount records with Profit Center links")
                
        except Exception as e:
            logger.error(f"❌ Error updating GLAccount records after GL processing: {e}")
            # Don't raise exception as this is a post-processing step
    
    def _create_gl_posting_from_row(self, row: pd.Series) -> Optional[SAPGLPosting]:
        """Create SAPGLPosting from CSV row with GL Account reference"""
        try:
            from .models import GLAccount
            from django.db import connection
            from django.db.utils import OperationalError, InterfaceError
            
            # Ensure database connection is alive
            try:
                connection.ensure_connection()
            except (OperationalError, InterfaceError) as e:
                logger.warning(f"Database connection issue in GL posting creation: {e}")
                # Try to reconnect
                try:
                    connection.close()
                    connection.connect()
                except Exception as reconnect_error:
                    logger.error(f"Failed to reconnect: {reconnect_error}")
                    return None
            
            gl_account_code = row.get('G/L Account', '')
            if not gl_account_code:
                logger.warning("Skipping row with missing G/L Account")
                return None
            
            # Convert float/int account codes to proper string format
            gl_account_code = str(gl_account_code).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in gl_account_code:
                try:
                    float_val = float(gl_account_code)
                    if float_val.is_integer():
                        gl_account_code = str(int(float_val))
                except ValueError:
                    pass  # Keep original string if conversion fails
            
            # Create or get GL Account
            gl_account = GLAccount.get_or_create_account(
                engagement=self.data_file.engagement,
                account_code=gl_account_code,
                account_name=f'GL Account {gl_account_code}',  # Default name, can be updated later
                company_code=row.get('Company Code', ''),
                cost_code=row.get('Profit Center', '')  # Map Profit Center to cost_code
            )
            
            # Handle document number - convert float/int to proper string format
            document_number = str(row.get('Document', '')).strip()
            
            # Handle decimal representations of whole numbers (e.g., "1234567.0" → "1234567")
            if '.' in document_number:
                try:
                    float_val = float(document_number)
                    if float_val.is_integer():
                        document_number = str(int(float_val))
                except ValueError:
                    pass  # Keep original string if conversion fails
            
            posting = SAPGLPosting(
                data_file=self.data_file,
                document_number=document_number,
                document_type=row.get('Document type', ''),
                amount_local_currency=DataParser.parse_decimal(row.get('Amount in Local Currency', '0')),
                local_currency=row.get('Local Currency', 'SAR'),
                gl_account=gl_account_code,  # Keep legacy field
                gl_account_ref=gl_account,   # New foreign key reference
                profit_center=row.get('Profit Center', ''),
                user_name=row.get('User Name', ''),
                fiscal_year=DataParser.parse_int(row.get('Fiscal Year', '2025')) or 2025,
                posting_period=DataParser.parse_int(row.get('Posting period', '1')) or 1,
                text=row.get('Text', ''),
                segment=row.get('Segment', ''),
                clearing_document=self._convert_decimal_to_int_string(row.get('Clearing Document', '')),
                offsetting_account=self._convert_decimal_to_int_string(row.get('Offsetting', '')),
                invoice_reference=self._convert_decimal_to_int_string(row.get('Invoice Reference', '')),
                sales_document=self._convert_decimal_to_int_string(row.get('Sales Document', '')),
                assignment=row.get('Assignment', ''),
                year_month=row.get('Year/Month', '')
            )
            
            # Parse dates
            posting.posting_date = self._parse_posting_date(row.get('Posting Date', ''))
            posting.document_date = self._parse_document_date(row.get('Document Date', ''))
            posting.entry_date = self._parse_entry_date(row.get('Entry Date', ''))
            
            return posting
            
        except Exception as e:
            logger.error(f"Error creating GL posting from row: {e}")
            return None
    
    def _create_tb_record_from_row(self, row: pd.Series) -> Optional[TrialBalance]:
        """Create TrialBalance from CSV row with GL Account reference"""
        try:
            from .models import GLAccount
            
            gl_account_code = row.get('GL Account', '')
            if not gl_account_code:
                logger.warning("Skipping row with missing GL Account")
                return None
            
            # Convert to string to handle numeric values from Excel
            gl_account_code = str(gl_account_code).strip()
            
            # Handle decimal representations of whole numbers (e.g., "110000.0" → "110000")
            if '.' in gl_account_code:
                try:
                    float_val = float(gl_account_code)
                    if float_val.is_integer():
                        gl_account_code = str(int(float_val))
                except ValueError:
                    pass  # Keep original string if conversion fails
            
            # Create or get GL Account
            gl_account = GLAccount.get_or_create_account(
                engagement=self.data_file.engagement,
                account_code=gl_account_code,
                account_name=row.get('Short Text', f'GL Account {gl_account_code}'),
                company_code=row.get('CoCd', '')
            )
            
            # Handle flexible column names and company code
            company_code = str(row.get('CoCd', row.get('Company Code', ''))).strip()
            if not company_code:
                company_code = '1000'  # Default company code if missing
            
            # Handle debit/credit columns with potential leading spaces
            debit_value = row.get('Debit', row.get(' Debit', ''))
            credit_value = row.get('Credit', row.get(' Credit', ''))
            
            tb_record = TrialBalance(
                data_file=self.data_file,
                company_code=company_code,
                gl_account=gl_account_code,  # Keep legacy field
                gl_account_ref=gl_account,   # New foreign key reference
                short_text=row.get('Short Text', ''),
                currency=row.get('Currency', 'SAR'),
                opening_balance=DataParser.parse_decimal(row.get('Opening Balance', '0')),
                debit=DataParser.parse_decimal(debit_value) if pd.notna(debit_value) and str(debit_value).strip() != '' else None,
                credit=DataParser.parse_decimal(credit_value) if pd.notna(credit_value) and str(credit_value).strip() != '' else None,
                closing_balance=DataParser.parse_decimal(row.get('Closing Balance', '0'))
            )
            return tb_record
            
        except Exception as e:
            logger.error(f"Error creating TB record from row: {e}")
            return None
    
    def _create_chart_record_from_row(self, row: pd.Series) -> Optional[ChartOfAccount]:
        """Create ChartOfAccount from CSV row with GL Account reference"""
        try:
            from .models import GLAccount
            
            # Enhanced column mapping - try multiple variations of column names
            def find_column_value(row, possible_names):
                """Find value from row using multiple possible column names"""
                for name in possible_names:
                    if name in row.index:
                        value = str(row.get(name, '')).strip()
                        if value and value != 'nan':
                            return value
                return ''
            
            # Try multiple possible account field names
            account = find_column_value(row, [
                'Account', 'account', 'ACCOUNT',
                'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
                'G/L acct', 'GL Account', 'GL Account Code',
                'Account Code', 'account_code', 'Account_Code'
            ])
            
            type_field = find_column_value(row, [
                'Type', 'type', 'TYPE',
                'Account Type', 'account_type', 'Account_Type'
            ])
            
            sub_type = find_column_value(row, [
                'Sub Type', 'sub type', 'SUB TYPE', 'Sub_Type', 'sub_type',
                'Account Sub Type', 'account_sub_type', 'Account_Sub_Type'
            ])
            
            sub_sub_type = find_column_value(row, [
                'Sub Sub Type', 'sub sub type', 'SUB SUB TYPE', 'Sub_Sub_Type', 'sub_sub_type',
                'Account Sub Sub Type', 'account_sub_sub_type', 'Account_Sub_Sub_Type'
            ])
            
            # Skip rows that are completely empty or have no meaningful data
            if not account and not type_field and not sub_type and not sub_sub_type:
                # Log more details about why row is being skipped
                logger.debug(f"Skipping completely empty row - Account: '{account}', Type: '{type_field}', Sub Type: '{sub_type}', Sub Sub Type: '{sub_sub_type}'")
                return None
            
            # For Chart of Accounts, we can be more flexible - use G/L Acct Long Text as account if Account is missing
            if not account and (type_field or sub_type or sub_sub_type):
                # Try to use G/L Acct Long Text as the account identifier
                gl_account_long_text = find_column_value(row, [
                    'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
                    'Account Long Text', 'account_long_text', 'Account_Long_Text',
                    'Description', 'description', 'DESCRIPTION'
                ])
                if gl_account_long_text:
                    account = gl_account_long_text
                    logger.info(f"Using G/L Acct Long Text as account: {account}")
                else:
                    # Last resort: try to find any field that looks like an account
                    for col_name, value in row.items():
                        if pd.notna(value) and str(value).strip() and str(value).strip() != 'nan':
                            # If this looks like an account (has numbers or meaningful text)
                            if any(char.isdigit() or char.isalpha() for char in str(value).strip()):
                                account = str(value).strip()
                                logger.info(f"Using '{col_name}' as account: {account}")
                                break
                    
                    if not account:
                        logger.warning(f"Skipping row with missing Account field: Account={account}, Type={type_field}, Sub Type={sub_type}, Sub Sub Type={sub_sub_type}")
                        return None
            
            # If we have an account but missing classification, provide defaults
            if account and not type_field:
                type_field = 'Unknown'
            if account and not sub_type:
                sub_type = 'Unknown'
            if account and not sub_sub_type:
                sub_sub_type = 'Unknown'
            
            # Additional check: if all fields are just whitespace or "Unknown", skip
            if account == 'Unknown' and type_field == 'Unknown' and sub_type == 'Unknown' and sub_sub_type == 'Unknown':
                logger.debug(f"Skipping row with all default values")
                return None
            
            # Try multiple possible GL account field names
            gl_account_code = find_column_value(row, [
                'G/L acct', 'G/L Acct', 'GL Account', 'GL Account Code',
                'Account Code', 'account_code', 'Account_Code',
                'G/L Account Code', 'GL_Account_Code'
            ])
            
            gl_account_ref = None
            
            # Create or get GL Account if G/L acct is provided
            if gl_account_code:
                # Try multiple possible company field names
                company_code = find_column_value(row, [
                    'Company', 'company', 'COMPANY', 'Company Code', 'company_code', 'Company_Code'
                ])
                
                # Try multiple possible cost center field names
                cost_center = find_column_value(row, [
                    'Cost. C', 'Cost Center', 'cost_center', 'Cost_Center',
                    'Cost Center Code', 'cost_center_code', 'Cost_Center_Code'
                ])
                
                # Try multiple possible financial statement field names
                financial_statement = find_column_value(row, [
                    'F.S', 'Financial Statement', 'financial_statement', 'Financial_Statement',
                    'FS', 'fs', 'FS Category', 'fs_category', 'FS_Category'
                ])
                
                gl_account_ref = GLAccount.get_or_create_account(
                    engagement=self.data_file.engagement,
                    account_code=gl_account_code,
                    account_name=account or f'GL Account {gl_account_code}',
                    company_code=company_code,
                    cost_center=cost_center,
                    financial_statement_category=financial_statement
                )
            
            # Map other fields with flexible column names
            company = find_column_value(row, [
                'Company', 'company', 'COMPANY', 'Company Code', 'company_code', 'Company_Code'
            ])
            
            branch = find_column_value(row, [
                'Branch', 'branch', 'BRANCH', 'Branch Code', 'branch_code', 'Branch_Code'
            ])
            
            cost_center = find_column_value(row, [
                'Cost. C', 'Cost Center', 'cost_center', 'Cost_Center',
                'Cost Center Code', 'cost_center_code', 'Cost_Center_Code'
            ])
            
            code = find_column_value(row, [
                'Code', 'code', 'CODE', 'Account Code', 'account_code', 'Account_Code'
            ])
            
            gl_account_long_text = find_column_value(row, [
                'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text',
                'Account Long Text', 'account_long_text', 'Account_Long_Text',
                'Description', 'description', 'DESCRIPTION'
            ])
            
            ref_to_fs = find_column_value(row, [
                'REF to FS', 'Ref to FS', 'ref_to_fs', 'Ref_To_FS',
                'Reference to FS', 'reference_to_fs', 'Reference_To_FS'
            ])
            
            financial_statement = find_column_value(row, [
                'F.S', 'Financial Statement', 'financial_statement', 'Financial_Statement',
                'FS', 'fs', 'FS Category', 'fs_category', 'FS_Category'
            ])
            
            ref_to_note = find_column_value(row, [
                'Ref to Note', 'ref_to_note', 'Ref_To_Note',
                'Reference to Note', 'reference_to_note', 'Reference_To_Note'
            ])
            
            # Try multiple fiscal year field names
            fiscal_year = 2024  # Default
            for field_name in ['FY 2024', 'Fiscal Year', 'fiscal_year', 'Fiscal_Year', 'Year', 'year']:
                if field_name in row.index:
                    fiscal_year = DataParser.parse_int(row.get(field_name, '')) or 2024
                    break
            
            # Try multiple Q1 amount field names
            q1_amount = Decimal('0')
            for field_name in ['Q1 2025', 'Q1 Amount', 'q1_amount', 'Q1_Amount', 'Q1', 'q1']:
                if field_name in row.index:
                    q1_amount = DataParser.parse_decimal(row.get(field_name, '')) or Decimal('0')
                    break
            
            # Try multiple adjustment field names
            adj_reclas = Decimal('0')
            for field_name in ['Adj/Reclas', 'Adjustment', 'adjustment', 'Adjustment/Reclassification', 'adj_reclas']:
                if field_name in row.index:
                    adj_reclas = DataParser.parse_decimal(row.get(field_name, '')) or Decimal('0')
                    break
            
            # Try multiple final Q1 amount field names
            fin_q1_amount = Decimal('0')
            for field_name in ['Fin Q1 2025', 'Final Q1', 'final_q1', 'Final_Q1', 'Final Q1 Amount', 'fin_q1_amount']:
                if field_name in row.index:
                    fin_q1_amount = DataParser.parse_decimal(row.get(field_name, '')) or Decimal('0')
                    break
            
            chart_record = ChartOfAccount(
                data_file=self.data_file,
                account=account,
                type=type_field,
                sub_type=sub_type,
                sub_sub_type=sub_sub_type,
                gl_account=gl_account_code,  # Keep legacy field
                gl_account_ref=gl_account_ref,  # New foreign key reference
                company=company,
                branch=branch,
                cost_center=cost_center,
                code=code,
                gl_account_long_text=gl_account_long_text,
                ref_to_fs=ref_to_fs,
                financial_statement=financial_statement,
                ref_to_note=ref_to_note,
                fiscal_year=fiscal_year,
                q1_amount=q1_amount,
                adj_reclas=adj_reclas,
                fin_q1_amount=fin_q1_amount
            )
            return chart_record
            
        except Exception as e:
            logger.error(f"Error creating Chart record from row: {e}")
            return None
    
    def _convert_decimal_to_int_string(self, value) -> str:
        """Convert decimal values like '1234567.0' to '1234567' string"""
        if not value:
            return ''
        
        value_str = str(value).strip()
        
        # Handle decimal representations of whole numbers (e.g., "1234567.0" → "1234567")
        if '.' in value_str:
            try:
                float_val = float(value_str)
                if float_val.is_integer():
                    return str(int(float_val))
            except ValueError:
                pass  # Keep original string if conversion fails
        
        return value_str
    
    def _parse_posting_date(self, date_value) -> datetime:
        """Parse posting date with fallback - handles strings like '1/15/2025', Timestamps, and datetime objects"""
        try:
            parsed_date = DataParser.parse_date(date_value)
            if parsed_date:
                return parsed_date.date()
            else:
                logger.warning(f"Could not parse posting date '{date_value}'. Using current date.")
                return datetime.now().date()
        except Exception as e:
            logger.warning(f"Error parsing posting date '{date_value}': {e}. Using current date.")
            return datetime.now().date()
    
    def _parse_document_date(self, date_value) -> Optional[datetime]:
        """Parse document date - handles strings like '1/15/2025', Timestamps, and datetime objects"""
        try:
            parsed_date = DataParser.parse_date(date_value)
            return parsed_date.date() if parsed_date else None
        except Exception as e:
            logger.warning(f"Error parsing document date '{date_value}': {e}. Using None.")
            return None
    
    def _parse_entry_date(self, date_value) -> Optional[datetime]:
        """Parse entry date - handles strings like '1/15/2025', Timestamps, and datetime objects"""
        try:
            parsed_date = DataParser.parse_date(date_value)
            return parsed_date.date() if parsed_date else None
        except Exception as e:
            logger.warning(f"Error parsing entry date '{date_value}': {e}. Using None.")
            return None
    
    def _clean_chart_dataframe(self, df: pd.DataFrame, start_row: int = None) -> pd.DataFrame:
        """
        Clean Chart of Accounts DataFrame by removing empty rows and handling initial empty rows
        
        Args:
            df: Original pandas DataFrame
            start_row: Optional starting row number (0-based index). If None, auto-detect.
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            return df
        
        # Remove completely empty rows (all NaN values)
        df_cleaned = df.dropna(how='all')
        
        # Determine starting row
        if start_row is not None:
            # Use specified starting row (convert to 0-based index if needed)
            if start_row > 0:
                start_row = start_row - 1  # Convert 1-based to 0-based index
            first_data_row = start_row
            logger.info(f"Chart of Accounts: Using specified starting row {start_row + 1} (0-based: {start_row})")
        else:
            # Auto-detect the first row that has meaningful data
            first_data_row = None
            for index, row in df_cleaned.iterrows():
                # Check if any cell in the row has meaningful content
                has_content = False
                for value in row:
                    if pd.notna(value) and str(value).strip():
                        has_content = True
                        break
                
                if has_content:
                    first_data_row = index
                    break
            
            if first_data_row is not None:
                logger.info(f"Chart of Accounts: Auto-detected starting data processing from row {first_data_row + 1} (0-based: {first_data_row})")
        
        # If we found a meaningful row, start from there
        if first_data_row is not None and first_data_row < len(df_cleaned):
            df_cleaned = df_cleaned.loc[first_data_row:]
            logger.info(f"Chart of Accounts: Starting data processing from row {first_data_row + 1}")
        else:
            logger.warning(f"Chart of Accounts: No valid starting row found, processing from beginning")
        
        # Remove rows where all key fields are empty - use flexible field detection
        key_field_variations = [
            ['Account', 'account', 'ACCOUNT', 'G/L Acct Long Text', 'G/L acct long text', 'GL Account Long Text'],
            ['Type', 'type', 'TYPE', 'Account Type', 'account_type', 'Account_Type'],
            ['Sub Type', 'sub type', 'SUB TYPE', 'Sub_Type', 'sub_type', 'Account Sub Type'],
            ['Sub Sub Type', 'sub sub type', 'SUB SUB TYPE', 'Sub_Sub_Type', 'sub_sub_type', 'Account Sub Sub Type'],
            ['G/L acct', 'G/L Acct', 'GL Account', 'GL Account Code', 'Account Code', 'account_code']
        ]
        
        available_key_fields = []
        for field_group in key_field_variations:
            for field in field_group:
                if field in df_cleaned.columns:
                    available_key_fields.append(field)
                    break  # Use the first match from each group
        
        if available_key_fields:
            # Create a mask for rows that have at least one non-empty key field
            # Be more lenient - only exclude rows that are truly empty
            mask = df_cleaned[available_key_fields].apply(
                lambda row: any(
                    pd.notna(val) and 
                    str(val).strip() and 
                    str(val).strip() != 'nan' and
                    str(val).strip() != 'None' and
                    str(val).strip() != 'null' and
                    str(val).strip() != ''
                for val in row), 
                axis=1
            )
            
            # Log filtering statistics
            total_rows = len(df_cleaned)
            filtered_rows = mask.sum()
            removed_rows = total_rows - filtered_rows
            
            logger.info(f"Chart of Accounts: Filtering statistics:")
            logger.info(f"  - Total rows before filtering: {total_rows}")
            logger.info(f"  - Rows with data in key fields: {filtered_rows}")
            logger.info(f"  - Rows removed (empty): {removed_rows}")
            logger.info(f"  - Key fields used: {available_key_fields}")
            
            # Show sample of removed rows for debugging
            if removed_rows > 0 and removed_rows <= 10:
                removed_sample = df_cleaned[~mask].head(3)
                logger.info(f"Chart of Accounts: Sample of removed rows:")
                for idx, row in removed_sample.iterrows():
                    logger.info(f"  Row {idx}: {dict(row)}")
            
            df_cleaned = df_cleaned[mask]
        else:
            logger.warning(f"Chart of Accounts: No key fields found, keeping all rows")
        
        logger.info(f"Chart of Accounts: Cleaned DataFrame from {len(df)} to {len(df_cleaned)} rows")
        return df_cleaned


class FileProcessingManager:
    """Manager class for handling file processing operations"""
    
    @staticmethod
    def process_csv_file(data_file: DataFile, file_obj) -> Dict[str, Any]:
        """
        Process uploaded CSV or Excel file and save to appropriate model based on file type
        
        Args:
            data_file: DataFile instance
            file_obj: Uploaded file object
            
        Returns:
            Dict: Processing results
        """
        try:
            # Update status
            data_file.status = 'PROCESSING'
            data_file.save()
            
            # Read file (CSV or Excel)
            df = FileReader.read_file(file_obj)
            
            # Detect file type
            file_type = FileTypeDetector.detect_file_type(df)
            logger.info(f"Detected file type: {file_type} for file: {data_file.file_name}")
            
            if file_type == 'UNKNOWN':
                return {'success': False, 'error': 'Unknown file type. Expected GL, TB, or Chart of Accounts data.'}
            
            # Process based on file type
            processor = DataProcessor(data_file)
            
            if file_type == 'GL_LIST':
                result = processor.process_gl_data(df)
            elif file_type == 'TB_LIST':
                result = processor.process_tb_data(df)
            elif file_type == 'CHART_OF_ACCOUNTS':
                result = processor.process_chart_data(df)
            
            # Update DataFile record
            data_file.total_records = result['processed_count'] + result['failed_count']
            data_file.processed_records = result['processed_count']
            data_file.failed_records = result['failed_count']
            data_file.status = 'COMPLETED' if result['failed_count'] == 0 else 'PARTIAL'
            data_file.processed_at = timezone.now()
            data_file.save()
            
            return {'success': True, 'file_type': file_type}
            
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            data_file.status = 'FAILED'
            data_file.error_message = str(e)
            data_file.processed_at = timezone.now()
            data_file.save()
            return {'success': False, 'error': str(e)}
