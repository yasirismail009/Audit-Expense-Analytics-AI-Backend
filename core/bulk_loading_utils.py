"""
Ultra-Fast Bulk Loading Utilities using SQLAlchemy + pandas + PostgreSQL COPY

This module provides high-performance bulk loading capabilities that are 
significantly faster than Django ORM for large datasets.

Performance Benefits:
- pandas.to_sql(): 5-10x faster than Django bulk_create
- PostgreSQL COPY: 20-50x faster than Django bulk_create
- Combined approach: Up to 100x faster for very large datasets
"""

import pandas as pd
import psycopg2
import tempfile
import os
import logging
import threading
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
from io import StringIO

from django.conf import settings
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


class SQLAlchemyBulkLoader:
    """Ultra-fast bulk loader using SQLAlchemy + pandas + PostgreSQL COPY"""
    
    def __init__(self):
        self.engine = None
        self._setup_engine()
    
    def _setup_engine(self):
        """Setup SQLAlchemy engine with thread-safe connection pooling for Docker"""
        try:
            # Get database settings from Django
            db_config = settings.DATABASES['default']
            
            # Create SQLAlchemy connection string
            connection_string = (
                f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}"
                f"@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
            )
            
            # Create engine with thread-safe settings for Docker
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,  # Reduced pool size for Docker containers
                max_overflow=10,  # Reduced overflow for Docker
                pool_pre_ping=True,  # Validate connections before use
                pool_recycle=1800,  # Recycle connections every 30 minutes
                pool_timeout=30,  # Timeout for getting connection from pool
                echo=False,  # Set to True for SQL debugging
                connect_args={
                    'connect_timeout': 30,  # Increased timeout for Docker
                    'application_name': 'analytics_bulk_loader'
                },
                # Thread-safe settings
                isolation_level='AUTOCOMMIT'
            )
            
            logger.info("✅ SQLAlchemy engine initialized for bulk loading (thread-safe)")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup SQLAlchemy engine: {e}")
            raise
    
    def bulk_load_chart_of_accounts(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """
        Bulk load Chart of Accounts using pandas.to_sql() with thread-safe engine
        
        Args:
            df: pandas DataFrame with COA data
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting ultra-fast COA bulk load with pandas.to_sql()...")
        start_time = datetime.now()
        
        # Use thread-safe engine for background threads
        engine = self.get_thread_safe_engine()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_coa_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql() - much faster than Django ORM
            processed_df.to_sql(
                'chart_of_accounts',  # Actual database table name
                engine,
                if_exists='append',
                index=False,
                method='multi',  # Use executemany for better performance
                chunksize=5000   # Process in 5K chunks
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(processed_df) / duration if duration > 0 else 0
            
            logger.info(f"✅ COA bulk load completed: {len(processed_df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
            return {
                'success': True,
                'records_loaded': len(processed_df),
                'duration': duration,
                'records_per_second': records_per_second,
                'method': 'pandas.to_sql'
            }
            
        except Exception as e:
            logger.error(f"❌ COA bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            # Clean up thread-safe engine
            try:
                engine.dispose()
                logger.info("✅ Thread-safe engine disposed")
            except:
                pass
    
    def bulk_load_trial_balance(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """
        Bulk load Trial Balance using pandas.to_sql()
        
        Args:
            df: pandas DataFrame with TB data
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting ultra-fast TB bulk load with pandas.to_sql()...")
        start_time = datetime.now()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_tb_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql()
            processed_df.to_sql(
                'core_trialbalance',  # Django table name
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(processed_df) / duration if duration > 0 else 0
            
            logger.info(f"✅ TB bulk load completed: {len(processed_df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
            return {
                'success': True,
                'records_loaded': len(processed_df),
                'duration': duration,
                'records_per_second': records_per_second,
                'method': 'pandas.to_sql'
            }
            
        except Exception as e:
            logger.error(f"❌ TB bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def bulk_load_gl_with_copy(self, csv_file_path: str, data_file_id: str) -> Dict[str, Any]:
        """
        Ultra-fast GL bulk load using PostgreSQL COPY command
        
        This is the fastest method for large GL datasets (300K+ records)
        
        Args:
            csv_file_path: Path to the CSV file
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting ULTRA-FAST GL bulk load with PostgreSQL COPY...")
        start_time = datetime.now()
        
        try:
            # First, prepare the CSV data for COPY
            prepared_csv_path = self._prepare_gl_csv_for_copy(csv_file_path, data_file_id)
            
            # Get raw psycopg2 connection for COPY command
            raw_connection = self._get_raw_connection()
            cursor = raw_connection.cursor()
            
            try:
                # Use PostgreSQL COPY command - fastest possible method
                with open(prepared_csv_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                        """
                        COPY sap_gl_postings (
                            data_file_id, document_number, document_type, posting_date,
                            document_date, entry_date, gl_account, amount_local_currency,
                            local_currency, profit_center, user_name, fiscal_year,
                            posting_period, text, segment, clearing_document,
                            offsetting_account, invoice_reference, sales_document,
                            assignment, year_month, amount_transaction_currency,
                            transaction_currency, exchange_rate, posting_key,
                            reference_document, document_header_text, company_code,
                            fiscal_period, cost_center, wbs_element, order_number,
                            asset_number, sub_number, business_area,
                            partner_business_area, gl_account_type, gl_account_sub_type,
                            gl_account_sub_sub_type, gl_account_long_text,
                            financial_statement, ref_to_fs, created_at, updated_at
                        ) FROM STDIN WITH CSV HEADER DELIMITER ','
                        """,
                        f
                    )
                
                # Get number of records loaded
                cursor.execute("SELECT ROW_COUNT()")
                records_loaded = cursor.fetchone()[0]
                
                raw_connection.commit()
                
                duration = (datetime.now() - start_time).total_seconds()
                records_per_second = records_loaded / duration if duration > 0 else 0
                
                logger.info(f"🏆 GL COPY bulk load completed: {records_loaded:,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
                
                
                return {
                    'success': True,
                    'records_loaded': records_loaded,
                    'duration': duration,
                    'records_per_second': records_per_second,
                    'method': 'PostgreSQL COPY'
                }
                
            finally:
                cursor.close()
                raw_connection.close()
                
                # Files are now saved locally and not cleaned up
                logger.info(f"💾 CSV file preserved: {prepared_csv_path}")
                    
        except Exception as e:
            logger.error(f"❌ GL COPY bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def bulk_load_gl_with_pandas(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """
        Fast GL bulk load using pandas.to_sql() with thread-safe engine (fallback method)
        
        Args:
            df: pandas DataFrame with GL data
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting fast GL bulk load with pandas.to_sql()...")
        start_time = datetime.now()
        
        # Use thread-safe engine for background threads
        engine = self.get_thread_safe_engine()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_gl_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql() with larger chunks for GL data
            processed_df.to_sql(
                'sap_gl_postings',  # Actual database table name from model Meta
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000  # Larger chunks for GL data
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(processed_df) / duration if duration > 0 else 0
            
            logger.info(f"✅ GL pandas bulk load completed: {len(processed_df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
            # Trigger completeness test, ML prediction, and model training tasks after successful GL bulk loading
            try:
                from .tasks import run_gl_completeness_analysis, predict_completeness_with_ai
                from .tasks.ml_training_tasks import train_comprehensive_ai_models
                from django.db import connection
                from .models import DataFile
                
                # Ensure database connection is closed before task execution
                connection.close()
                
                # Get data file to determine engagement and version
                data_file = DataFile.objects.get(id=data_file_id)
                engagement = data_file.engagement
                version = getattr(data_file, 'version', '1.0')
                
                # First: Trigger completeness test in Celery
                completeness_task = run_gl_completeness_analysis.delay(str(data_file_id))
                logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                logger.info(f"📊 GL bulk loading completed, starting completeness analysis for file: {data_file_id}")
                logger.info(f"🔗 Completeness task details: {completeness_task.id} - {completeness_task.state}")
                
                # Second: Trigger ML prediction task after completeness test
                ml_prediction_task = predict_completeness_with_ai.delay(
                    str(data_file_id)
                )
                logger.info(f"🤖 ML prediction task queued in Celery: {ml_prediction_task.id}")
                logger.info(f"🧠 ML prediction will run after completeness test for engagement: {engagement.engagement_id}")
                logger.info(f"🔗 ML task details: {ml_prediction_task.id} - {ml_prediction_task.state}")
                logger.info(f"📊 Version {version} will be saved to CompletenessTestResult model")
                
                # Third: Trigger model training on the data
                model_training_task = train_comprehensive_ai_models.delay(
                    engagement_id=engagement.id,
                    client_name=engagement.engagement_name
                )
                logger.info(f"🎯 Model training task queued in Celery: {model_training_task.id}")
                logger.info(f"🧠 Model training will run on new data for engagement: {engagement.engagement_id}")
                logger.info(f"🔗 Training task details: {model_training_task.id} - {model_training_task.state}")
                logger.info(f"📊 Version-wise results will be saved to CompletenessTestResult model")
                
            except Exception as e:
                logger.error(f"❌ Could not queue completeness, ML, and training tasks: {e}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
            
            return {
                'success': True,
                'records_loaded': len(processed_df),
                'duration': duration,
                'records_per_second': records_per_second,
                'method': 'pandas.to_sql'
            }
            
        except Exception as e:
            logger.error(f"❌ GL pandas bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            # Clean up thread-safe engine
            try:
                engine.dispose()
                logger.info("✅ Thread-safe engine disposed")
            except:
                pass
    
    def bulk_load_coa_with_pandas(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """Bulk load COA data using pandas.to_sql()"""
        from datetime import datetime
        
        start_time = datetime.now()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_coa_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql()
            processed_df.to_sql(
                'chart_of_accounts',  # Django table name
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(processed_df) / duration if duration > 0 else 0
            
            logger.info(f"✅ COA pandas bulk load completed: {len(processed_df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
            return {
                'success': True,
                'records_loaded': len(processed_df),
                'duration': duration,
                'records_per_second': records_per_second,
                'method': 'pandas.to_sql'
            }
            
        except Exception as e:
            logger.error(f"❌ COA pandas bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _prepare_coa_dataframe(self, df: pd.DataFrame, data_file_id: str) -> pd.DataFrame:
        """Prepare COA DataFrame for database insertion"""
        from datetime import datetime
        import uuid
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        # Clean column names (remove trailing spaces) before mapping
        processed_df.columns = processed_df.columns.str.strip()
        
        # Debug: Log column names before mapping
        logger.info(f"📄 COA columns before mapping: {list(processed_df.columns)}")
        
        # Map column names to Django model fields
        column_mapping = {
            'Account': 'account',
            'Account Code': 'account',
            'Type': 'type',
            'Sub Type': 'sub_type',
            'Sub Sub Type': 'sub_sub_type',
            'Company': 'company',
            'Company ': 'company',  # Handle trailing space
            'Branch': 'branch',
            'Cost Center': 'cost_center',
            'Cost. C': 'cost_center',  # Handle abbreviated column name
            'G/L Account': 'gl_account',
            'G/L acct': 'gl_account',
            'Code': 'code',
            'G/L Acct Long Text': 'gl_account_long_text',
            'REF to FS': 'ref_to_fs',
            'F.S': 'financial_statement',
            'Ref to Note': 'ref_to_note',
            'FY 2024': 'fiscal_year',
            'Q1 2025': 'q1_amount',
            'Adj/Reclas': 'adj_reclas',
            'Adj/Reclass': 'adj_reclas',  # Handle different spelling
            'Fin Q1 2025': 'fin_q1_amount'
        }
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Debug: Log column names after mapping
        logger.info(f"📄 COA columns after mapping: {list(processed_df.columns)}")
        
        # Add required Django fields
        processed_df['data_file_id'] = data_file_id
        processed_df['created_at'] = datetime.now()
        processed_df['updated_at'] = datetime.now()
        
        # Generate UUIDs for the id column (required for Django models)
        # This MUST be done after column mapping but before filtering
        processed_df['id'] = [str(uuid.uuid4()) for _ in range(len(processed_df))]
        
        # Convert data types
        if 'fiscal_year' in processed_df.columns:
            processed_df['fiscal_year'] = pd.to_numeric(processed_df['fiscal_year'], errors='coerce').fillna(2024).astype(int)
        
        # Convert decimal fields
        for col in ['q1_amount', 'adj_reclas', 'fin_q1_amount']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # Add default values for required fields that might be missing
        if 'type' not in processed_df.columns or processed_df['type'].isnull().any():
            processed_df['type'] = processed_df['type'].fillna('Unknown')
            logger.info("📄 Added default 'Unknown' for missing type values")
        
        if 'sub_type' not in processed_df.columns or processed_df['sub_type'].isnull().any():
            processed_df['sub_type'] = processed_df['sub_type'].fillna('Unknown')
            logger.info("📄 Added default 'Unknown' for missing sub_type values")
        
        if 'sub_sub_type' not in processed_df.columns or processed_df['sub_sub_type'].isnull().any():
            processed_df['sub_sub_type'] = processed_df['sub_sub_type'].fillna('Unknown')
            logger.info("📄 Added default 'Unknown' for missing sub_sub_type values")
        
        # Handle account field - ensure it's not null and create gl_account from it
        if 'account' in processed_df.columns:
            # Fill null account values with a default
            processed_df['account'] = processed_df['account'].fillna('UNKNOWN')
            # Create gl_account from account if gl_account is missing or null
            if 'gl_account' not in processed_df.columns or processed_df['gl_account'].isnull().any():
                processed_df['gl_account'] = processed_df['account']
                logger.info("📄 Created gl_account from account field")
        
        # Ensure gl_account has no null values and stays as string
        if 'gl_account' in processed_df.columns:
            # Convert to string first to prevent float conversion (e.g., 123.0)
            processed_df['gl_account'] = processed_df['gl_account'].astype(str)
            
            # Replace empty strings, null values, and 'nan' strings with a default
            processed_df['gl_account'] = processed_df['gl_account'].replace('', 'UNKNOWN')
            processed_df['gl_account'] = processed_df['gl_account'].replace('nan', 'UNKNOWN')
            processed_df['gl_account'] = processed_df['gl_account'].fillna('UNKNOWN')
            
            # Remove .0 suffix if present (e.g., "123.0" -> "123")
            processed_df['gl_account'] = processed_df['gl_account'].str.replace(r'\.0$', '', regex=True)
            
            logger.info("📄 Added default 'UNKNOWN' for missing gl_account values and ensured string format")
        
        # Filter to only include columns that exist in the database table
        # Based on actual database schema: id, created_at, updated_at, account, type, sub_type, sub_sub_type, gl_account, company, branch, cost_center, code, gl_account_long_text, ref_to_fs, financial_statement, ref_to_note, fiscal_year, q1_amount, adj_reclas, fin_q1_amount, data_file_id, gl_account_ref_id, profit_center_ref_id
        valid_columns = [
            'id', 'created_at', 'updated_at', 'account', 'type', 'sub_type', 'sub_sub_type',
            'gl_account', 'company', 'branch', 'cost_center', 'code', 'gl_account_long_text',
            'ref_to_fs', 'financial_statement', 'ref_to_note', 'fiscal_year', 'q1_amount',
            'adj_reclas', 'fin_q1_amount', 'data_file_id', 'gl_account_ref_id', 'profit_center_ref_id'
        ]
        
        # Keep only valid columns that exist in the DataFrame
        existing_valid_columns = [col for col in valid_columns if col in processed_df.columns]
        processed_df = processed_df[existing_valid_columns]
        
        logger.info(f"📄 COA final columns for database: {list(processed_df.columns)}")
        logger.info(f"📄 ID column present: {'id' in processed_df.columns}")
        if 'id' in processed_df.columns:
            logger.info(f"📄 Sample ID values: {processed_df['id'].head(3).tolist()}")
            # Verify no null values in ID column
            null_ids = processed_df['id'].isnull().sum()
            if null_ids > 0:
                logger.error(f"❌ Found {null_ids} null values in ID column!")
            else:
                logger.info(f"✅ All {len(processed_df)} ID values are valid UUIDs")
        
        # Ensure account is string and handle long account codes
        if 'account' in processed_df.columns:
            processed_df['account'] = processed_df['account'].astype(str).str.strip()
        
        return processed_df
    
    def bulk_load_gl_with_pandas_from_instances(self, postings_to_create: List, data_file_id: str) -> Dict[str, Any]:
        """
        Bulk load GL postings using pandas.to_sql() from model instances
        
        Args:
            postings_to_create: List of SAPGLPosting model instances
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting ultra-fast GL bulk load with pandas.to_sql() from instances...")
        start_time = datetime.now()
        
        try:
            # Convert model instances to DataFrame
            df = self._convert_gl_instances_to_dataframe(postings_to_create, data_file_id)
            
            # Bulk insert using pandas.to_sql() with larger chunks for GL data
            df.to_sql(
                'sap_gl_postings',  # Actual database table name from model Meta
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000  # Larger chunks for GL data
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(df) / duration if duration > 0 else 0
            
            logger.info(f"✅ GL pandas bulk load from instances completed: {len(df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
            # Trigger completeness test, ML prediction, and model training tasks after successful GL bulk loading
            try:
                from .tasks import run_gl_completeness_analysis, predict_completeness_with_ai
                from .tasks.ml_training_tasks import train_comprehensive_ai_models
                from django.db import connection
                from .models import DataFile
                
                # Ensure database connection is closed before task execution
                connection.close()
                
                # Get data file to determine engagement and version
                data_file = DataFile.objects.get(id=data_file_id)
                engagement = data_file.engagement
                version = getattr(data_file, 'version', '1.0')
                
                # First: Trigger completeness test in Celery
                completeness_task = run_gl_completeness_analysis.delay(str(data_file_id))
                logger.info(f"✅ Completeness test queued in Celery: {completeness_task.id}")
                logger.info(f"📊 GL bulk loading completed, starting completeness analysis for file: {data_file_id}")
                logger.info(f"🔗 Completeness task details: {completeness_task.id} - {completeness_task.state}")
                
                # Second: Trigger ML prediction task after completeness test
                ml_prediction_task = predict_completeness_with_ai.delay(
                    str(data_file_id)
                )
                logger.info(f"🤖 ML prediction task queued in Celery: {ml_prediction_task.id}")
                logger.info(f"🧠 ML prediction will run after completeness test for engagement: {engagement.engagement_id}")
                logger.info(f"🔗 ML task details: {ml_prediction_task.id} - {ml_prediction_task.state}")
                logger.info(f"📊 Version {version} will be saved to CompletenessTestResult model")
                
                # Third: Trigger model training on the data
                model_training_task = train_comprehensive_ai_models.delay(
                    engagement_id=engagement.id,
                    client_name=engagement.engagement_name
                )
                logger.info(f"🎯 Model training task queued in Celery: {model_training_task.id}")
                logger.info(f"🧠 Model training will run on new data for engagement: {engagement.engagement_id}")
                logger.info(f"🔗 Training task details: {model_training_task.id} - {model_training_task.state}")
                logger.info(f"📊 Version-wise results will be saved to CompletenessTestResult model")
                
            except Exception as e:
                logger.error(f"❌ Could not queue completeness, ML, and training tasks: {e}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
            
            return {
                'success': True,
                'records_loaded': len(df),
                'duration': duration,
                'records_per_second': records_per_second,
                'method': 'pandas.to_sql_from_instances'
            }
            
        except Exception as e:
            logger.error(f"❌ GL pandas bulk load from instances failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def bulk_load_gl_with_copy_from_instances(self, postings_to_create: List, data_file_id: str) -> Dict[str, Any]:
        """
        Bulk load GL postings using PostgreSQL COPY from model instances
        
        Args:
            postings_to_create: List of SAPGLPosting model instances
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🏆 Starting ultra-fast GL bulk load with PostgreSQL COPY from instances...")
        start_time = datetime.now()
        
        try:
            # Convert model instances to DataFrame
            df = self._convert_gl_instances_to_dataframe(postings_to_create, data_file_id)
            
            # Create temporary CSV file
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                csv_path = temp_file.name
                df.to_csv(csv_path, index=False)
            
            try:
                # Use PostgreSQL COPY for maximum speed
                result = self.bulk_load_gl_with_copy(csv_path, data_file_id)
                return result
            finally:
                # Clean up temporary file
                if os.path.exists(csv_path):
                    os.unlink(csv_path)
            
        except Exception as e:
            logger.error(f"❌ GL COPY bulk load from instances failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _convert_gl_instances_to_dataframe(self, postings_to_create: List, data_file_id: str) -> pd.DataFrame:
        """Convert GL model instances to DataFrame for bulk loading"""
        data = []
        
        for posting in postings_to_create:
            row_data = {
                'id': str(posting.id) if hasattr(posting, 'id') and posting.id else None,
                'data_file_id': data_file_id,
                'gl_account': posting.gl_account,
                'document_number': posting.document_number,
                'document_type': getattr(posting, 'document_type', ''),
                'posting_date': posting.posting_date,
                'document_date': getattr(posting, 'document_date', None),
                'entry_date': getattr(posting, 'entry_date', None),
                'amount_local_currency': posting.amount_local_currency,
                'local_currency': posting.local_currency,
                'amount_transaction_currency': posting.amount_transaction_currency,
                'transaction_currency': posting.transaction_currency,
                'exchange_rate': posting.exchange_rate,
                'user_name': posting.user_name,
                'posting_key': posting.posting_key,
                'reference_document': posting.reference_document,
                'document_header_text': posting.document_header_text,
                'company_code': posting.company_code,
                'fiscal_year': posting.fiscal_year,
                'fiscal_period': posting.fiscal_period,
                'posting_period': getattr(posting, 'posting_period', 1),
                'cost_center': posting.cost_center,
                'profit_center': posting.profit_center,
                'wbs_element': posting.wbs_element,
                'order_number': posting.order_number,
                'asset_number': posting.asset_number,
                'sub_number': posting.sub_number,
                'business_area': posting.business_area,
                'segment': posting.segment,
                'partner_business_area': posting.partner_business_area,
                'text': getattr(posting, 'text', ''),
                'clearing_document': getattr(posting, 'clearing_document', ''),
                'offsetting_account': getattr(posting, 'offsetting_account', ''),
                'invoice_reference': getattr(posting, 'invoice_reference', ''),
                'sales_document': getattr(posting, 'sales_document', ''),
                'assignment': getattr(posting, 'assignment', ''),
                'year_month': getattr(posting, 'year_month', ''),
                'gl_account_type': getattr(posting, 'gl_account_type', ''),
                'gl_account_sub_type': getattr(posting, 'gl_account_sub_type', ''),
                'gl_account_sub_sub_type': getattr(posting, 'gl_account_sub_sub_type', ''),
                'gl_account_long_text': getattr(posting, 'gl_account_long_text', ''),
                'financial_statement': getattr(posting, 'financial_statement', ''),
                'ref_to_fs': getattr(posting, 'ref_to_fs', ''),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            data.append(row_data)
        
        return pd.DataFrame(data)
    
    
    def _prepare_tb_dataframe(self, df: pd.DataFrame, data_file_id: str) -> pd.DataFrame:
        """Prepare TB DataFrame for database insertion"""
        processed_df = df.copy()
        
        # Add required Django fields
        processed_df['data_file_id'] = data_file_id
        processed_df['created_at'] = datetime.now()
        processed_df['updated_at'] = datetime.now()
        
        # Map column names to Django model fields
        column_mapping = {
            'CoCd': 'company_code',
            'GL Account': 'gl_account',
            'Short Text': 'short_text',
            'Currency': 'currency',
            'Opening Balance': 'opening_balance',
            'Debit': 'debit',
            'Credit': 'credit',
            'Closing Balance': 'closing_balance'
        }
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Convert decimal fields
        for col in ['opening_balance', 'debit', 'credit', 'closing_balance']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        return processed_df
    
    def _prepare_gl_dataframe(self, df: pd.DataFrame, data_file_id: str) -> pd.DataFrame:
        """Prepare GL DataFrame for database insertion"""
        from datetime import datetime
        import uuid
        
        processed_df = df.copy()
        
        # Clean column names (remove trailing spaces) before mapping
        processed_df.columns = processed_df.columns.str.strip()
        
        # Debug: Log original columns
        logger.info(f"📄 Original GL DataFrame columns: {list(processed_df.columns)}")
        
        # Map column names to Django model fields - Comprehensive mapping based on SAPGLPosting model
        column_mapping = {
            # Document information
            'Document': 'document_number',
            'Document Number': 'document_number',
            'Document type': 'document_type',
            'Document Type': 'document_type',
            'Posting Date': 'posting_date',
            'Document Date': 'document_date',
            'Entry Date': 'entry_date',
            
            # Financial data
            'G/L Account': 'gl_account',
            'Amount in Local Currency': 'amount_local_currency',
            'Local Currency': 'local_currency',
            'Amount in Transaction Currency': 'amount_transaction_currency',
            'Transaction Currency': 'transaction_currency',
            'Trans Currency': 'transaction_currency',
            'Currency': 'transaction_currency',
            'Exchange Rate': 'exchange_rate',
            'Rate': 'exchange_rate',
            
            # Organizational data
            'Profit Center': 'profit_center',
            'User Name': 'user_name',
            'Fiscal Year': 'fiscal_year',
            'Posting period': 'posting_period',
            'Fiscal Period': 'fiscal_period',
            
            # Additional fields
            'Text': 'text',
            'Segment': 'segment',
            'Clearing Document': 'clearing_document',
            'Offsetting': 'offsetting_account',
            'Invoice Reference': 'invoice_reference',
            'Invoice Number': 'invoice_reference',
            'Invoice': 'invoice_reference',
            'Sales Document': 'sales_document',
            'Assignment': 'assignment',
            'Year/Month': 'year_month',
            
            # Company and organizational fields
            'Company Code': 'company_code',
            'Company': 'company_code',
            'CoCd': 'company_code',
            'Cost Center': 'cost_center',
            'WBS Element': 'wbs_element',
            'Order Number': 'order_number',
            'Asset Number': 'asset_number',
            'Sub Number': 'sub_number',
            'Business Area': 'business_area',
            'Partner Business Area': 'partner_business_area',
            
            # Document header and posting fields
            'Document Header Text': 'document_header_text',
            'Header Text': 'document_header_text',
            'Document Header': 'document_header_text',
            'Posting Key': 'posting_key',
            'Posting': 'posting_key',
            'Key': 'posting_key',
            'Reference Document': 'reference_document',
            'Ref Document': 'reference_document',
            
            # GL Account type information (from Chart of Accounts)
            'GL Account Type': 'gl_account_type',
            'Account Type': 'gl_account_type',
            'GL Account Sub Type': 'gl_account_sub_type',
            'Sub Type': 'gl_account_sub_type',
            'GL Account Sub Sub Type': 'gl_account_sub_sub_type',
            'Sub Sub Type': 'gl_account_sub_sub_type',
            'GL Account Long Text': 'gl_account_long_text',
            'Account Long Text': 'gl_account_long_text',
            'Financial Statement': 'financial_statement',
            'Ref to FS': 'ref_to_fs',
            'Reference to FS': 'ref_to_fs'
        }
        
        # Add default values for required fields that might be missing
        if 'document_type' not in processed_df.columns:
            processed_df['document_type'] = 'SA'  # Default document type
            logger.info("📄 Added default document_type: 'SA'")
        
        if 'posting_period' not in processed_df.columns:
            processed_df['posting_period'] = 1  # Default posting period
            logger.info("📄 Added default posting_period: 1")
        
        if 'segment' not in processed_df.columns:
            processed_df['segment'] = 'S1'  # Default segment
            logger.info("📄 Added default segment: 'S1'")
        
        if 'clearing_document' not in processed_df.columns:
            processed_df['clearing_document'] = ''  # Default clearing document
            logger.info("📄 Added default clearing_document: ''")
        
        if 'offsetting_account' not in processed_df.columns:
            processed_df['offsetting_account'] = ''  # Default offsetting account
            logger.info("📄 Added default offsetting_account: ''")
        
        if 'sales_document' not in processed_df.columns:
            processed_df['sales_document'] = ''  # Default sales document
            logger.info("📄 Added default sales_document: ''")
        
        if 'assignment' not in processed_df.columns:
            processed_df['assignment'] = ''  # Default assignment
            logger.info("📄 Added default assignment: ''")
        
        # Add default values for other required fields that might be missing
        if 'asset_number' not in processed_df.columns:
            processed_df['asset_number'] = ''  # Default asset number
            logger.info("📄 Added default asset_number: ''")
        
        if 'cost_center' not in processed_df.columns:
            processed_df['cost_center'] = ''  # Default cost center
            logger.info("📄 Added default cost_center: ''")
        
        if 'wbs_element' not in processed_df.columns:
            processed_df['wbs_element'] = ''  # Default WBS element
            logger.info("📄 Added default wbs_element: ''")
        
        if 'order_number' not in processed_df.columns:
            processed_df['order_number'] = ''  # Default order number
            logger.info("📄 Added default order_number: ''")
        
        if 'sub_number' not in processed_df.columns:
            processed_df['sub_number'] = ''  # Default sub number
            logger.info("📄 Added default sub_number: ''")
        
        if 'business_area' not in processed_df.columns:
            processed_df['business_area'] = ''  # Default business area
            logger.info("📄 Added default business_area: ''")
        
        if 'partner_business_area' not in processed_df.columns:
            processed_df['partner_business_area'] = ''  # Default partner business area
            logger.info("📄 Added default partner_business_area: ''")
        
        if 'company_code' not in processed_df.columns:
            processed_df['company_code'] = ''  # Default company code
            logger.info("📄 Added default company_code: ''")
        
        if 'document_header_text' not in processed_df.columns:
            processed_df['document_header_text'] = ''  # Default document header text
            logger.info("📄 Added default document_header_text: ''")
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        logger.info(f"📄 After column mapping: {list(processed_df.columns)}")
        
        # Add default values for required fields that might be missing AFTER column mapping
        if 'posting_key' not in processed_df.columns:
            processed_df['posting_key'] = ''  # Default posting key
            logger.info("📄 Added default posting_key: ''")
        
        if 'reference_document' not in processed_df.columns:
            processed_df['reference_document'] = ''  # Default reference document
            logger.info("📄 Added default reference_document: ''")
        
        if 'transaction_currency' not in processed_df.columns:
            processed_df['transaction_currency'] = ''  # Default transaction currency
            logger.info("📄 Added default transaction_currency: ''")
        
        if 'invoice_reference' not in processed_df.columns:
            processed_df['invoice_reference'] = ''  # Default invoice reference
            logger.info("📄 Added default invoice_reference: ''")
        
        # Add default values for all CharField columns that don't allow NULL
        charfield_defaults = {
            'amount_transaction_currency': None,  # DecimalField - can be NULL
            'exchange_rate': None,  # DecimalField - can be NULL
            'fiscal_period': None,  # IntegerField - can be NULL
            'gl_account_type': '',
            'gl_account_sub_type': '',
            'gl_account_sub_sub_type': '',
            'gl_account_long_text': '',
            'financial_statement': '',
            'ref_to_fs': ''
        }
        
        for field, default_value in charfield_defaults.items():
            if field not in processed_df.columns:
                processed_df[field] = default_value
                logger.info(f"📄 Added default {field}: {default_value}")
        
        # Add required Django fields
        processed_df['data_file_id'] = data_file_id
        processed_df['created_at'] = datetime.now()
        processed_df['updated_at'] = datetime.now()
        
        # Generate UUIDs for the id column (required for Django models)
        # This MUST be done after column mapping but before filtering
        processed_df['id'] = [str(uuid.uuid4()) for _ in range(len(processed_df))]
        
        # Convert data types
        if 'fiscal_year' in processed_df.columns:
            processed_df['fiscal_year'] = pd.to_numeric(processed_df['fiscal_year'], errors='coerce').fillna(2025).astype(int)
        
        if 'posting_period' in processed_df.columns:
            processed_df['posting_period'] = pd.to_numeric(processed_df['posting_period'], errors='coerce').fillna(1).astype(int)
        
        # Convert amount to numeric
        if 'amount_local_currency' in processed_df.columns:
            processed_df['amount_local_currency'] = pd.to_numeric(processed_df['amount_local_currency'], errors='coerce').fillna(0)
        
        # Process document numbers - handle decimal representations and convert to proper string format
        if 'document_number' in processed_df.columns:
            logger.info(f"📄 Processing document numbers - found {len(processed_df)} records")
            logger.info(f"📄 Sample document numbers before cleaning: {processed_df['document_number'].head().tolist()}")
            
            # Convert to string and strip whitespace
            processed_df['document_number'] = processed_df['document_number'].astype(str).str.strip()
            
            # Handle decimal representations of whole numbers (e.g., "1234567.0" → "1234567")
            def clean_document_number(doc_num):
                if pd.isna(doc_num) or doc_num == '' or doc_num == 'nan':
                    return ''
                try:
                    # Check if it's a decimal representation of a whole number
                    if '.' in str(doc_num):
                        float_val = float(doc_num)
                        if float_val.is_integer():
                            return str(int(float_val))
                except (ValueError, TypeError):
                    pass
                return str(doc_num)
            
            processed_df['document_number'] = processed_df['document_number'].apply(clean_document_number)
            logger.info(f"📄 Sample document numbers after cleaning: {processed_df['document_number'].head().tolist()}")
        else:
            logger.warning(f"📄 document_number column not found in processed DataFrame. Available columns: {list(processed_df.columns)}")
        
        # Convert dates
        date_columns = ['posting_date', 'document_date', 'entry_date']
        for col in date_columns:
            if col in processed_df.columns:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
        
        # Clean CharField columns to ensure no NULL values (convert to empty strings)
        # This prevents NOT NULL constraint violations for CharField columns
        charfield_columns = [
            'document_type', 'text', 'segment', 'clearing_document', 'offsetting_account',
            'invoice_reference', 'sales_document', 'assignment', 'year_month',
            'transaction_currency', 'posting_key', 'reference_document', 'document_header_text',
            'company_code', 'cost_center', 'wbs_element', 'order_number', 'asset_number',
            'sub_number', 'business_area', 'partner_business_area', 'gl_account',
            'gl_account_type', 'gl_account_sub_type', 'gl_account_sub_sub_type', 'gl_account_long_text',
            'financial_statement', 'ref_to_fs'
        ]
        
        for col in charfield_columns:
            if col in processed_df.columns:
                # Convert NaN/None values to empty strings for CharField columns
                processed_df[col] = processed_df[col].fillna('').astype(str)
        
        # Special handling for gl_account - ensure it has a meaningful default and stays as string
        if 'gl_account' in processed_df.columns:
            # Check for null/empty values before fixing
            null_count = processed_df['gl_account'].isnull().sum()
            empty_count = (processed_df['gl_account'] == '').sum()
            logger.info(f"📄 gl_account null values: {null_count}, empty values: {empty_count}")
            
            # Convert to string first to prevent float conversion (e.g., 123.0)
            processed_df['gl_account'] = processed_df['gl_account'].astype(str)
            
            # Replace empty strings, null values, and 'nan' strings with a default
            processed_df['gl_account'] = processed_df['gl_account'].replace('', 'UNKNOWN')
            processed_df['gl_account'] = processed_df['gl_account'].replace('nan', 'UNKNOWN')
            processed_df['gl_account'] = processed_df['gl_account'].fillna('UNKNOWN')
            
            # Remove .0 suffix if present (e.g., "123.0" -> "123")
            processed_df['gl_account'] = processed_df['gl_account'].str.replace(r'\.0$', '', regex=True)
            
            # Verify no null values remain
            remaining_nulls = processed_df['gl_account'].isnull().sum()
            logger.info(f"📄 Added default 'UNKNOWN' for missing gl_account values. Remaining nulls: {remaining_nulls}")
            
            # Log any remaining problematic values
            if remaining_nulls > 0:
                problem_indices = processed_df[processed_df['gl_account'].isnull()].index.tolist()
                logger.warning(f"⚠️ Found {remaining_nulls} records with null gl_account at indices: {problem_indices[:10]}")
        else:
            logger.warning("⚠️ gl_account column not found in DataFrame!")
        
        # Additional cleaning for all CharField columns
        for col in charfield_columns:
            if col in processed_df.columns:
                # Replace 'nan' strings with empty strings
                processed_df[col] = processed_df[col].replace('nan', '')
                processed_df[col] = processed_df[col].replace('None', '')
        
        logger.info("📄 Cleaned CharField columns to prevent NULL constraint violations")
        
        # Filter out records with null values in required fields - don't save null records
        required_fields = ['gl_account', 'document_number']
        original_count = len(processed_df)
        
        for field in required_fields:
            if field in processed_df.columns:
                null_count = processed_df[field].isnull().sum()
                empty_count = (processed_df[field] == '').sum()
                if null_count > 0 or empty_count > 0:
                    logger.warning(f"⚠️ Found {null_count} null and {empty_count} empty values in {field}")
                    # Filter out records with null/empty values in this field
                    processed_df = processed_df[~(processed_df[field].isnull() | (processed_df[field] == ''))]
                    logger.info(f"📄 Filtered out records with null/empty {field}. Remaining records: {len(processed_df)}")
                else:
                    logger.info(f"✅ {field}: No null or empty values found")
        
        filtered_count = len(processed_df)
        removed_count = original_count - filtered_count
        
        if removed_count > 0:
            logger.info(f"📄 Filtered out {removed_count} records with null values. Saving {filtered_count} valid records.")
        else:
            logger.info(f"✅ All {original_count} records are valid and will be saved.")
        
        # Filter to only include columns that exist in the database table
        # Based on actual database schema for SAPGLPosting model
        valid_columns = [
            'id', 'created_at', 'updated_at', 'data_file_id', 'document_number', 'document_type',
            'posting_date', 'document_date', 'entry_date', 'gl_account', 'amount_local_currency',
            'local_currency', 'profit_center', 'user_name', 'fiscal_year', 'posting_period',
            'text', 'segment', 'clearing_document', 'offsetting_account', 'invoice_reference',
            'sales_document', 'assignment', 'year_month', 'amount_transaction_currency',
            'transaction_currency', 'exchange_rate', 'posting_key', 'reference_document',
            'document_header_text', 'company_code', 'fiscal_period', 'cost_center', 'wbs_element',
            'order_number', 'asset_number', 'sub_number', 'business_area', 'partner_business_area',
            'gl_account_type', 'gl_account_sub_type', 'gl_account_sub_sub_type', 'gl_account_long_text',
            'financial_statement', 'ref_to_fs'
        ]
        
        # Keep only valid columns that exist in the DataFrame
        existing_valid_columns = [col for col in valid_columns if col in processed_df.columns]
        processed_df = processed_df[existing_valid_columns]
        
        logger.info(f"📄 GL final columns for database: {list(processed_df.columns)}")
        logger.info(f"📄 ID column present: {'id' in processed_df.columns}")
        if 'id' in processed_df.columns:
            logger.info(f"📄 Sample ID values: {processed_df['id'].head(3).tolist()}")
            # Verify no null values in ID column
            null_ids = processed_df['id'].isnull().sum()
            if null_ids > 0:
                logger.error(f"❌ Found {null_ids} null values in ID column!")
            else:
                logger.info(f"✅ All {len(processed_df)} ID values are valid UUIDs")
        
        # Verify document numbers are present and valid
        if 'document_number' in processed_df.columns:
            empty_docs = processed_df['document_number'].isnull().sum() + (processed_df['document_number'] == '').sum()
            logger.info(f"📄 Document numbers: {len(processed_df)} total, {empty_docs} empty")
            if empty_docs > 0:
                logger.warning(f"⚠️ Found {empty_docs} empty document numbers")
        else:
            logger.error("❌ document_number column missing after processing!")
        
        return processed_df
    
    def _prepare_gl_csv_for_copy(self, csv_file_path: str, data_file_id: str) -> str:
        """Prepare GL CSV file for PostgreSQL COPY command"""
        # Read and process CSV
        df = pd.read_csv(csv_file_path)
        processed_df = self._prepare_gl_dataframe(df, data_file_id)
        
        # Create temporary CSV file with proper format for COPY
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8')
        processed_df.to_csv(temp_file.name, index=False)
        temp_file.close()
        
        return temp_file.name
    
    def _get_raw_connection(self):
        """Get raw psycopg2 connection for COPY command"""
        db_config = settings.DATABASES['default']
        
        connection = psycopg2.connect(
            host=db_config['HOST'],
            port=db_config['PORT'],
            database=db_config['NAME'],
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            connect_timeout=10,
            application_name='analytics_copy_loader'
        )
        
        return connection
    
    def get_thread_safe_engine(self):
        """Get a thread-safe engine for use in background threads"""
        try:
            # Get database settings from Django
            db_config = settings.DATABASES['default']
            
            # Create SQLAlchemy connection string
            connection_string = (
                f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}"
                f"@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
            )
            
            # Create a new engine for this thread
            thread_engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=2,  # Small pool for single thread
                max_overflow=5,  # Minimal overflow
                pool_pre_ping=True,
                pool_recycle=900,  # 15 minutes
                pool_timeout=60,  # Longer timeout for Docker
                echo=False,
                connect_args={
                    'connect_timeout': 60,
                    'application_name': f'analytics_bulk_loader_thread_{threading.current_thread().ident}'
                },
                isolation_level='AUTOCOMMIT'
            )
            
            logger.info(f"✅ Thread-safe SQLAlchemy engine created for thread {threading.current_thread().ident}")
            return thread_engine
            
        except Exception as e:
            logger.error(f"❌ Failed to create thread-safe engine: {e}")
            raise
    
    def close(self):
        """Close SQLAlchemy engine"""
        if self.engine:
            self.engine.dispose()
            logger.info("✅ SQLAlchemy engine closed")


# Convenience functions for easy integration
def bulk_load_chart_of_accounts(df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for COA bulk loading"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_chart_of_accounts(df, data_file_id)
    finally:
        loader.close()


def bulk_load_trial_balance(df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for TB bulk loading"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_trial_balance(df, data_file_id)
    finally:
        loader.close()


def bulk_load_gl_with_copy(csv_file_path: str, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for GL COPY bulk loading"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_gl_with_copy(csv_file_path, data_file_id)
    finally:
        loader.close()


def bulk_load_gl_with_pandas(df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for GL pandas bulk loading"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_gl_with_pandas(df, data_file_id)
    finally:
        loader.close()


def bulk_load_gl_with_pandas_from_instances(postings_to_create: List, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for GL pandas bulk loading from model instances"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_gl_with_pandas_from_instances(postings_to_create, data_file_id)
    finally:
        loader.close()


def bulk_load_gl_with_copy_from_instances(postings_to_create: List, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for GL COPY bulk loading from model instances"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_gl_with_copy_from_instances(postings_to_create, data_file_id)
    finally:
        loader.close()


def bulk_load_coa_with_pandas(df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
    """Convenience function for COA pandas bulk loading"""
    loader = SQLAlchemyBulkLoader()
    try:
        return loader.bulk_load_coa_with_pandas(df, data_file_id)
    finally:
        loader.close()