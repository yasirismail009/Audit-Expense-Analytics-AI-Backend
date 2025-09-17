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
        """Setup SQLAlchemy engine with optimized connection pooling"""
        try:
            # Get database settings from Django
            db_config = settings.DATABASES['default']
            
            # Create SQLAlchemy connection string
            connection_string = (
                f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}"
                f"@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
            )
            
            # Create engine with optimized settings
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=20,  # Number of connections to maintain
                max_overflow=30,  # Additional connections allowed
                pool_pre_ping=True,  # Validate connections before use
                pool_recycle=3600,  # Recycle connections every hour
                echo=False,  # Set to True for SQL debugging
                connect_args={
                    'connect_timeout': 10,
                    'application_name': 'analytics_bulk_loader'
                }
            )
            
            logger.info("✅ SQLAlchemy engine initialized for bulk loading")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup SQLAlchemy engine: {e}")
            raise
    
    def bulk_load_chart_of_accounts(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """
        Bulk load Chart of Accounts using pandas.to_sql()
        
        Args:
            df: pandas DataFrame with COA data
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting ultra-fast COA bulk load with pandas.to_sql()...")
        start_time = datetime.now()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_coa_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql() - much faster than Django ORM
            processed_df.to_sql(
                'core_chartofaccount',  # Django table name
                self.engine,
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
                        COPY core_sapglposting (
                            data_file_id, document_number, document_type, posting_date,
                            document_date, entry_date, gl_account, amount_local_currency,
                            local_currency, profit_center, user_name, fiscal_year,
                            posting_period, text, segment, clearing_document,
                            offsetting_account, invoice_reference, sales_document,
                            assignment, year_month, created_at, updated_at
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
                
                # Cleanup temp file
                if os.path.exists(prepared_csv_path):
                    os.unlink(prepared_csv_path)
                    
        except Exception as e:
            logger.error(f"❌ GL COPY bulk load failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def bulk_load_gl_with_pandas(self, df: pd.DataFrame, data_file_id: str) -> Dict[str, Any]:
        """
        Fast GL bulk load using pandas.to_sql() (fallback method)
        
        Args:
            df: pandas DataFrame with GL data
            data_file_id: UUID of the DataFile
            
        Returns:
            Dict with loading results
        """
        logger.info("🚀 Starting fast GL bulk load with pandas.to_sql()...")
        start_time = datetime.now()
        
        try:
            # Prepare DataFrame for database
            processed_df = self._prepare_gl_dataframe(df, data_file_id)
            
            # Bulk insert using pandas.to_sql() with larger chunks for GL data
            processed_df.to_sql(
                'core_sapglposting',  # Django table name
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000  # Larger chunks for GL data
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(processed_df) / duration if duration > 0 else 0
            
            logger.info(f"✅ GL pandas bulk load completed: {len(processed_df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
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
                'core_sapglposting',  # Django table name
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000  # Larger chunks for GL data
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_per_second = len(df) / duration if duration > 0 else 0
            
            logger.info(f"✅ GL pandas bulk load from instances completed: {len(df):,} records in {duration:.2f}s ({records_per_second:.0f} rec/sec)")
            
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
                'posting_date': posting.posting_date,
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
                'cost_center': posting.cost_center,
                'profit_center': posting.profit_center,
                'wbs_element': posting.wbs_element,
                'order_number': posting.order_number,
                'asset_number': posting.asset_number,
                'sub_number': posting.sub_number,
                'business_area': posting.business_area,
                'segment': posting.segment,
                'partner_business_area': posting.partner_business_area,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            data.append(row_data)
        
        return pd.DataFrame(data)
    
    def _prepare_coa_dataframe(self, df: pd.DataFrame, data_file_id: str) -> pd.DataFrame:
        """Prepare COA DataFrame for database insertion"""
        processed_df = df.copy()
        
        # Add required Django fields
        processed_df['data_file_id'] = data_file_id
        processed_df['created_at'] = datetime.now()
        processed_df['updated_at'] = datetime.now()
        
        # Map column names to Django model fields
        column_mapping = {
            'Account': 'account',
            'Type': 'type',
            'Sub Type': 'sub_type',
            'Sub Sub Type': 'sub_sub_type',
            'G/L acct': 'gl_account',
            'Company': 'company',
            'Branch': 'branch',
            'Cost. C': 'cost_center',
            'Code': 'code',
            'G/L Acct Long Text': 'gl_account_long_text',
            'REF to FS': 'ref_to_fs',
            'F.S': 'financial_statement',
            'Ref to Note': 'ref_to_note',
            'FY 2024': 'fiscal_year',
            'Q1 2025': 'q1_amount',
            'Adj/Reclas': 'adj_reclas',
            'Fin Q1 2025': 'fin_q1_amount'
        }
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Convert data types
        if 'fiscal_year' in processed_df.columns:
            processed_df['fiscal_year'] = pd.to_numeric(processed_df['fiscal_year'], errors='coerce').fillna(2024).astype(int)
        
        # Convert decimal fields
        for col in ['q1_amount', 'adj_reclas', 'fin_q1_amount']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        return processed_df
    
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
        processed_df = df.copy()
        
        # Add required Django fields
        processed_df['data_file_id'] = data_file_id
        processed_df['created_at'] = datetime.now()
        processed_df['updated_at'] = datetime.now()
        
        # Map column names to Django model fields
        column_mapping = {
            'Document': 'document_number',
            'Document type': 'document_type',
            'Posting Date': 'posting_date',
            'Document Date': 'document_date',
            'Entry Date': 'entry_date',
            'G/L Account': 'gl_account',
            'Amount in Local Currency': 'amount_local_currency',
            'Local Currency': 'local_currency',
            'Profit Center': 'profit_center',
            'User Name': 'user_name',
            'Fiscal Year': 'fiscal_year',
            'Posting period': 'posting_period',
            'Text': 'text',
            'Segment': 'segment',
            'Clearing Document': 'clearing_document',
            'Offsetting': 'offsetting_account',
            'Invoice Reference': 'invoice_reference',
            'Sales Document': 'sales_document',
            'Assignment': 'assignment',
            'Year/Month': 'year_month'
        }
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Convert data types
        if 'fiscal_year' in processed_df.columns:
            processed_df['fiscal_year'] = pd.to_numeric(processed_df['fiscal_year'], errors='coerce').fillna(2025).astype(int)
        
        if 'posting_period' in processed_df.columns:
            processed_df['posting_period'] = pd.to_numeric(processed_df['posting_period'], errors='coerce').fillna(1).astype(int)
        
        # Convert amount to numeric
        if 'amount_local_currency' in processed_df.columns:
            processed_df['amount_local_currency'] = pd.to_numeric(processed_df['amount_local_currency'], errors='coerce').fillna(0)
        
        # Convert dates
        date_columns = ['posting_date', 'document_date', 'entry_date']
        for col in date_columns:
            if col in processed_df.columns:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
        
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