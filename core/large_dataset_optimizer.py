#!/usr/bin/env python3
"""
Large Dataset Optimizer - Simplified Version
Specialized module for handling datasets of 38,000+ records efficiently
with parallel processing and basic analysis optimization
"""

import os
import time
import logging
import psutil
from datetime import datetime, timedelta
from typing import List, Dict, Any, Generator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.db import connection, transaction
from django.core.cache import cache
from django.utils import timezone
from django.conf import settings

logger = logging.getLogger(__name__)

class LargeDatasetOptimizer:
    """
    Optimized processor for large datasets (38,000+ records)
    Implements advanced memory management and parallel processing
    """
    
    def __init__(self, data_file_id: str, max_workers: int = 4, 
                 batch_size: int = 2000, memory_limit_mb: int = 1000):
        self.data_file_id = data_file_id
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.memory_limit_mb = memory_limit_mb
        self.processing_stats = {
            'start_time': None,
            'total_records': 0,
            'processed_records': 0,
            'failed_records': 0,
            'memory_peak_mb': 0,
            'processing_time_seconds': 0,
            'batches_processed': 0,
            'parallel_analyses': 0
        }
        
    def optimize_for_large_dataset(self) -> Dict[str, Any]:
        """
        Main optimization method for large datasets
        """
        logger.info(f"Starting large dataset optimization for file {self.data_file_id}")
        self.processing_stats['start_time'] = timezone.now()
        
        try:
            # 1. Pre-processing optimization
            self._pre_processing_optimization()
            
            # 2. Memory-optimized data loading
            data_chunks = self._load_data_in_chunks()
            
            # 3. Parallel processing
            results = self._process_chunks_parallel(data_chunks)
            
            # 4. Post-processing optimization
            final_results = self._post_processing_optimization(results)
            
            # 5. Performance reporting
            self._generate_performance_report()
            
            return {
                'success': True,
                'results': final_results,
                'stats': self.processing_stats,
                'optimization_applied': True,
                'parallel_processing': True
            }
            
        except Exception as e:
            logger.error(f"Large dataset optimization failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': self.processing_stats
            }
    
    def _pre_processing_optimization(self):
        """Pre-processing optimizations for large datasets"""
        logger.info("Applying pre-processing optimizations")
        
        # 1. Database connection optimization
        self._optimize_database_connections()
        
        # 2. Memory cleanup
        self._cleanup_memory()
        
        # 3. Cache preparation
        self._prepare_cache()
        
        # 4. Batch size adjustment based on dataset size
        self._adjust_batch_size()
    
    def _optimize_database_connections(self):
        """Optimize database connections for large datasets"""
        # Set connection parameters for large datasets (only those that don't require server restart)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET work_mem = '256MB'")
                cursor.execute("SET maintenance_work_mem = '512MB'")
        except Exception as e:
            logger.warning(f"Could not optimize database connections: {e}")
        
        logger.info("Database connections optimized for large dataset processing")
    
    def _cleanup_memory(self):
        """Clean up memory before processing"""
        import gc
        gc.collect()
        
        # Clear Django cache
        cache.clear()
        
        # Close database connections
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Could not close database connection: {e}")
        
        logger.info("Memory cleanup completed")
    
    def _prepare_cache(self):
        """Prepare cache for large dataset processing"""
        cache.set(f"large_dataset_{self.data_file_id}_status", "PROCESSING", timeout=7200)
        cache.set(f"large_dataset_{self.data_file_id}_start_time", timezone.now(), timeout=7200)
        
        logger.info("Cache prepared for large dataset processing")
    
    def _adjust_batch_size(self):
        """Adjust batch size based on dataset size and available memory"""
        try:
            from core.models import SAPGLPosting
            total_records = SAPGLPosting.objects.filter(data_file_id=self.data_file_id).count()
            self.processing_stats['total_records'] = total_records
            
            # Adjust batch size based on dataset size
            if total_records > 50000:
                self.batch_size = 1000
            elif total_records > 30000:
                self.batch_size = 1500
            else:
                self.batch_size = 2000
            
            # Adjust based on available memory
            available_memory = psutil.virtual_memory().available / (1024 * 1024)  # MB
            if available_memory < 2000:
                self.batch_size = min(self.batch_size, 500)
            
            logger.info(f"Adjusted batch size to {self.batch_size} for {total_records} records")
            
        except Exception as e:
            logger.warning(f"Could not adjust batch size: {e}")
    
    def _load_data_in_chunks(self) -> Generator[List[Any], None, None]:
        """Load data in memory-efficient chunks"""
        from core.models import SAPGLPosting
        
        offset = 0
        while True:
            chunk = list(SAPGLPosting.objects.filter(
                data_file_id=self.data_file_id
            ).select_related('data_file')[offset:offset + self.batch_size])
            
            if not chunk:
                break
            
            yield chunk
            offset += self.batch_size
            
            # Memory check
            self._check_memory_usage()
    
    def _check_memory_usage(self):
        """Check memory usage and cleanup if necessary"""
        memory_usage = psutil.virtual_memory().percent
        if memory_usage > 80:
            logger.warning(f"High memory usage detected: {memory_usage}%")
            self._force_memory_cleanup()
        
        # Update peak memory
        current_memory = psutil.virtual_memory().used / (1024 * 1024)  # MB
        self.processing_stats['memory_peak_mb'] = max(
            self.processing_stats['memory_peak_mb'], current_memory
        )
    
    def _force_memory_cleanup(self):
        """Force memory cleanup"""
        import gc
        gc.collect()
        cache.clear()
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Could not close database connection: {e}")
    
    def _process_chunks_parallel(self, data_chunks: Generator[List[Any], None, None]) -> List[Dict[str, Any]]:
        """Process data chunks in parallel"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            future_to_chunk = {}
            chunk_index = 0
            
            for chunk in data_chunks:
                future = executor.submit(self._process_single_chunk, chunk, chunk_index)
                future_to_chunk[future] = chunk_index
                chunk_index += 1
            
            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.processing_stats['batches_processed'] += 1
                    
                    # Update progress
                    self.processing_stats['processed_records'] += result.get('processed_count', 0)
                    self.processing_stats['failed_records'] += result.get('failed_count', 0)
                    
                    logger.info(f"Chunk {chunk_index} completed: {result.get('processed_count', 0)} records processed")
                    
                except Exception as e:
                    logger.error(f"Chunk {chunk_index} failed: {e}")
                    results.append({
                        'chunk_index': chunk_index,
                        'success': False,
                        'error': str(e),
                        'processed_count': 0,
                        'failed_count': len(chunk) if 'chunk' in locals() else 0
                    })
        
        return results
    
    def _process_single_chunk(self, chunk: List[Any], chunk_index: int) -> Dict[str, Any]:
        """Process a single chunk of data"""
        start_time = time.time()
        processed_count = 0
        failed_count = 0
        chunk_results = []
        
        try:
            for record in chunk:
                try:
                    # Process individual record with basic analysis
                    record_result = self._process_single_record(record)
                    chunk_results.append(record_result)
                    processed_count += 1
                    
                    if record_result.get('parallel_processed', False):
                        self.processing_stats['parallel_analyses'] += 1
                    
                except Exception as e:
                    logger.error(f"Record processing failed: {e}")
                    failed_count += 1
                    chunk_results.append({
                        'record_id': str(record.id),
                        'success': False,
                        'error': str(e)
                    })
            
            processing_time = time.time() - start_time
            
            return {
                'chunk_index': chunk_index,
                'success': True,
                'processed_count': processed_count,
                'failed_count': failed_count,
                'processing_time': processing_time,
                'results': chunk_results
            }
            
        except Exception as e:
            logger.error(f"Chunk {chunk_index} processing failed: {e}")
            return {
                'chunk_index': chunk_index,
                'success': False,
                'error': str(e),
                'processed_count': 0,
                'failed_count': len(chunk)
            }
    
    def _process_single_record(self, record: Any) -> Dict[str, Any]:
        """
        Process a single record with rule-based analysis
        """
        try:
            # Rule-based analysis without ML models
            analysis_results = {}
            
            # 1. Duplicate Analysis (basic check)
            duplicate_risk = self._check_duplicate_basic(record)
            analysis_results['duplicate_analysis'] = {
                'risk_score': duplicate_risk,
                'analysis_type': 'rule_based'
            }
            
            # 2. Backdated Analysis (basic check)
            backdated_risk = self._check_backdated_basic(record)
            analysis_results['backdated_analysis'] = {
                'risk_score': backdated_risk,
                'analysis_type': 'rule_based'
            }
            
            # 3. User Analysis (basic check)
            user_risk = self._check_user_basic(record)
            analysis_results['user_analysis'] = {
                'risk_score': user_risk,
                'analysis_type': 'rule_based'
            }
            
            # 4. Unusual Days Analysis (basic check)
            unusual_days_risk = self._check_unusual_days_basic(record)
            analysis_results['unusual_days_analysis'] = {
                'risk_score': unusual_days_risk,
                'analysis_type': 'rule_based'
            }
            
            # 5. Closing Entries Analysis (basic check)
            closing_entries_risk = self._check_closing_entries_basic(record)
            analysis_results['closing_entries_analysis'] = {
                'risk_score': closing_entries_risk,
                'analysis_type': 'rule_based'
            }
            
            # 6. Holiday Analysis (basic check)
            holiday_risk = self._check_holiday_basic(record)
            analysis_results['holiday_analysis'] = {
                'risk_score': holiday_risk,
                'analysis_type': 'rule_based'
            }
            
            return {
                'record_id': str(record.id),
                'success': True,
                'analysis_results': analysis_results,
                'parallel_processed': True,
                'analysis_type': 'rule_based'
            }
            
        except Exception as e:
            logger.error(f"Rule-based analysis failed for record {record.id}: {e}")
            return {
                'record_id': str(record.id),
                'success': False,
                'error': str(e),
                'analysis_results': {
                    'duplicate_analysis': {'risk_score': 0.0},
                    'backdated_analysis': {'risk_score': 0.0},
                    'user_analysis': {'risk_score': 0.0},
                    'unusual_days_analysis': {'risk_score': 0.0},
                    'closing_entries_analysis': {'risk_score': 0.0},
                    'holiday_analysis': {'risk_score': 0.0}
                },
                'parallel_processed': False,
                'analysis_type': 'failed'
            }
    
    def _check_duplicate_basic(self, record: Any) -> float:
        """Basic duplicate check"""
        try:
            # Check if record has duplicate flag
            if hasattr(record, 'is_duplicate') and record.is_duplicate:
                return 25.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic duplicate check: {e}")
            return 0.0
    
    def _check_backdated_basic(self, record: Any) -> float:
        """Basic backdated check"""
        try:
            # Check if record has backdated flag
            if hasattr(record, 'is_backdated') and record.is_backdated:
                return 25.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic backdated check: {e}")
            return 0.0
    
    def _check_user_basic(self, record: Any) -> float:
        """Basic user analysis check"""
        try:
            # Check for unusual user patterns
            if hasattr(record, 'user_name') and record.user_name:
                # Basic check - could be enhanced
                return 0.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic user check: {e}")
            return 0.0
    
    def _check_unusual_days_basic(self, record: Any) -> float:
        """Basic unusual days check"""
        try:
            # Check for weekend postings
            if hasattr(record, 'posting_date') and record.posting_date:
                day_of_week = record.posting_date.weekday()
                if day_of_week in [4, 5]:  # Friday, Saturday
                    return 15.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic unusual days check: {e}")
            return 0.0
    
    def _check_closing_entries_basic(self, record: Any) -> float:
        """Basic closing entries check"""
        try:
            # Check for month-end postings
            if hasattr(record, 'posting_date') and record.posting_date:
                day = record.posting_date.day
                if day >= 28:  # Near month-end
                    return 15.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic closing entries check: {e}")
            return 0.0
    
    def _check_holiday_basic(self, record: Any) -> float:
        """Basic holiday analysis check"""
        try:
            # Check if the posting date is a holiday
            if hasattr(record, 'posting_date') and record.posting_date:
                # This is a placeholder. In a real scenario, you'd have a list of holidays.
                # For now, we'll return a low risk score.
                return 0.0
            return 0.0
        except Exception as e:
            logger.warning(f"Error in basic holiday check: {e}")
            return 0.0
    
    def _post_processing_optimization(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Post-processing optimizations and result aggregation
        """
        logger.info("Applying post-processing optimizations")
        
        # 1. Aggregate results
        aggregated_results = self._aggregate_results(results)
        
        # 2. Optimize database operations
        self._optimize_database_operations(aggregated_results)
        
        # 3. Cache final results
        self._cache_final_results(aggregated_results)
        
        # 4. Final memory cleanup
        self._cleanup_memory()
        
        return aggregated_results
    
    def _aggregate_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate results from all chunks
        """
        total_processed = sum(r.get('processed_count', 0) for r in results)
        total_failed = sum(r.get('failed_count', 0) for r in results)
        total_time = sum(r.get('processing_time', 0) for r in results)
        
        # Aggregate analysis results
        all_analysis_results = []
        for result in results:
            if result.get('success') and 'results' in result:
                all_analysis_results.extend(result['results'])
        
        return {
            'total_processed': total_processed,
            'total_failed': total_failed,
            'total_processing_time': total_time,
            'analysis_results': all_analysis_results,
            'success_rate': (total_processed / (total_processed + total_failed)) * 100 if (total_processed + total_failed) > 0 else 0,
            'parallel_analyses': self.processing_stats['parallel_analyses']
        }
    
    def _optimize_database_operations(self, aggregated_results: Dict[str, Any]):
        """
        Optimize database operations for large result sets
        """
        # Use bulk operations for updating analysis results
        if 'analysis_results' in aggregated_results:
            self._bulk_update_analysis_results(aggregated_results['analysis_results'])
    
    def _bulk_update_analysis_results(self, analysis_results: List[Dict[str, Any]]):
        """
        Bulk update analysis results in the database
        """
        from core.models import SAPGLPosting
        
        # Prepare bulk update data
        updates = []
        for result in analysis_results:
            if result.get('success'):
                updates.append({
                    'id': result['record_id'],
                    'overall_risk_score': self._calculate_overall_risk_score(result['analysis_results']),
                    'anomaly_types': self._extract_anomaly_types(result['analysis_results'])
                })
        
        # Perform bulk update
        if updates:
            with transaction.atomic():
                for update in updates:
                    SAPGLPosting.objects.filter(id=update['id']).update(
                        overall_risk_score=update['overall_risk_score'],
                        anomaly_types=update['anomaly_types']
                    )
        
        logger.info(f"Bulk updated {len(updates)} analysis results")
    
    def _calculate_overall_risk_score(self, analysis_results: Dict[str, Any]) -> float:
        """
        Calculate overall risk score from analysis results
        """
        try:
            total_risk_score = 0.0
            
            # Extract risk scores from each analysis type
            duplicate_risk = analysis_results.get('duplicate_analysis', {}).get('risk_score', 0.0)
            backdated_risk = analysis_results.get('backdated_analysis', {}).get('risk_score', 0.0)
            user_risk = analysis_results.get('user_analysis', {}).get('risk_score', 0.0)
            unusual_days_risk = analysis_results.get('unusual_days_analysis', {}).get('risk_score', 0.0)
            closing_entries_risk = analysis_results.get('closing_entries_analysis', {}).get('risk_score', 0.0)
            holiday_risk = analysis_results.get('holiday_analysis', {}).get('risk_score', 0.0)
            
            # Apply weighted scoring
            total_risk_score += min(duplicate_risk, 25.0)
            total_risk_score += min(backdated_risk, 25.0)
            total_risk_score += min(user_risk, 20.0)
            total_risk_score += min(unusual_days_risk, 15.0)
            total_risk_score += min(closing_entries_risk, 15.0)
            total_risk_score += min(holiday_risk, 10.0) # Assuming a holiday risk score
            
            # Cap the total risk score at 100
            return min(total_risk_score, 100.0)
            
        except Exception as e:
            logger.error(f"Error calculating overall risk score: {e}")
            return 0.0
    
    def _extract_anomaly_types(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Extract anomaly types from analysis results
        """
        anomaly_types = []
        for analysis_type, result in analysis_results.items():
            if result.get('risk_score', 0) > 0:
                # Map analysis type to anomaly type
                if analysis_type == 'duplicate_analysis':
                    anomaly_types.append('duplicate')
                elif analysis_type == 'backdated_analysis':
                    anomaly_types.append('backdated')
                elif analysis_type == 'user_analysis':
                    anomaly_types.append('user_anomaly')
                elif analysis_type == 'unusual_days_analysis':
                    anomaly_types.append('unusual_days')
                elif analysis_type == 'closing_entries_analysis':
                    anomaly_types.append('closing_entries')
                elif analysis_type == 'holiday_analysis':
                    anomaly_types.append('holiday')
        return anomaly_types
    
    def _cache_final_results(self, aggregated_results: Dict[str, Any]):
        """
        Cache final results for quick access
        """
        cache_key = f"large_dataset_{self.data_file_id}_final_results"
        cache.set(cache_key, aggregated_results, timeout=7200)  # 2 hours
        
        # Update processing status
        cache.set(f"large_dataset_{self.data_file_id}_status", "COMPLETED", timeout=7200)
        
        logger.info("Final results cached successfully")
    
    def _log_progress(self):
        """
        Log processing progress
        """
        progress_percentage = (self.processing_stats['processed_records'] / 
                             self.processing_stats['total_records']) * 100 if self.processing_stats['total_records'] > 0 else 0
        
        logger.info(f"Progress: {progress_percentage:.1f}% "
                   f"({self.processing_stats['processed_records']}/{self.processing_stats['total_records']} records) "
                   f"Memory: {self.processing_stats['memory_peak_mb']:.2f}MB "
                   f"Parallel: {self.processing_stats['parallel_analyses']}")
    
    def _generate_performance_report(self):
        """
        Generate comprehensive performance report
        """
        end_time = timezone.now()
        self.processing_stats['processing_time_seconds'] = (
            end_time - self.processing_stats['start_time']
        ).total_seconds()
        
        # Calculate performance metrics
        records_per_second = (self.processing_stats['processed_records'] / 
                            self.processing_stats['processing_time_seconds']) if self.processing_stats['processing_time_seconds'] > 0 else 0
        
        performance_report = {
            'file_id': self.data_file_id,
            'total_records': self.processing_stats['total_records'],
            'processed_records': self.processing_stats['processed_records'],
            'failed_records': self.processing_stats['failed_records'],
            'success_rate': (self.processing_stats['processed_records'] / 
                           self.processing_stats['total_records']) * 100 if self.processing_stats['total_records'] > 0 else 0,
            'processing_time_seconds': self.processing_stats['processing_time_seconds'],
            'records_per_second': records_per_second,
            'memory_peak_mb': self.processing_stats['memory_peak_mb'],
            'batches_processed': self.processing_stats['batches_processed'],
            'batch_size_used': self.batch_size,
            'workers_used': self.max_workers,
            'optimization_applied': True,
            'parallel_processing': True,
            'parallel_analyses': self.processing_stats['parallel_analyses']
        }
        
        # Cache performance report
        cache.set(f"large_dataset_{self.data_file_id}_performance", performance_report, timeout=3600)
        
        logger.info(f"Performance report generated: {records_per_second:.2f} records/second, "
                   f"{self.processing_stats['memory_peak_mb']:.2f}MB peak memory, "
                   f"{self.processing_stats['parallel_analyses']} parallel analyses")


class LargeDatasetMonitor:
    """
    Monitor for large dataset processing
    """
    
    def __init__(self, data_file_id: str):
        self.data_file_id = data_file_id
    
    def get_processing_status(self) -> Dict[str, Any]:
        """
        Get current processing status
        """
        cache_key_prefix = f"large_dataset_{self.data_file_id}"
        
        status = cache.get(f"{cache_key_prefix}_status", "UNKNOWN")
        start_time = cache.get(f"{cache_key_prefix}_start_time")
        performance = cache.get(f"{cache_key_prefix}_performance", {})
        
        return {
            'status': status,
            'start_time': start_time,
            'performance': performance
        }
    
    def get_memory_usage(self) -> Dict[str, float]:
        """
        Get current memory usage
        """
        memory = psutil.virtual_memory()
        return {
            'total_mb': memory.total / (1024 * 1024),
            'available_mb': memory.available / (1024 * 1024),
            'used_mb': memory.used / (1024 * 1024),
            'percent': memory.percent
        }
    
    def get_system_resources(self) -> Dict[str, Any]:
        """
        Get system resource information
        """
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory': self.get_memory_usage(),
            'disk_usage': psutil.disk_usage('/').percent
        }


def optimize_large_dataset_processing(data_file_id: str, 
                                    max_workers: int = 4,
                                    batch_size: int = 2000,
                                    memory_limit_mb: int = 1000) -> Dict[str, Any]:
    """
    Convenience function to optimize large dataset processing
    """
    optimizer = LargeDatasetOptimizer(
        data_file_id=data_file_id,
        max_workers=max_workers,
        batch_size=batch_size,
        memory_limit_mb=memory_limit_mb
    )
    return optimizer.optimize_for_large_dataset()


def get_large_dataset_status(data_file_id: str) -> Dict[str, Any]:
    """
    Get status of large dataset processing
    """
    monitor = LargeDatasetMonitor(data_file_id)
    return monitor.get_processing_status() 