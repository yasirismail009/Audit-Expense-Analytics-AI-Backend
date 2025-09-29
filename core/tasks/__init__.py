"""
Tasks package for analytics system
"""

# Import all task functions for easy access
from .analysis_tasks import (
    run_restructured_analysis,
    run_general_analysis,
    run_duplicate_analysis,
    run_backdated_analysis,
    run_user_analysis,
    run_unusual_days_analysis,
    run_closing_entries_analysis,
    run_holiday_analysis,
    run_overall_analysis,
    run_manual_entry_analysis,
    run_ai_risk_recommendations,
    run_risk_analysis,
)

from .completeness_tasks import (
    run_completeness_job,
    run_completeness_test,
    run_gl_completeness_analysis,
    trigger_eng008_completeness,
    run_eng008_complete_workflow,
)

from .ml_completeness_tasks import (
    run_ml_completeness_analysis,
    generate_ml_recommendations,
    validate_ml_recommendations,
)

from .perfect_flow_tasks import (
    trigger_perfect_ml_flow,
)

from .ml_training_tasks import (
    train_rule_based_models,
    retrain_rule_based_models,
    train_duplicate_analysis_model,
    train_backdated_analysis_model,
    train_user_analysis_model,
    train_unusual_days_analysis_model,
    train_closing_entries_analysis_model,
    train_holiday_analysis_model,
    train_overall_risk_analysis_model,
    train_ml_models,
    retrain_ml_models,
    predict_anomalies_ml,
    train_comprehensive_ai_models,
    train_completeness_recommendation_model,
    predict_completeness_with_ai,
)

from .monitoring_tasks import (
    debug_task,
    process_queued_jobs,
    monitor_processing_jobs,
    worker_health_check,
    monitor_worker_performance,
)

from .risk_tasks import (
    run_ai_risk_recommendations,
    run_risk_analysis,
)

from .utils import (
    send_notification_if_available,
    get_user_from_job,
    _serialize_transaction,
    _calculate_holiday_risk_score,
    _get_holiday_risk_level,
    _generate_holiday_risk_assessment,
    _generate_holiday_audit_recommendations,
    _generate_holiday_breakdown,
    log_task_info,
    debug_task_state,
    debug_task_data,
    debug_task_exception,
    get_system_info,
)

__all__ = [
    # Analysis tasks
    'run_restructured_analysis',
    'run_general_analysis',
    'run_duplicate_analysis',
    'run_backdated_analysis',
    'run_user_analysis',
    'run_unusual_days_analysis',
    'run_closing_entries_analysis',
    'run_holiday_analysis',
    'run_overall_analysis',
    'run_manual_entry_analysis',
    
    # Completeness tasks
    'run_completeness_job',
    'run_completeness_test',
    'run_gl_completeness_analysis',
    'trigger_eng008_completeness',
    'run_eng008_complete_workflow',
    
    # ML Completeness tasks
    'run_ml_completeness_analysis',
    'generate_ml_recommendations',
    'validate_ml_recommendations',
    
    # Perfect Flow tasks
    'run_perfect_ml_flow',
    'trigger_perfect_ml_flow',
    
    # ML training tasks
    'train_rule_based_models',
    'retrain_rule_based_models',
    'train_duplicate_analysis_model',
    'train_backdated_analysis_model',
    'train_user_analysis_model',
    'train_unusual_days_analysis_model',
    'train_closing_entries_analysis_model',
    'train_holiday_analysis_model',
    'train_overall_risk_analysis_model',
    'train_ml_models',
    'retrain_ml_models',
    'predict_anomalies_ml',
    'predict_completeness_with_ai',
    'train_comprehensive_ai_models',
    'train_completeness_recommendation_model',
    
    # Monitoring tasks
    'debug_task',
    'process_queued_jobs',
    'monitor_processing_jobs',
    'worker_health_check',
    'monitor_worker_performance',
    
    # Risk tasks
    'run_ai_risk_recommendations',
    'run_risk_analysis',
    
    # Utils
    'send_notification_if_available',
    'get_user_from_job',
    '_serialize_transaction',
    '_calculate_holiday_risk_score',
    '_get_holiday_risk_level',
    '_generate_holiday_risk_assessment',
    '_generate_holiday_audit_recommendations',
    '_generate_holiday_breakdown',
    'log_task_info',
    'debug_task_state',
    'debug_task_data',
    'debug_task_exception',
    'get_system_info',
]
