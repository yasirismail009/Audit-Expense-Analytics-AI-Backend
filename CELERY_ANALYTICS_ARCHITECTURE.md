# Celery Analytics Architecture

## Overview

This document explains how all analytics and ML training are processed in Celery for optimal performance and scalability.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   File Upload   │───▶│  Celery Task    │───▶│  Background     │
│   (Django View) │    │  Queue          │    │  Processing     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  Analytics &    │
                       │  ML Training    │
                       │  (Celery Tasks) │
                       └─────────────────┘
```

## 🔄 Processing Flow

### 1. File Upload (Synchronous)
- User uploads file via API
- File is validated and saved
- `FileProcessingJob` is created with status `PENDING`
- Celery task is queued for background processing
- API returns immediately with job ID

### 2. Background Processing (Asynchronous - Celery)
- **File Processing**: Parse CSV and create transactions
- **Default Analytics**: Trial Balance, Transaction Entries, GL Summaries
- **Anomaly Detection**: Run requested anomaly tests
- **ML Training**: Auto-train models if sufficient data
- **Comprehensive Analytics**: Expense breakdown, user patterns, risk assessment
- **Results Storage**: Save all results to database

## 📊 What Runs in Celery

### ✅ **Analytics Processing**
- **Trial Balance Generation**
- **Transaction Entry Analysis**
- **GL Account Summaries**
- **Comprehensive Expense Analytics**
  - Expense breakdown by account
  - User spending patterns
  - Account usage patterns
  - Temporal patterns (monthly/daily)
  - Risk assessment and scoring

### ✅ **Anomaly Detection**
- **Duplicate Transaction Detection**
- **Backdated Entry Detection**
- **User Anomaly Detection**
- **Pattern-based Anomalies**

### ✅ **ML Model Training**
- **Automatic Training Trigger**: When 50+ transactions available
- **Model Types**: Isolation Forest, Random Forest, Statistical, Rule-based
- **Feature Engineering**: Temporal, amount-based, categorical features
- **Performance Metrics**: Accuracy, precision, recall calculation
- **In-memory Storage**: Models stored securely in memory

### ✅ **Risk Assessment**
- **Risk Factor Analysis**: High-value, unusual patterns, round amounts
- **Risk Scoring**: Calculated risk score with LOW/MEDIUM/HIGH classification
- **Recommendations**: Automated audit recommendations

## 🚀 Celery Tasks

### Main Processing Task
```python
@shared_task(bind=True)
def process_file_with_anomalies(self, job_id):
    """Main Celery task for file processing and analytics"""
```

### ML Training Tasks
```python
@shared_task(bind=True)
def train_ml_models(self, training_session_id):
    """Train ML models in background"""

@shared_task(bind=True)
def retrain_ml_models(self, training_session_id):
    """Retrain existing models with new data"""
```

### Maintenance Tasks
```python
@shared_task
def monitor_processing_jobs():
    """Monitor job status and performance"""

@shared_task
def monitor_ml_model_performance():
    """Monitor ML model performance metrics"""
```

## 🔧 Configuration

### Celery Settings
```python
# settings.py
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True
```

### Task Configuration
```python
# tasks.py
CELERY_TASK_ROUTES = {
    'core.tasks.process_file_with_anomalies': {'queue': 'analytics'},
    'core.tasks.train_ml_models': {'queue': 'ml_training'},
    'core.tasks.retrain_ml_models': {'queue': 'ml_training'},
}
```

## 📈 Performance Benefits

### ✅ **Asynchronous Processing**
- File upload returns immediately
- No timeout issues for large files
- Better user experience

### ✅ **Scalability**
- Multiple workers can process files concurrently
- Horizontal scaling possible
- Resource isolation

### ✅ **Reliability**
- Failed tasks can be retried
- Job status tracking
- Error handling and logging

### ✅ **Resource Management**
- CPU-intensive analytics don't block web server
- Memory usage controlled
- Background processing doesn't affect API response times

## 🔍 Monitoring and Debugging

### Job Status Tracking
```python
# Check job status
GET /api/processing-jobs/{job_id}/status/

# Response
{
    "job_id": "uuid",
    "status": "PROCESSING|COMPLETED|FAILED",
    "processing_duration": 45.2,
    "analytics_results": {...},
    "anomaly_results": {...}
}
```

### ML Training Status
```python
# Check ML training status
GET /api/ml-model-training/{training_id}/status/

# Response
{
    "status": "TRAINING|COMPLETED|FAILED",
    "training_duration": 120.5,
    "performance_metrics": {...}
}
```

### Celery Worker Monitoring
```python
# Check worker status
from celery import current_app
i = current_app.control.inspect()
stats = i.stats()
```

## 🧪 Testing

### Test Scripts
- `test_celery_analytics.py`: Comprehensive Celery testing
- `test_enhanced_analytics.py`: Enhanced analytics testing
- `test_analytics_dashboard.py`: Dashboard analytics testing

### Test Coverage
- ✅ File upload and processing
- ✅ Analytics generation
- ✅ ML model training
- ✅ Anomaly detection
- ✅ Risk assessment
- ✅ Performance monitoring

## 🚀 Running Celery

### Start Celery Worker
```bash
# Start worker for analytics queue
celery -A analytics worker --loglevel=info --queues=analytics,ml_training

# Start worker with specific concurrency
celery -A analytics worker --loglevel=info --concurrency=4

# Start worker for specific queue
celery -A analytics worker --loglevel=info -Q analytics
```

### Start Celery Beat (for scheduled tasks)
```bash
celery -A analytics beat --loglevel=info
```

### Monitor Celery
```bash
# Monitor tasks
celery -A analytics monitor

# Check worker status
celery -A analytics inspect stats
```

## 📊 Analytics Results Structure

### Analytics Results (Celery Processed)
```json
{
    "trial_balance": {...},
    "transaction_entries": {...},
    "gl_account_summaries": {...},
    "expense_analytics": {
        "summary": {
            "total_transactions": 1000,
            "total_amount": 50000000.00,
            "unique_users": 25,
            "unique_accounts": 15
        },
        "expense_breakdown": {...},
        "user_patterns": {...},
        "account_patterns": {...},
        "temporal_patterns": {...},
        "risk_assessment": {
            "risk_score": 35.5,
            "risk_level": "MEDIUM",
            "risk_factors": {...},
            "recommendations": [...]
        }
    },
    "ml_training": {
        "status": "STARTED",
        "training_session_id": "uuid",
        "transactions_count": 1000
    }
}
```

### Anomaly Results (Celery Processed)
```json
{
    "duplicate": {
        "anomalies_found": 5,
        "details": [...]
    },
    "backdated": {
        "anomalies_found": 12,
        "details": [...]
    },
    "user_anomalies": {
        "anomalies_found": 8,
        "details": [...]
    }
}
```

## 🔒 Security Features

### ✅ **In-Memory ML Models**
- Models stored in memory, not on disk
- Base64 encoded for security
- No file system access required

### ✅ **Data Privacy**
- Raw files not saved to disk
- Only processed transactions stored
- Secure hash-based duplicate detection

### ✅ **Access Control**
- Job-based access control
- Session-based authentication
- API key validation

## 🎯 Best Practices

### ✅ **Task Design**
- Keep tasks idempotent
- Handle exceptions gracefully
- Use appropriate timeouts
- Log important events

### ✅ **Resource Management**
- Monitor memory usage
- Set appropriate concurrency
- Use task routing for load balancing
- Implement retry logic

### ✅ **Monitoring**
- Track task completion rates
- Monitor processing times
- Alert on failures
- Log performance metrics

## 📝 Summary

All analytics and ML training operations are now processed in Celery, providing:

1. **Asynchronous Processing**: Non-blocking file uploads
2. **Scalable Architecture**: Multiple workers, horizontal scaling
3. **Comprehensive Analytics**: Deep expense analysis and risk assessment
4. **Automatic ML Training**: Self-improving anomaly detection
5. **Reliable Processing**: Error handling, retries, monitoring
6. **Security**: In-memory models, no file storage
7. **Performance**: Optimized resource usage and response times

This architecture ensures that the system can handle large files, complex analytics, and ML training without impacting API responsiveness or user experience. 