# Non-Blocking Upload and Dedicated Duplicate Detection Model

## Overview

This document describes the enhanced system that provides:
1. **Non-blocking file uploads** - Uploads return immediately without waiting for Celery completion
2. **Dedicated duplicate detection model** - Trained once and used for analysis with 6 duplicate types

## Key Improvements

### 1. Non-Blocking Upload System

#### Before
- Upload requests waited for Celery task completion
- Users had to wait for full processing before getting response
- Blocking behavior caused timeout issues

#### After
- Upload requests return immediately with job ID
- Processing continues in background via Celery
- Users can check job status via dedicated endpoint

#### Upload Response Format
```json
{
    "job_id": "uuid",
    "status": "PROCESSING",
    "message": "File uploaded successfully. Processing started in background.",
    "file_info": {...},
    "celery_task_id": "task-uuid",
    "status_endpoint": "/api/file-processing-jobs/{job_id}/status/",
    "analytics_endpoint": "/api/analysis/file/{file_id}/",
    "duplicate_analysis_endpoint": "/api/duplicate-anomalies/?sheet_id={file_id}"
}
```

#### Status Check Endpoint
```
GET /api/file-processing-jobs/{job_id}/status/
```

### 2. Dedicated Duplicate Detection Model

#### Model Characteristics
- **Trained once** - No retraining, single training session
- **Duplicate-focused** - Only handles duplicate detection
- **6 duplicate types** - Uses enhanced duplicate definitions
- **ML-enhanced** - Combines rule-based and ML predictions

#### Duplicate Types Supported
1. **Type 1 Duplicate** - Account Number + Amount
2. **Type 2 Duplicate** - Account Number + Source + Amount
3. **Type 3 Duplicate** - Account Number + User + Amount
4. **Type 4 Duplicate** - Account Number + Posted Date + Amount
5. **Type 5 Duplicate** - Account Number + Effective Date + Amount
6. **Type 6 Duplicate** - Account Number + Effective Date + Posted Date + User + Source + Amount

#### Model Training Process
```python
# Check if model already trained
if duplicate_model.is_trained():
    return {'status': 'SKIPPED', 'reason': 'Model already trained'}

# Train once with enhanced duplicate data
training_result = duplicate_model.train_once(
    transactions=transactions,
    enhanced_duplicates=enhanced_duplicates,
    training_session=training_session
)
```

#### Model Features
- **Amount-based features**: Log amount, amount categories, rounded amounts
- **Account features**: GL account length, account patterns
- **User features**: User name length, user patterns
- **Date features**: Day of week, day of month, month, quarter
- **Text features**: Text length, content patterns

## API Endpoints

### File Upload (Non-Blocking)
```
POST /api/targeted-anomaly-upload/
```

**Response**: Immediate job ID and status endpoints

### Job Status Check
```
GET /api/file-processing-jobs/{job_id}/status/
```

**Response**: Current processing status and progress

### Duplicate Analysis (ML-Enhanced)
```
GET /api/duplicate-anomalies/?sheet_id={file_id}
```

**Response**: Enhanced with ML predictions and confidence scores

### Duplicate CSV Export
```
GET /api/duplicate-anomalies/export_csv/?sheet_id={file_id}
```

**Response**: CSV file with ML-enhanced duplicate data

## Implementation Details

### 1. Non-Blocking Upload Implementation

#### File: `core/views.py` - `TargetedAnomalyUploadView`
```python
# Submit task immediately without waiting
task_result = process_file_with_anomalies.delay(str(processing_job.id))

# Return immediately with job information
return Response({
    'job_id': str(processing_job.id),
    'status': 'PROCESSING',
    'message': 'File uploaded successfully. Processing started in background.',
    'celery_task_id': task_result.id,
    'status_endpoint': f'/api/file-processing-jobs/{processing_job.id}/status/',
    # ... other endpoints
}, status=status.HTTP_202_ACCEPTED)
```

### 2. Duplicate Detection Model

#### File: `core/ml_models.py` - `DuplicateDetectionModel`
```python
class DuplicateDetectionModel:
    def __init__(self):
        self.duplicate_types = [
            'Type 1 Duplicate - Account Number + Amount',
            'Type 2 Duplicate - Account Number + Source + Amount',
            # ... all 6 types
        ]
        self.risk_scores = {
            'Type 1 Duplicate': 10,
            'Type 2 Duplicate': 12,
            # ... risk scores for each type
        }
    
    def train_once(self, transactions, enhanced_duplicates, training_session):
        # Train Random Forest model once
        # Store model in memory
        # Return training results
```

### 3. ML-Enhanced Analysis

#### File: `core/views.py` - `DuplicateAnomalyViewSet`
```python
# Get rule-based duplicates
duplicate_results = analyzer.detect_duplicate_entries(transactions)

# Enhance with ML predictions
if duplicate_model.is_trained():
    ml_predictions = duplicate_model.predict_duplicates(transactions)
    
    # Add ML confidence to each transaction
    for transaction in duplicate_results['duplicates']:
        transaction['ml_confidence'] = ml_pred['duplicate_probability']
        transaction['ml_risk_score'] = ml_pred['risk_score']
```

## Usage Examples

### 1. Upload File (Non-Blocking)
```bash
curl -X POST http://localhost:8000/api/targeted-anomaly-upload/ \
  -F "file=@data.csv" \
  -F "engagement_id=123" \
  -F "client_name=Test Client"
```

**Response**:
```json
{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "PROCESSING",
    "message": "File uploaded successfully. Processing started in background.",
    "status_endpoint": "/api/file-processing-jobs/550e8400-e29b-41d4-a716-446655440000/status/"
}
```

### 2. Check Processing Status
```bash
curl http://localhost:8000/api/file-processing-jobs/550e8400-e29b-41d4-a716-446655440000/status/
```

**Response**:
```json
{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "COMPLETED",
    "processing_duration": "00:02:30",
    "analytics_results": {...},
    "anomaly_results": {...}
}
```

### 3. Get ML-Enhanced Duplicate Analysis
```bash
curl "http://localhost:8000/api/duplicate-anomalies/?sheet_id=123"
```

**Response**:
```json
{
    "total_duplicates": 15,
    "duplicates": [
        {
            "type": "Type 1 Duplicate",
            "transactions": [
                {
                    "id": "txn-1",
                    "gl_account": "1000",
                    "amount": 1000.00,
                    "ml_confidence": 0.95,
                    "ml_risk_score": 24,
                    "ml_prediction": true
                }
            ]
        }
    ],
    "ml_model_info": {
        "status": "trained",
        "training_accuracy": 0.92,
        "duplicate_types": ["Type 1 Duplicate", ...]
    }
}
```

## Benefits

### 1. Non-Blocking Uploads
- **Faster response times** - No waiting for processing
- **Better user experience** - Immediate feedback
- **Reduced timeout issues** - No long-running requests
- **Scalability** - Can handle multiple uploads simultaneously

### 2. Dedicated Duplicate Model
- **Focused performance** - Optimized for duplicate detection only
- **Consistent results** - Same model used across all analyses
- **Enhanced accuracy** - ML predictions improve rule-based detection
- **Risk scoring** - ML confidence scores for better decision making

### 3. Enhanced Analysis
- **ML confidence scores** - Probability of duplicate detection
- **Risk assessment** - Scaled risk scores (0-25)
- **Combined approach** - Rule-based + ML predictions
- **Comprehensive breakdowns** - 6 duplicate types with detailed analysis

## Configuration

### Celery Settings
```python
# settings.py
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
CELERY_TASK_ALWAYS_EAGER = False  # Enable background processing
```

### Model Settings
```python
# ML model configuration
DUPLICATE_MODEL_CONFIG = {
    'min_training_samples': 50,
    'duplicate_threshold': 2,
    'risk_scores': {
        'Type 1 Duplicate': 10,
        'Type 2 Duplicate': 12,
        # ... etc
    }
}
```

## Error Handling

### Upload Errors
- **Celery unavailable**: Returns error but doesn't block
- **File validation**: Immediate error response
- **Duplicate content**: Returns existing results immediately

### Model Errors
- **Training failure**: Graceful fallback to rule-based detection
- **Prediction errors**: Continues with rule-based results
- **Model not trained**: Uses only rule-based detection

## Monitoring

### Job Status Tracking
- **PENDING**: Job created, waiting for processing
- **PROCESSING**: Currently being processed
- **COMPLETED**: Successfully completed
- **FAILED**: Processing failed with error details

### Model Performance
- **Training accuracy**: Model performance metrics
- **Prediction confidence**: ML confidence scores
- **Duplicate breakdown**: Distribution across 6 types

## Future Enhancements

1. **Model persistence** - Save trained models to disk
2. **Incremental training** - Update model with new data
3. **Real-time predictions** - Stream processing capabilities
4. **Advanced features** - More sophisticated ML algorithms
5. **Performance optimization** - Faster training and prediction

## Troubleshooting

### Common Issues

1. **Upload not processing**
   - Check Celery worker status
   - Verify job status endpoint
   - Check logs for errors

2. **Model not training**
   - Ensure sufficient data (minimum 50 transactions)
   - Check scikit-learn installation
   - Verify feature extraction

3. **ML predictions missing**
   - Confirm model is trained
   - Check model status endpoint
   - Verify transaction data format

### Debug Endpoints
```
GET /api/ml-model-training/model_info/  # Model status
GET /api/file-processing-jobs/          # All jobs
GET /api/duplicate-anomalies/           # Duplicate analysis
``` 