# Frequent Model Training Implementation

This document describes the implementation of frequent model training for the Expense Fraud Analytics System.

## Overview

The system now includes automatic and frequent model training to ensure fraud detection models stay current with new data and maintain high accuracy over time.

## Key Features

### 1. Auto-Training on Sheet Upload
- **Trigger**: When new expense sheets are uploaded via API
- **Behavior**: Automatically checks if retraining is needed and trains models if conditions are met
- **Response**: Returns training status in the upload response

```python
# Example API Response
{
    "message": "Expenses uploaded successfully.",
    "training_status": "Models auto-trained",
    "sheet_info": {...},
    "data": [...]
}
```

### 2. Auto-Training Before Analysis
- **Trigger**: Before analyzing any expense sheet
- **Behavior**: Checks for new data and retrains if needed before running analysis
- **Response**: Includes training status in analysis results

```python
# Example Analysis Response
{
    "message": "Sheet analysis completed successfully",
    "training_status": "Models auto-trained before analysis",
    "analysis_summary": {...}
}
```

### 3. Scheduled Training Command
- **Command**: `python manage.py scheduled_training`
- **Options**:
  - `--force`: Force retraining regardless of new data
  - `--days N`: Look back N days for new data (default: 1)
- **Use Cases**: Cron jobs, scheduled tasks, manual execution

```bash
# Check for new data in last 7 days
python manage.py scheduled_training --days 7

# Force retraining
python manage.py scheduled_training --force
```

### 4. Performance-Based Training
- **Trigger**: Automatic performance evaluation
- **Criteria**: Anomaly rate too high (>30%) or too low (<5%)
- **Behavior**: Automatically retrains models when performance degrades

### 5. Configurable Training Thresholds
- **Auto-train threshold**: 10 new sheets (configurable)
- **Minimum training data**: 50 expenses (configurable)
- **Retrain interval**: 24 hours (configurable)
- **Performance threshold**: 10% anomaly rate (configurable)

## Implementation Details

### Training Configuration

```python
self.training_config = {
    'auto_train_threshold': 10,  # New sheets before auto-training
    'min_training_data': 50,     # Minimum expenses for training
    'retrain_interval_hours': 24, # Hours between retrains
    'performance_threshold': 0.1  # Anomaly rate threshold for retraining
}
```

### Key Methods

#### `should_retrain()`
Checks if models should be retrained based on new data:
- Compares last training time with new sheet creation dates
- Returns True if threshold of new sheets is reached

#### `auto_train_if_needed()`
Main auto-training method:
- Calls `should_retrain()` to check conditions
- Trains models if needed
- Updates last training timestamp

#### `evaluate_model_performance()`
Evaluates model performance on recent data:
- Analyzes anomaly rates in last 30 days
- Suggests retraining if performance is poor

## API Endpoints with Training

### 1. Sheet Upload (`POST /expenses/upload/`)
```python
# Auto-trains after successful upload
response = {
    "training_status": "Models auto-trained" | "No training needed" | "Training failed: error"
}
```

### 2. Sheet Analysis (`POST /sheets/{id}/analyze/`)
```python
# Auto-trains before analysis
response = {
    "training_status": "Models auto-trained before analysis" | "No training needed"
}
```

### 3. Bulk Analysis (`POST /analysis/bulk/`)
```python
# Auto-trains before bulk analysis
response = {
    "training_status": "Models auto-trained before bulk analysis" | "No training needed"
}
```

### 4. Model Training (`POST /analysis/train/`)
```python
# Manual training endpoint
response = {
    "message": "Models trained successfully",
    "sheets_used": 15,
    "models_trained": ["isolation_forest", "random_forest"]
}
```

## Testing

Run the comprehensive test script:

```bash
python test_frequent_training.py
```

This tests:
- Auto-training on sheet upload
- Auto-training before analysis
- Scheduled training command
- Force training command
- Performance-based training
- Training configuration
- Bulk analysis with training

## Deployment Considerations

### 1. Scheduled Training
Set up cron jobs for regular training:

```bash
# Daily training check
0 2 * * * cd /path/to/analytics && python manage.py scheduled_training --days 1

# Weekly force training
0 3 * * 0 cd /path/to/analytics && python manage.py scheduled_training --force
```

### 2. Performance Monitoring
Monitor training logs and performance metrics:
- Training frequency
- Model accuracy over time
- Anomaly detection rates
- Training success/failure rates

### 3. Resource Management
- Training can be resource-intensive
- Consider running during off-peak hours
- Monitor memory and CPU usage during training

## Benefits

### 1. Improved Accuracy
- Models stay current with new patterns
- Adapts to changing fraud patterns
- Maintains high detection rates

### 2. Reduced Manual Intervention
- Automatic training reduces manual work
- Self-maintaining system
- Proactive performance management

### 3. Better User Experience
- Users get immediate feedback on training status
- Transparent about model updates
- Confidence in analysis results

### 4. Scalability
- Handles growing datasets automatically
- Configurable thresholds for different environments
- Performance-based adaptation

## Troubleshooting

### Common Issues

1. **Training Fails Due to Insufficient Data**
   - Ensure at least 2 sheets with 5+ expenses each
   - Check data quality and completeness

2. **Performance Degradation**
   - Review anomaly rate thresholds
   - Check for data drift or new patterns
   - Consider adjusting training configuration

3. **High Resource Usage**
   - Schedule training during off-peak hours
   - Consider reducing training frequency
   - Monitor system resources

### Debug Commands

```bash
# Check training status
python manage.py scheduled_training --days 30

# Force retraining
python manage.py scheduled_training --force

# Check model files
ls -la trained_models/

# Test specific functionality
python test_frequent_training.py
```

## Future Enhancements

1. **Incremental Learning**: Update models without full retraining
2. **A/B Testing**: Compare model versions
3. **Model Versioning**: Track model performance over time
4. **Advanced Performance Metrics**: More sophisticated evaluation criteria
5. **Distributed Training**: Handle larger datasets efficiently

## Conclusion

The frequent training implementation ensures that the fraud detection system remains accurate and effective as new data is added. The combination of automatic triggers, scheduled training, and performance monitoring creates a robust, self-maintaining system that adapts to changing patterns and maintains high detection accuracy. 