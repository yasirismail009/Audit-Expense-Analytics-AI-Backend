# Comprehensive Analytics API Documentation

## Overview

The Comprehensive Analytics API (`/api/comprehensive-analytics/file/<file_id>/`) provides a complete analysis of uploaded financial data files, including transaction summaries, anomaly detection, risk assessments, and processing statistics.

## API Endpoint

```
GET /api/comprehensive-analytics/file/{file_id}/
```

**Parameters:**
- `file_id` (UUID): The unique identifier of the uploaded file

**Response Format:** JSON

## Complete Data Structure

The API response contains the following comprehensive data structure:

### 1. File Information (`file_info`)

Basic information about the uploaded file and processing status.

```json
{
  "file_info": {
    "id": "uuid-string",
    "file_name": "string",
    "client_name": "string",
    "company_name": "string",
    "fiscal_year": "integer",
    "status": "string (PENDING|PROCESSING|COMPLETED|FAILED|PARTIAL)",
    "total_records": "integer",
    "processed_records": "integer",
    "failed_records": "integer",
    "uploaded_at": "ISO datetime string",
    "processed_at": "ISO datetime string or null"
  }
}
```

**Key Metrics:**
- **Processing Efficiency**: `(processed_records / total_records) * 100`
- **Failure Rate**: `(failed_records / total_records) * 100`

### 2. Transactions Summary (`transactions_summary`)

High-level statistics about the processed transactions.

```json
{
  "transactions_summary": {
    "total_count": "integer",
    "unique_accounts": "integer",
    "unique_users": "integer",
    "total_amount": "float",
    "date_range": {
      "min_date": "ISO date string or null",
      "max_date": "ISO date string or null"
    }
  }
}
```

**Calculated Metrics:**
- **Average Transaction Amount**: `total_amount / total_count`
- **Account Density**: `(unique_accounts / total_count) * 100`
- **User Density**: `(unique_users / total_count) * 100`

### 3. Processing Jobs (`processing_jobs`)

Information about background processing jobs and their results.

```json
{
  "processing_jobs": [
    {
      "id": "uuid-string",
      "status": "string (PENDING|QUEUED|PROCESSING|COMPLETED|FAILED|CELERY_ERROR|SKIPPED)",
      "run_anomalies": "boolean",
      "requested_anomalies": ["array of anomaly types"],
      "analytics_results": "object (default analytics results)",
      "anomaly_results": "object (anomaly detection results)",
      "processing_duration": "float (seconds)",
      "started_at": "ISO datetime string or null",
      "completed_at": "ISO datetime string or null",
      "error_message": "string or null"
    }
  ]
}
```

**Job Statistics:**
- **Total Jobs**: Count of all processing jobs
- **Success Rate**: `(completed_jobs / total_jobs) * 100`
- **Average Processing Time**: Mean processing duration across all jobs

### 4. Transaction Analyses (`transaction_analyses`)

Individual transaction-level risk analysis and anomaly detection.

```json
{
  "transaction_analyses": [
    {
      "transaction_id": "uuid-string",
      "risk_score": "float (0-100)",
      "risk_level": "string (LOW|MEDIUM|HIGH|CRITICAL)",
      "amount_anomaly": "boolean",
      "timing_anomaly": "boolean",
      "user_anomaly": "boolean",
      "account_anomaly": "boolean",
      "pattern_anomaly": "boolean",
      "created_at": "ISO datetime string"
    }
  ]
}
```

**Risk Analysis Statistics:**
- **Average Risk Score**: Mean risk score across all transactions
- **Risk Level Distribution**: Percentage breakdown by risk level
- **Anomaly Flag Summary**: Count and percentage of each anomaly type

### 5. Real-Time Analytics (`real_time_analytics`)

Comprehensive anomaly detection and risk assessment results.

#### 5.1 Overall Risk Assessment

```json
{
  "overall_risk": {
    "score": "float (0-100)",
    "level": "string (LOW|MEDIUM|HIGH|CRITICAL)",
    "high_risk_anomalies": "integer"
  }
}
```

#### 5.2 Anomaly Detection by Type

##### Duplicates Analysis
```json
{
  "duplicates": {
    "count": "integer",
    "details": "array of duplicate transactions",
    "summary": "object with detailed breakdown",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

**Duplicate Summary Includes:**
- Total duplicate groups
- Total duplicate transactions
- Total amount involved
- Type breakdown (exact, similar, etc.)
- User breakdown
- Financial statement line breakdown

##### Backdated Entries
```json
{
  "backdated_entries": {
    "count": "integer",
    "details": "array of backdated transactions",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

##### User Anomalies
```json
{
  "user_anomalies": {
    "count": "integer",
    "details": "array of anomalous user activities",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

##### Closing Entries
```json
{
  "closing_entries": {
    "count": "integer",
    "details": "array of closing entries",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

##### Unusual Days
```json
{
  "unusual_days": {
    "count": "integer",
    "details": "array of unusual day activities",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

##### Holiday Entries
```json
{
  "holiday_entries": {
    "count": "integer",
    "details": "array of holiday transactions",
    "risk_score": "float (0-100)",
    "risk_level": "string"
  }
}
```

## Risk Scoring Methodology

### Risk Levels
- **LOW**: 0-39 (Green)
- **MEDIUM**: 40-69 (Yellow)
- **HIGH**: 70-89 (Orange)
- **CRITICAL**: 90-100 (Red)

### Risk Weights by Anomaly Type
- **Duplicates**: 25 points (High risk - potential fraud)
- **Backdated Entries**: 20 points (High risk - manipulation)
- **User Anomalies**: 15 points (Medium-high risk)
- **Closing Entries**: 10 points (Medium risk)
- **Unusual Days**: 8 points (Medium-low risk)
- **Holiday Entries**: 5 points (Low risk)

## Additional Analytics Features

### Monthly Trends Analysis
- Transaction volume by month
- Average transaction amounts by month
- Monthly activity patterns

### Amount Distribution Analysis
- Transaction amounts grouped by ranges (0-1K, 1K-10K, 10K-100K, 100K-1M, 1M+)
- Percentage distribution across amount ranges

### Department/Profit Center Analysis
- Expenses by department/profit center
- Top spending departments
- Average transaction amounts by department

### GL Account Analysis
- Top GL accounts by transaction volume
- Trial balance summary
- Account activity patterns

### User Analysis
- Top users by transaction amount
- User activity patterns
- Employee expense analysis

### Document Type Analysis
- Transaction distribution by document type
- Document type patterns and anomalies

## Usage Examples

### Python Script Example
```python
import requests

# Get comprehensive analytics for a file
file_id = "your-file-uuid"
url = f"http://localhost:8000/api/comprehensive-analytics/file/{file_id}/"
response = requests.get(url)
analytics_data = response.json()

# Access specific data
file_info = analytics_data['file_info']
risk_score = analytics_data['real_time_analytics']['overall_risk']['score']
duplicate_count = analytics_data['real_time_analytics']['duplicates']['count']
```

### cURL Example
```bash
curl -X GET "http://localhost:8000/api/comprehensive-analytics/file/your-file-uuid/" \
     -H "Content-Type: application/json"
```

## Error Handling

The API returns appropriate HTTP status codes:
- **200**: Success
- **400**: Invalid file ID format
- **404**: File not found
- **500**: Internal server error

Error responses include descriptive messages:
```json
{
  "error": "Error message description"
}
```

## Performance Considerations

- **Response Time**: Varies based on file size and complexity
- **Timeout**: 30 seconds recommended
- **Data Size**: Large files may generate substantial response payloads
- **Caching**: Consider caching results for frequently accessed files

## Data Export

The API response can be saved as JSON for further analysis:
```python
import json

with open('analytics_export.json', 'w') as f:
    json.dump(analytics_data, f, indent=2, default=str)
```

## Integration Points

This comprehensive analytics data can be integrated with:
- Business Intelligence tools
- Risk management systems
- Audit workflows
- Financial reporting systems
- Compliance monitoring platforms

## Security Considerations

- File access is restricted to authenticated users
- File IDs are UUIDs for security
- Sensitive data is not exposed in the API response
- All timestamps are in ISO format for consistency 