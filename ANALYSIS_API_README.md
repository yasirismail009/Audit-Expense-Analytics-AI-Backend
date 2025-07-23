# Analysis API Documentation

## Overview

The Analysis API provides comprehensive access to analysis results for files, processing jobs, transactions, and ML models. It includes both general analysis endpoints and specific file-by-file analysis capabilities.

## API Endpoints

### 1. General Analysis API

**Endpoint:** `GET /api/analysis/`

**Description:** Get comprehensive analysis data across all entities (files, jobs, transactions, ML models)

**Query Parameters:**
- `type` (optional): Filter by analysis type
  - `file` - File analysis only
  - `job` - Processing job analysis only  
  - `transaction` - Transaction analysis only
  - `ml` - ML model analysis only
  - `all` (default) - All analysis types
- `file_id` (optional): Get analysis for specific file
- `job_id` (optional): Get analysis for specific job
- `transaction_id` (optional): Get analysis for specific transaction
- `date_from` (optional): Filter by start date (YYYY-MM-DD)
- `date_to` (optional): Filter by end date (YYYY-MM-DD)
- `anomaly_type` (optional): Filter by anomaly type (amount, timing, user, account, pattern)
- `risk_level` (optional): Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)

**Example Requests:**

```bash
# Get all analysis types
curl -X GET "http://localhost:8000/api/analysis/"

# Get file analysis only
curl -X GET "http://localhost:8000/api/analysis/?type=file"

# Get job analysis with date filter
curl -X GET "http://localhost:8000/api/analysis/?type=job&date_from=2024-01-01&date_to=2024-12-31"

# Get transaction analysis with risk filter
curl -X GET "http://localhost:8000/api/analysis/?type=transaction&risk_level=HIGH"
```

**Response Structure:**
```json
{
  "files_analysis": {
    "total_files": 10,
    "files_by_status": {"COMPLETED": 8, "PENDING": 2},
    "files_by_client": {"Client A": 5, "Client B": 5},
    "processing_summary": {
      "total_processed": 10000,
      "total_failed": 100,
      "success_rate": 99.0
    }
  },
  "jobs_analysis": {
    "total_jobs": 10,
    "jobs_by_status": {"COMPLETED": 8, "PROCESSING": 2},
    "anomaly_requests": {
      "total_with_anomalies": 5,
      "anomaly_types_requested": {"duplicate": 3, "backdated": 2}
    }
  },
  "transactions_analysis": {
    "total_transactions": 10000,
    "analyzed_transactions": 5000,
    "risk_distribution": {"LOW": 4000, "MEDIUM": 800, "HIGH": 150, "CRITICAL": 50},
    "anomaly_distribution": {
      "amount_anomalies": 200,
      "timing_anomalies": 150,
      "user_anomalies": 100
    }
  },
  "ml_analysis": {
    "total_models": 5,
    "models_by_type": {"isolation_forest": 2, "random_forest": 2, "ensemble": 1},
    "performance_summary": {
      "avg_accuracy": 0.85,
      "best_performing_model": {
        "id": "uuid",
        "session_name": "Training Session 1",
        "model_type": "ensemble",
        "accuracy": 0.92
      }
    }
  }
}
```

### 2. File Analysis by ID API

**Endpoint:** `GET /api/analysis/file/{file_id}/`

**Description:** Get comprehensive analysis for a specific file by its UUID

**Path Parameters:**
- `file_id` (required): UUID of the file to analyze

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/analysis/file/b3369705-8b54-4db0-9152-b3d17a503ae7/"
```

**Response Structure:**
```json
{
  "file_info": {
    "id": "b3369705-8b54-4db0-9152-b3d17a503ae7",
    "file_name": "sample_data.csv",
    "file_size": 1024000,
    "engagement_id": "ENG-2024-001",
    "client_name": "Sample Client",
    "company_name": "Sample Company",
    "fiscal_year": 2024,
    "audit_start_date": "2024-01-01",
    "audit_end_date": "2024-12-31",
    "status": "COMPLETED",
    "uploaded_at": "2024-01-15T10:30:00Z",
    "processed_at": "2024-01-15T10:35:00Z",
    "total_records": 1000,
    "processed_records": 995,
    "failed_records": 5,
    "success_rate": 99.5
  },
  "processing_job": {
    "job_id": "job-uuid-here",
    "status": "COMPLETED",
    "run_anomalies": true,
    "requested_anomalies": ["duplicate", "backdated"],
    "created_at": "2024-01-15T10:30:00Z",
    "started_at": "2024-01-15T10:30:05Z",
    "completed_at": "2024-01-15T10:35:00Z",
    "processing_duration": 295.5,
    "file_hash": "sha256-hash-here",
    "is_duplicate_content": false
  },
  "analytics_results": {
    "trial_balance": {
      "total_debits": 1000000.00,
      "total_credits": 1000000.00,
      "balance": 0.00,
      "account_summaries": [...]
    },
    "transaction_entries": {
      "total_transactions": 1000,
      "unique_documents": 500,
      "date_range": {...}
    },
    "gl_account_summaries": [...]
  },
  "anomaly_results": {
    "duplicate_analysis": {
      "total_duplicates": 10,
      "duplicate_amount": 50000.00,
      "duplicate_transactions": [...]
    },
    "backdated_analysis": {
      "total_backdated": 5,
      "backdated_amount": 25000.00,
      "backdated_transactions": [...]
    }
  },
  "transactions_summary": {
    "total_transactions": 1000,
    "total_amount": 1000000.00,
    "unique_accounts": 50,
    "unique_users": 20,
    "unique_documents": 500,
    "date_range": {
      "min_date": "2024-01-01",
      "max_date": "2024-12-31"
    },
    "amount_range": {
      "min_amount": 100.00,
      "max_amount": 50000.00
    }
  },
  "analysis_sessions": [
    {
      "id": "session-uuid",
      "session_name": "Risk Analysis Session 1",
      "description": "Comprehensive risk analysis",
      "status": "COMPLETED",
      "total_transactions": 1000,
      "total_amount": 1000000.00,
      "flagged_transactions": 50,
      "high_value_transactions": 25,
      "flag_rate": 5.0
    }
  ],
  "transaction_analyses": [
    {
      "transaction_id": "transaction-uuid",
      "document_number": "DOC-001",
      "posting_date": "2024-06-15",
      "gl_account": "1000",
      "amount": 50000.00,
      "user_name": "USER001",
      "risk_score": 85.5,
      "risk_level": "HIGH",
      "amount_anomaly": true,
      "timing_anomaly": false,
      "user_anomaly": true,
      "account_anomaly": false,
      "pattern_anomaly": false,
      "analysis_details": {
        "amount_factor": 0.8,
        "timing_factor": 0.2,
        "user_factor": 0.9
      }
    }
  ],
  "risk_summary": {
    "total_analyzed": 1000,
    "risk_distribution": {
      "LOW": 800,
      "MEDIUM": 150,
      "HIGH": 40,
      "CRITICAL": 10
    },
    "anomaly_distribution": {
      "amount_anomalies": 50,
      "timing_anomalies": 30,
      "user_anomalies": 25,
      "account_anomalies": 15,
      "pattern_anomalies": 10
    },
    "top_high_risk_transactions": [
      {
        "transaction_id": "transaction-uuid",
        "document_number": "DOC-001",
        "risk_score": 95.5,
        "risk_level": "CRITICAL",
        "amount": 100000.00,
        "user_name": "USER001"
      }
    ]
  },
  "ml_model_info": {
    "has_trained_models": true,
    "total_training_sessions": 5,
    "recent_training_sessions": [
      {
        "id": "model-uuid",
        "session_name": "ML Training Session 1",
        "model_type": "ensemble",
        "status": "COMPLETED",
        "training_data_size": 10000,
        "performance_metrics": {
          "accuracy": 0.92,
          "precision": 0.89,
          "recall": 0.91
        },
        "created_at": "2024-01-10T09:00:00Z"
      }
    ]
  }
}
```

## Usage Examples

### Python Examples

```python
import requests

# Get all analysis data
response = requests.get("http://localhost:8000/api/analysis/")
all_analysis = response.json()

# Get file analysis only
response = requests.get("http://localhost:8000/api/analysis/?type=file")
file_analysis = response.json()

# Get analysis for specific file
file_id = "b3369705-8b54-4db0-9152-b3d17a503ae7"
response = requests.get(f"http://localhost:8000/api/analysis/file/{file_id}/")
file_specific_analysis = response.json()

# Get high-risk transactions
response = requests.get("http://localhost:8000/api/analysis/?type=transaction&risk_level=HIGH")
high_risk_analysis = response.json()
```

### JavaScript Examples

```javascript
// Get all analysis data
fetch('/api/analysis/')
  .then(response => response.json())
  .then(data => {
    console.log('Files:', data.files_analysis);
    console.log('Jobs:', data.jobs_analysis);
    console.log('Transactions:', data.transactions_analysis);
    console.log('ML Models:', data.ml_analysis);
  });

// Get file analysis by ID
const fileId = 'b3369705-8b54-4db0-9152-b3d17a503ae7';
fetch(`/api/analysis/file/${fileId}/`)
  .then(response => response.json())
  .then(data => {
    console.log('File Info:', data.file_info);
    console.log('Risk Summary:', data.risk_summary);
    console.log('Analytics Results:', data.analytics_results);
  });
```

### cURL Examples

```bash
# Get comprehensive analysis
curl -X GET "http://localhost:8000/api/analysis/" \
  -H "Content-Type: application/json"

# Get file analysis by ID
curl -X GET "http://localhost:8000/api/analysis/file/b3369705-8b54-4db0-9152-b3d17a503ae7/" \
  -H "Content-Type: application/json"

# Get high-risk transactions with date filter
curl -X GET "http://localhost:8000/api/analysis/?type=transaction&risk_level=HIGH&date_from=2024-01-01&date_to=2024-12-31" \
  -H "Content-Type: application/json"
```

## Error Handling

### Common Error Responses

**400 Bad Request:**
```json
{
  "error": "Invalid file ID format"
}
```

**404 Not Found:**
```json
{
  "error": "File not found"
}
```

**500 Internal Server Error:**
```json
{
  "error": "Error retrieving analysis: [specific error message]"
}
```

## Testing

Use the provided test script to verify the API functionality:

```bash
python test_file_analysis_api.py
```

This script will:
1. Test the general analysis API with different type filters
2. Test the file analysis by ID API
3. Test error handling with invalid file IDs
4. Display comprehensive results

## Data Relationships

The analysis APIs provide data from the following models:

- **DataFile**: File upload information and metadata
- **FileProcessingJob**: Processing status and results
- **SAPGLPosting**: Transaction data
- **TransactionAnalysis**: Risk analysis results
- **AnalysisSession**: Analysis session metadata
- **MLModelTraining**: ML model training information

## Performance Considerations

- Large datasets may take time to process
- Use specific filters to reduce response size
- Consider pagination for large result sets
- Cache frequently accessed analysis results

## Security Notes

- File IDs are validated as UUIDs
- Access control should be implemented based on user permissions
- Sensitive data is filtered out in responses
- File content is not stored, only metadata and analysis results 