# Analytics Dashboard & Detailed Anomaly APIs

## Overview

The Analytics Dashboard and Detailed Anomaly APIs provide comprehensive analytics, statistics, and insights for financial data analysis, anomaly detection, and risk assessment. These APIs deliver the data needed to create powerful dashboard visualizations similar to the ones shown in the reference images.

## API Endpoints

### 1. Analytics Dashboard API

**Endpoint:** `GET /api/analytics/dashboard/`

**Description:** Comprehensive analytics dashboard with detailed statistics, trends, and visualizations

**Query Parameters:**
- `date_from` (optional): Filter by start date (YYYY-MM-DD)
- `date_to` (optional): Filter by end date (YYYY-MM-DD)
- `client_name` (optional): Filter by client name
- `fiscal_year` (optional): Filter by fiscal year
- `file_id` (optional): Filter by specific file ID (UUID)

**Example Requests:**

```bash
# Get all analytics data
curl -X GET "http://localhost:8000/api/analytics/dashboard/"

# Get analytics for specific date range
curl -X GET "http://localhost:8000/api/analytics/dashboard/?date_from=2024-01-01&date_to=2024-12-31"

# Get analytics for specific client
curl -X GET "http://localhost:8000/api/analytics/dashboard/?client_name=Sample%20Client"

# Get analytics for specific file
curl -X GET "http://localhost:8000/api/analytics/dashboard/?file_id=12345678-1234-1234-1234-123456789012"
```

**Response Structure:**

```json
{
  "overall_analytics": {
    "summary_statistics": {
      "total_transactions": 10000,
      "total_amount": 1000000000.00,
      "unique_users": 50,
      "unique_accounts": 100,
      "unique_documents": 5000,
      "unique_profit_centers": 25,
      "average_transaction_amount": 100000.00,
      "max_transaction_amount": 5000000.00,
      "min_transaction_amount": 100.00
    },
    
    "monthly_trend_analysis": {
      "data": [
        {
          "month": "2024-01",
          "count": 850,
          "total_amount": 85000000.00,
          "avg_amount": 100000.00
        }
      ],
      "summary": {
        "total_months": 12,
        "avg_monthly_transactions": 833,
        "avg_monthly_amount": 83333333.33
      }
    },
    
    "amount_distribution": [
      {
        "range": "0-1K",
        "count": 2000,
        "percentage": 20.0
      },
      {
        "range": "1K-10K",
        "count": 3000,
        "percentage": 30.0
      },
      {
        "range": "10K-100K",
        "count": 3000,
        "percentage": 30.0
      },
      {
        "range": "100K-1M",
        "count": 1500,
        "percentage": 15.0
      },
      {
        "range": "1M+",
        "count": 500,
        "percentage": 5.0
      }
    ],
    
    "department_expenses": [
      {
        "department": "IT",
        "count": 500,
        "total_amount": 50000000.00,
        "avg_amount": 100000.00
      }
    ],
    
    "top_gl_accounts": {
      "summary": {
        "total_accounts": 100,
        "total_trial_balance": 1000000000.00,
        "total_trading_equity": 1000000000.00,
        "total_debits": 1000000000.00,
        "total_credits": 0.00,
        "currency": "SAR"
      },
      "accounts": [
        {
          "account": "131005",
          "amount": 157900000.00,
          "transactions": 38,
          "trial_balance": 157900000.00
        }
      ]
    },
    
    "top_users_by_amount": [
      {
        "user": "M.ALJOHANI",
        "amount": 82300000.00,
        "transactions": 16,
        "avg_amount": 5143750.00
      }
    ],
    
    "employee_expenses": {
      "employees": [
        {
          "name": "M.ALJOHANI",
          "amount": 82300000.00
        }
      ],
      "total_employees": 5,
      "total_expenses": 275700000.00
    },
    
    "transaction_type_analysis": {
      "debit_transactions": 5000,
      "credit_transactions": 5000,
      "debit_amount": 1000000000.00,
      "credit_amount": 1000000000.00
    },
    
    "document_type_analysis": [
      {
        "document_type": "SA",
        "count": 2000,
        "total_amount": 200000000.00
      }
    ],
    
    "fiscal_year_analysis": [
      {
        "fiscal_year": 2024,
        "count": 10000,
        "total_amount": 1000000000.00,
        "avg_amount": 100000.00
      }
    ]
  },
  
  "anomaly_analytics": {
    "summary": {
      "total_analyzed": 10000,
      "flagged_transactions": 500,
      "flag_rate": 5.0,
      "overall_risk_score": 25.5
    },
    
    "risk_distribution": {
      "LOW": 8000,
      "MEDIUM": 1500,
      "HIGH": 400,
      "CRITICAL": 100
    },
    
    "anomaly_types": {
      "amount_anomalies": 200,
      "timing_anomalies": 150,
      "user_anomalies": 100,
      "account_anomalies": 50,
      "pattern_anomalies": 25
    },
    
    "duplicate_analysis": {
      "total_duplicates": 50,
      "duplicate_amount": 2500000.00,
      "duplicate_transactions": [...],
      "duplicate_rate": 0.5
    },
    
    "backdated_analysis": {
      "total_backdated": 30,
      "backdated_amount": 1500000.00,
      "backdated_transactions": [...],
      "backdated_rate": 0.3
    },
    
    "user_anomaly_analysis": {
      "total_anomalous_users": 10,
      "anomalous_users": [...]
    },
    
    "top_anomalies": [...],
    "anomaly_trends": {...},
    "ml_model_performance": {...}
  },
  
  "file_analytics": {
    "file_summary": {
      "total_files": 25,
      "completed_files": 20,
      "processing_files": 3,
      "failed_files": 2,
      "total_records_processed": 100000,
      "total_records_failed": 500,
      "success_rate": 99.5
    },
    
    "client_analysis": [
      {
        "client_name": "Sample Client",
        "file_count": 10,
        "total_records": 50000,
        "total_amount": 500000000.00
      }
    ],
    
    "processing_performance": {
      "avg_processing_time": 45.5,
      "processing_jobs_by_status": {
        "COMPLETED": 20,
        "PROCESSING": 3,
        "FAILED": 2
      }
    }
  },
  
  "risk_analytics": {
    "summary": {
      "total_analyzed": 10000,
      "high_risk_count": 500,
      "critical_risk_count": 100,
      "overall_risk_score": 25.5
    },
    
    "risk_distribution": {
      "LOW": 8000,
      "MEDIUM": 1500,
      "HIGH": 400,
      "CRITICAL": 100
    },
    
    "risk_percentages": {
      "LOW": 80.0,
      "MEDIUM": 15.0,
      "HIGH": 4.0,
      "CRITICAL": 1.0
    },
    
    "high_risk_transactions": [...],
    "risk_trends": {...}
  },
  
  "filters_applied": {
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "client_name": "Sample Client",
    "fiscal_year": 2024,
    "file_id": "12345678-1234-1234-1234-123456789012"
  }
}
```

### 2. Detailed Anomaly Analysis API

**Endpoint:** `GET /api/analytics/anomalies/`

**Description:** Detailed anomaly analysis with comprehensive filtering and breakdown options

**Query Parameters:**
- `type` (optional): Filter by anomaly type (amount, timing, user, account, pattern)
- `risk_level` (optional): Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
- `date_from` (optional): Filter by start date (YYYY-MM-DD)
- `date_to` (optional): Filter by end date (YYYY-MM-DD)
- `user_name` (optional): Filter by user name
- `gl_account` (optional): Filter by GL account

**Example Requests:**

```bash
# Get all anomalies
curl -X GET "http://localhost:8000/api/analytics/anomalies/"

# Get amount anomalies only
curl -X GET "http://localhost:8000/api/analytics/anomalies/?type=amount"

# Get high-risk anomalies
curl -X GET "http://localhost:8000/api/analytics/anomalies/?risk_level=HIGH"

# Get anomalies for specific user
curl -X GET "http://localhost:8000/api/analytics/anomalies/?user_name=M.ALJOHANI"

# Get anomalies with date filter
curl -X GET "http://localhost:8000/api/analytics/anomalies/?date_from=2024-01-01&date_to=2024-12-31"
```

**Response Structure:**

```json
{
  "summary": {
    "total_anomalies": 500,
    "total_amount": 25000000.00,
    "avg_risk_score": 75.5,
    "high_risk_count": 150
  },
  
  "anomaly_breakdown": {
    "amount_anomalies": {
      "count": 200,
      "amount": 10000000.00,
      "avg_risk_score": 85.2
    },
    "timing_anomalies": {
      "count": 150,
      "amount": 7500000.00,
      "avg_risk_score": 70.8
    },
    "user_anomalies": {
      "count": 100,
      "amount": 5000000.00,
      "avg_risk_score": 65.3
    },
    "account_anomalies": {
      "count": 50,
      "amount": 2500000.00,
      "avg_risk_score": 60.1
    },
    "pattern_anomalies": {
      "count": 25,
      "amount": 1250000.00,
      "avg_risk_score": 55.7
    }
  },
  
  "risk_level_breakdown": {
    "LOW": {
      "count": 100,
      "amount": 5000000.00
    },
    "MEDIUM": {
      "count": 150,
      "amount": 7500000.00
    },
    "HIGH": {
      "count": 200,
      "amount": 10000000.00
    },
    "CRITICAL": {
      "count": 50,
      "amount": 2500000.00
    }
  },
  
  "top_anomalous_users": [
    {
      "transaction__user_name": "M.ALJOHANI",
      "count": 25,
      "total_amount": 2500000.00,
      "avg_risk_score": 85.5
    }
  ],
  
  "top_anomalous_accounts": [
    {
      "transaction__gl_account": "131005",
      "count": 15,
      "total_amount": 1500000.00,
      "avg_risk_score": 80.2
    }
  ],
  
  "anomaly_details": [
    {
      "transaction__document_number": "DOC-001",
      "transaction__posting_date": "2024-06-15",
      "transaction__gl_account": "131005",
      "transaction__amount_local_currency": 500000.00,
      "transaction__user_name": "M.ALJOHANI",
      "transaction__text": "Suspicious transaction",
      "risk_score": 95.5,
      "risk_level": "CRITICAL",
      "amount_anomaly": true,
      "timing_anomaly": false,
      "user_anomaly": true,
      "account_anomaly": false,
      "pattern_anomaly": false,
      "analysis_details": {
        "amount_factor": 0.9,
        "timing_factor": 0.1,
        "user_factor": 0.8
      }
    }
  ],
  
  "anomaly_trends": {
    "monthly_trends": [
      {
        "month": "2024-01",
        "count": 50,
        "total_amount": 2500000.00,
        "avg_risk_score": 75.5
      }
    ],
    "anomaly_type_trends": {
      "amount_anomalies": 200,
      "timing_anomalies": 150,
      "user_anomalies": 100,
      "account_anomalies": 50,
      "pattern_anomalies": 25
    }
  },
  
  "ml_predictions": {
    "model_confidence": 0.85,
    "prediction_accuracy": 0.92,
    "false_positive_rate": 0.08,
    "false_negative_rate": 0.05
  }
}
```

### 3. File Analytics by ID API

**Endpoint:** `GET /api/analytics/file/{file_id}/`

**Description:** Comprehensive analytics for a specific file by its ID, including file information, processing details, transaction analysis, and anomaly detection results

**Path Parameters:**
- `file_id` (required): UUID of the file to analyze

**Example Requests:**

```bash
# Get analytics for a specific file
curl -X GET "http://localhost:8000/api/analytics/file/12345678-1234-1234-1234-123456789012/"

# Get analytics for a file (alternative format)
curl -X GET "http://localhost:8000/api/analytics/file/12345678-1234-1234-1234-123456789012"
```

**Response Structure:**

```json
{
  "file_info": {
    "id": "12345678-1234-1234-1234-123456789012",
    "file_name": "Data For DA.csv",
    "file_size": 1024000,
    "engagement_id": "ENG-001",
    "client_name": "Sample Client",
    "company_name": "Sample Company",
    "fiscal_year": 2024,
    "audit_start_date": "2024-01-01",
    "audit_end_date": "2024-12-31",
    "status": "COMPLETED",
    "uploaded_at": "2024-06-15T10:30:00Z",
    "processed_at": "2024-06-15T10:35:00Z",
    "error_message": null,
    "total_records": 1000,
    "processed_records": 995,
    "failed_records": 5,
    "success_rate": 99.5
  },
  
  "processing_job": {
    "job_id": "87654321-4321-4321-4321-210987654321",
    "status": "COMPLETED",
    "run_anomalies": true,
    "requested_anomalies": ["duplicate", "backdated"],
    "created_at": "2024-06-15T10:30:00Z",
    "started_at": "2024-06-15T10:30:05Z",
    "completed_at": "2024-06-15T10:35:00Z",
    "processing_duration": 295.5,
    "error_message": null,
    "file_hash": "abc123def456...",
    "is_duplicate_content": false
  },
  
  "analytics_results": {
    "trial_balance": {...},
    "transaction_entries": {...},
    "gl_account_summaries": {...}
  },
  
  "anomaly_results": {
    "duplicate_analysis": {...},
    "backdated_analysis": {...}
  },
  
  "transaction_summary": {
    "total_transactions": 1000,
    "total_amount": 100000000.00,
    "unique_accounts": 50,
    "unique_users": 25,
    "unique_documents": 500,
    "unique_profit_centers": 10,
    "average_transaction_amount": 100000.00,
    "max_transaction_amount": 5000000.00,
    "min_transaction_amount": 100.00,
    "date_range": {
      "min_date": "2024-01-01",
      "max_date": "2024-12-31"
    },
    "amount_range": {
      "min_amount": 100.00,
      "max_amount": 5000000.00
    }
  },
  
  "monthly_trend_analysis": {
    "data": [
      {
        "month": "2024-01",
        "count": 85,
        "total_amount": 8500000.00,
        "avg_amount": 100000.00
      }
    ],
    "summary": {
      "total_months": 12,
      "avg_monthly_transactions": 83,
      "avg_monthly_amount": 8333333.33
    }
  },
  
  "amount_distribution": [
    {
      "range": "0-1K",
      "count": 200,
      "percentage": 20.0
    }
  ],
  
  "department_expenses": [
    {
      "department": "IT",
      "count": 50,
      "total_amount": 5000000.00,
      "avg_amount": 100000.00
    }
  ],
  
  "top_gl_accounts": {
    "summary": {
      "total_accounts": 50,
      "total_trial_balance": 100000000.00,
      "total_trading_equity": 100000000.00,
      "total_debits": 100000000.00,
      "total_credits": 0.00,
      "currency": "SAR"
    },
    "accounts": [
      {
        "account": "131005",
        "amount": 15790000.00,
        "transactions": 38,
        "trial_balance": 15790000.00
      }
    ]
  },
  
  "top_users_by_amount": [
    {
      "user": "M.ALJOHANI",
      "amount": 8230000.00,
      "transactions": 16,
      "avg_amount": 514375.00
    }
  ],
  
  "employee_expenses": {
    "employees": [
      {
        "name": "M.ALJOHANI",
        "amount": 8230000.00
      }
    ],
    "total_employees": 5,
    "total_expenses": 27570000.00
  },
  
  "transaction_type_analysis": {
    "debit_transactions": 500,
    "credit_transactions": 500,
    "debit_amount": 100000000.00,
    "credit_amount": 100000000.00
  },
  
  "document_type_analysis": [
    {
      "document_type": "SA",
      "count": 200,
      "total_amount": 20000000.00
    }
  ],
  
  "anomaly_summary": {
    "total_analyzed": 1000,
    "flagged_transactions": 50,
    "flag_rate": 5.0,
    "overall_risk_score": 25.5
  },
  
  "risk_distribution": {
    "LOW": 800,
    "MEDIUM": 150,
    "HIGH": 40,
    "CRITICAL": 10
  },
  
  "anomaly_types": {
    "amount_anomalies": 20,
    "timing_anomalies": 15,
    "user_anomalies": 10,
    "account_anomalies": 5,
    "pattern_anomalies": 2
  },
  
  "duplicate_analysis": {
    "total_duplicates": 5,
    "duplicate_amount": 250000.00,
    "duplicate_transactions": [...],
    "duplicate_rate": 0.5
  },
  
  "backdated_analysis": {
    "total_backdated": 3,
    "backdated_amount": 150000.00,
    "backdated_transactions": [...],
    "backdated_rate": 0.3
  },
  
  "user_anomaly_analysis": {
    "total_anomalous_users": 2,
    "anomalous_users": [...]
  },
  
  "top_anomalies": [
    {
      "transaction_id": "98765432-5432-5432-5432-321098765432",
      "document_number": "DOC-001",
      "posting_date": "2024-06-15",
      "gl_account": "131005",
      "amount": 500000.00,
      "user_name": "M.ALJOHANI",
      "risk_score": 95.5,
      "risk_level": "CRITICAL",
      "amount_anomaly": true,
      "timing_anomaly": false,
      "user_anomaly": true,
      "account_anomaly": false,
      "pattern_anomaly": false,
      "analysis_details": {...}
    }
  ],
  
  "high_risk_transactions": [
    {
      "transaction_id": "98765432-5432-5432-5432-321098765432",
      "document_number": "DOC-001",
      "posting_date": "2024-06-15",
      "gl_account": "131005",
      "amount": 500000.00,
      "user_name": "M.ALJOHANI",
      "risk_score": 95.5,
      "risk_level": "CRITICAL"
    }
  ],
  
  "ml_model_info": {
    "has_trained_models": true,
    "total_training_sessions": 5,
    "recent_training_sessions": [
      {
        "id": "11111111-1111-1111-1111-111111111111",
        "session_name": "Model Training Session 1",
        "model_type": "ensemble",
        "status": "COMPLETED",
        "training_data_size": 10000,
        "performance_metrics": {
          "accuracy": 0.92,
          "precision": 0.89,
          "recall": 0.94
        },
        "created_at": "2024-06-10T09:00:00Z"
      }
    ]
  },
  
  "analysis_sessions": [
    {
      "id": "22222222-2222-2222-2222-222222222222",
      "session_name": "Analysis Session 1",
      "description": "Comprehensive analysis session",
      "status": "COMPLETED",
      "created_at": "2024-06-15T10:30:00Z",
      "started_at": "2024-06-15T10:30:05Z",
      "completed_at": "2024-06-15T10:35:00Z",
      "total_transactions": 1000,
      "total_amount": 100000000.00,
      "flagged_transactions": 50,
      "high_value_transactions": 25,
      "flag_rate": 5.0
    }
  ]
}
```

**Description:** Detailed anomaly analysis with comprehensive filtering and breakdown options

**Query Parameters:**
- `type` (optional): Filter by anomaly type (amount, timing, user, account, pattern)
- `risk_level` (optional): Filter by risk level (LOW, MEDIUM, HIGH, CRITICAL)
- `date_from` (optional): Filter by start date (YYYY-MM-DD)
- `date_to` (optional): Filter by end date (YYYY-MM-DD)
- `user_name` (optional): Filter by user name
- `gl_account` (optional): Filter by GL account

**Example Requests:**

```bash
# Get all anomalies
curl -X GET "http://localhost:8000/api/analytics/anomalies/"

# Get amount anomalies only
curl -X GET "http://localhost:8000/api/analytics/anomalies/?type=amount"

# Get high-risk anomalies
curl -X GET "http://localhost:8000/api/analytics/anomalies/?risk_level=HIGH"

# Get anomalies for specific user
curl -X GET "http://localhost:8000/api/analytics/anomalies/?user_name=M.ALJOHANI"

# Get anomalies with date filter
curl -X GET "http://localhost:8000/api/analytics/anomalies/?date_from=2024-01-01&date_to=2024-12-31"
```

**Response Structure:**

```json
{
  "summary": {
    "total_anomalies": 500,
    "total_amount": 25000000.00,
    "avg_risk_score": 75.5,
    "high_risk_count": 150
  },
  
  "anomaly_breakdown": {
    "amount_anomalies": {
      "count": 200,
      "amount": 10000000.00,
      "avg_risk_score": 85.2
    },
    "timing_anomalies": {
      "count": 150,
      "amount": 7500000.00,
      "avg_risk_score": 70.8
    },
    "user_anomalies": {
      "count": 100,
      "amount": 5000000.00,
      "avg_risk_score": 65.3
    },
    "account_anomalies": {
      "count": 50,
      "amount": 2500000.00,
      "avg_risk_score": 60.1
    },
    "pattern_anomalies": {
      "count": 25,
      "amount": 1250000.00,
      "avg_risk_score": 55.7
    }
  },
  
  "risk_level_breakdown": {
    "LOW": {
      "count": 100,
      "amount": 5000000.00
    },
    "MEDIUM": {
      "count": 150,
      "amount": 7500000.00
    },
    "HIGH": {
      "count": 200,
      "amount": 10000000.00
    },
    "CRITICAL": {
      "count": 50,
      "amount": 2500000.00
    }
  },
  
  "top_anomalous_users": [
    {
      "transaction__user_name": "M.ALJOHANI",
      "count": 25,
      "total_amount": 2500000.00,
      "avg_risk_score": 85.5
    }
  ],
  
  "top_anomalous_accounts": [
    {
      "transaction__gl_account": "131005",
      "count": 15,
      "total_amount": 1500000.00,
      "avg_risk_score": 80.2
    }
  ],
  
  "anomaly_details": [
    {
      "transaction__document_number": "DOC-001",
      "transaction__posting_date": "2024-06-15",
      "transaction__gl_account": "131005",
      "transaction__amount_local_currency": 500000.00,
      "transaction__user_name": "M.ALJOHANI",
      "transaction__text": "Suspicious transaction",
      "risk_score": 95.5,
      "risk_level": "CRITICAL",
      "amount_anomaly": true,
      "timing_anomaly": false,
      "user_anomaly": true,
      "account_anomaly": false,
      "pattern_anomaly": false,
      "analysis_details": {
        "amount_factor": 0.9,
        "timing_factor": 0.1,
        "user_factor": 0.8
      }
    }
  ],
  
  "anomaly_trends": {
    "monthly_trends": [
      {
        "month": "2024-01",
        "count": 50,
        "total_amount": 2500000.00,
        "avg_risk_score": 75.5
      }
    ],
    "anomaly_type_trends": {
      "amount_anomalies": 200,
      "timing_anomalies": 150,
      "user_anomalies": 100,
      "account_anomalies": 50,
      "pattern_anomalies": 25
    }
  },
  
  "ml_predictions": {
    "model_confidence": 0.85,
    "prediction_accuracy": 0.92,
    "false_positive_rate": 0.08,
    "false_negative_rate": 0.05
  }
}
```

## Dashboard Visualizations

The APIs provide data for creating the following dashboard visualizations:

### 1. Monthly Trend Analysis
- **Chart Type:** Line chart
- **Data Source:** `overall_analytics.monthly_trend_analysis.data`
- **Y-axis:** Expense amounts in thousands
- **X-axis:** Months (YYYY-MM format)

### 2. Amount Distribution
- **Chart Type:** Bar chart
- **Data Source:** `overall_analytics.amount_distribution`
- **Y-axis:** Transaction count
- **X-axis:** Amount ranges (0-1K, 1K-10K, etc.)

### 3. Department Expenses
- **Chart Type:** Area chart
- **Data Source:** `overall_analytics.department_expenses`
- **Y-axis:** Expense amounts
- **X-axis:** Departments/Profit Centers

### 4. Top GL Accounts
- **Chart Type:** Table with summary statistics
- **Data Source:** `overall_analytics.top_gl_accounts`
- **Columns:** Account, Amount, Transactions, Trial Balance

### 5. Top Users by Amount
- **Chart Type:** Table and bar chart
- **Data Source:** `overall_analytics.top_users_by_amount`
- **Columns:** User, Amount, Transactions

### 6. Employee Expenses
- **Chart Type:** Bar chart
- **Data Source:** `overall_analytics.employee_expenses.employees`
- **Y-axis:** Expense amounts
- **X-axis:** Employee names

### 7. Risk Distribution
- **Chart Type:** Pie chart or horizontal bars
- **Data Source:** `anomaly_analytics.risk_distribution`
- **Categories:** LOW, MEDIUM, HIGH, CRITICAL

### 8. Anomaly Summary
- **Chart Type:** Cards with metrics
- **Data Source:** `anomaly_analytics.summary`
- **Metrics:** Total Analyzed, Flagged Transactions, Flag Rate, Overall Risk Score

## Usage Examples

### Python Examples

```python
import requests

# Get comprehensive analytics dashboard
response = requests.get("http://localhost:8000/api/analytics/dashboard/")
dashboard_data = response.json()

# Extract key metrics
overall_stats = dashboard_data['overall_analytics']['summary_statistics']
print(f"Total Transactions: {overall_stats['total_transactions']:,}")
print(f"Total Amount: {overall_stats['total_amount']:,.2f} SAR")

# Get anomaly analytics
anomaly_stats = dashboard_data['anomaly_analytics']['summary']
print(f"Flagged Transactions: {anomaly_stats['flagged_transactions']}")
print(f"Flag Rate: {anomaly_stats['flag_rate']}%")

# Get detailed anomalies
response = requests.get("http://localhost:8000/api/analytics/anomalies/?type=amount")
anomaly_data = response.json()
print(f"Amount Anomalies: {anomaly_data['summary']['total_anomalies']}")

# Get analytics for specific file
file_id = "12345678-1234-1234-1234-123456789012"
response = requests.get(f"http://localhost:8000/api/analytics/dashboard/?file_id={file_id}")
file_dashboard = response.json()
print(f"File Analytics: {file_dashboard['overall_analytics']['summary_statistics']['total_transactions']} transactions")

# Get anomalies for specific file
response = requests.get(f"http://localhost:8000/api/analytics/anomalies/?file_id={file_id}")
file_anomalies = response.json()
print(f"File Anomalies: {file_anomalies['summary']['total_anomalies']} anomalies")

# Get file analytics by ID (detailed)
response = requests.get(f"http://localhost:8000/api/analytics/file/{file_id}/")
file_analytics = response.json()

# Extract file information
file_info = file_analytics['file_info']
print(f"File: {file_info['file_name']}")
print(f"Client: {file_info['client_name']}")
print(f"Status: {file_info['status']}")
print(f"Success Rate: {file_info['success_rate']}%")

# Extract transaction summary
transaction_summary = file_analytics['transaction_summary']
print(f"Total Transactions: {transaction_summary['total_transactions']:,}")
print(f"Total Amount: {transaction_summary['total_amount']:,.2f} SAR")

# Extract anomaly summary
anomaly_summary = file_analytics['anomaly_summary']
print(f"Flagged Transactions: {anomaly_summary['flagged_transactions']}")
print(f"Flag Rate: {anomaly_summary['flag_rate']}%")
```

### JavaScript Examples

```javascript
// Get analytics dashboard
fetch('/api/analytics/dashboard/')
  .then(response => response.json())
  .then(data => {
    const overall = data.overall_analytics;
    const anomalies = data.anomaly_analytics;
    
    console.log('Total Transactions:', overall.summary_statistics.total_transactions);
    console.log('Flagged Transactions:', anomalies.summary.flagged_transactions);
    
    // Create charts with the data
    createMonthlyTrendChart(overall.monthly_trend_analysis.data);
    createAmountDistributionChart(overall.amount_distribution);
    createRiskDistributionChart(anomalies.risk_distribution);
  });

// Get detailed anomalies
fetch('/api/analytics/anomalies/?risk_level=HIGH')
  .then(response => response.json())
  .then(data => {
    console.log('High Risk Anomalies:', data.summary.total_anomalies);
    displayAnomalyDetails(data.anomaly_details);
  });

// Get file analytics by ID
const fileId = '12345678-1234-1234-1234-123456789012';
fetch(`/api/analytics/file/${fileId}/`)
  .then(response => response.json())
  .then(data => {
    const fileInfo = data.file_info;
    const transactionSummary = data.transaction_summary;
    const anomalySummary = data.anomaly_summary;
    
    console.log('File:', fileInfo.file_name);
    console.log('Client:', fileInfo.client_name);
    console.log('Status:', fileInfo.status);
    console.log('Success Rate:', fileInfo.success_rate + '%');
    
    console.log('Total Transactions:', transactionSummary.total_transactions);
    console.log('Total Amount:', transactionSummary.total_amount);
    
    console.log('Flagged Transactions:', anomalySummary.flagged_transactions);
    console.log('Flag Rate:', anomalySummary.flag_rate + '%');
    
    // Create file-specific charts
    createFileMonthlyTrendChart(data.monthly_trend_analysis.data);
    createFileAmountDistributionChart(data.amount_distribution);
    createFileRiskDistributionChart(data.risk_distribution);
  });
```

### cURL Examples

```bash
# Get comprehensive dashboard
curl -X GET "http://localhost:8000/api/analytics/dashboard/" \
  -H "Content-Type: application/json"

# Get anomalies for specific date range
curl -X GET "http://localhost:8000/api/analytics/anomalies/?date_from=2024-01-01&date_to=2024-12-31" \
  -H "Content-Type: application/json"

# Get high-risk anomalies for specific user
curl -X GET "http://localhost:8000/api/analytics/anomalies/?risk_level=HIGH&user_name=M.ALJOHANI" \
  -H "Content-Type: application/json"

# Get analytics for specific file
curl -X GET "http://localhost:8000/api/analytics/dashboard/?file_id=12345678-1234-1234-1234-123456789012" \
  -H "Content-Type: application/json"

# Get anomalies for specific file
curl -X GET "http://localhost:8000/api/analytics/anomalies/?file_id=12345678-1234-1234-1234-123456789012" \
  -H "Content-Type: application/json"

# Get file analytics by ID (detailed)
curl -X GET "http://localhost:8000/api/analytics/file/12345678-1234-1234-1234-123456789012/" \
  -H "Content-Type: application/json"
```

## Testing

Use the provided test scripts to verify the analytics functionality:

```bash
# Test comprehensive analytics dashboard and anomalies
python test_analytics_dashboard.py

# Test file analytics by ID
python test_file_analytics_by_id.py
```

These scripts will:
1. Test the analytics dashboard API with various filters
2. Test the detailed anomalies API with different parameters
3. Test the file analytics by ID API with valid and invalid file IDs
4. Display comprehensive results and statistics
5. Verify all data sections are working correctly

## Performance Considerations

- Large datasets may take time to process
- Use specific filters to reduce response size
- Consider caching frequently accessed analytics data
- Monitor database query performance for complex aggregations

## Security Notes

- All data is filtered based on user permissions (if implemented)
- Sensitive transaction details are included in anomaly analysis
- File content is never exposed, only metadata and analysis results
- Risk scores and anomaly flags are calculated based on business rules

## Integration with Frontend

The APIs are designed to work seamlessly with modern frontend frameworks:

- **React/Vue/Angular:** Use the JSON responses directly for state management
- **Chart.js/D3.js:** Use the structured data for creating visualizations
- **Dashboard Frameworks:** Integrate with tools like Grafana, PowerBI, or custom dashboards
- **Real-time Updates:** Poll the APIs periodically for live dashboard updates 