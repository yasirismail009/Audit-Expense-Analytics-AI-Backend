# 📊 Analytics System API Documentation

## 🚀 **System Status**

### ✅ **Current Status: FULLY OPERATIONAL**
- ✅ **Auto-processing**: Running every minute via Celery Beat
- ✅ **All Analysis Types**: Working and saving to database
- ✅ **ML Model Training**: Now included in auto-processing
- ✅ **Data Storage**: All results being saved to database

### 📈 **Analysis Results Count**
```
GeneralAnalysisResult: 8
DuplicateAnalysisResult: 3
BackdatedAnalysisResult: 3
UserAnalysisResult: 1
ClosingEntriesAnalysisResult: 1
UnusualDaysAnalysisResult: 1
HolidayAnalysisResult: 1
OverallAnalysisResult: 3
RiskScoringDocument: 3
```

---

## 🔧 **API Endpoints**

### 1. **File Upload & Processing**

#### **POST** `/api/upload/`
Upload a CSV file for analysis.

**Request:**
```json
{
  "file": "multipart/form-data",
  "run_anomalies": true,
  "requested_anomalies": ["general", "duplicate", "backdated", "user", "unusual_days", "closing_entries", "holiday"]
}
```

**Response:**
```json
{
  "status": "success",
  "data_file_id": "uuid",
  "job_id": "uuid",
  "message": "File uploaded and processing started"
}
```

#### **GET** `/api/files/`
Get list of uploaded files.

**Response:**
```json
{
  "status": "success",
  "files": [
    {
      "id": "uuid",
      "file_name": "string",
      "upload_date": "datetime",
      "file_size": "number",
      "status": "string",
      "total_transactions": "number"
    }
  ]
}
```

### 2. **Analysis Results APIs**

#### **GET** `/api/file-analysis-statistics/{file_id}/`
Get comprehensive analysis statistics for a file.

**Response:**
```json
{
  "status": "success",
  "file_id": "uuid",
  "file_name": "string",
  "analysis_summary": {
    "total_transactions": 1000,
    "total_amount": 500000.00,
    "unique_users": 25,
    "unique_accounts": 50,
    "analysis_status": "COMPLETED",
    "processing_duration": 45.2
  },
  "anomaly_summary": {
    "duplicateEntries": 5,
    "userAnomalies": 3,
    "backdatedEntries": 2,
    "closingEntries": 8,
    "unusualDays": 1,
    "holidayEntries": 0,
    "highRiskUsers": 2,
    "totalUsers": 25,
    "totalAnomalies": 21
  },
  "risk_assessment": {
    "overall_risk_level": "MEDIUM",
    "risk_score": 65.5,
    "critical_risks": 2,
    "high_risks": 5,
    "medium_risks": 8,
    "low_risks": 6
  },
  "alerts": [
    {
      "type": "DUPLICATE_TRANSACTION",
      "severity": "HIGH",
      "message": "Found 5 duplicate transactions",
      "count": 5
    }
  ]
}
```

#### **GET** `/api/general-analysis/{file_id}/`
Get general analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "trial_balance_summary": {
    "total_debits": 250000.00,
    "total_credits": 250000.00,
    "balance": 0.00,
    "is_balanced": true,
    "balance_percentage": 0.0,
    "total_amount": 500000.00
  },
  "gl_account_summaries": [
    {
      "account": "1000",
      "total_amount": 50000.00,
      "transaction_count": 25,
      "users": ["user1", "user2"]
    }
  ],
  "user_summaries": [
    {
      "user": "user1",
      "total_amount": 75000.00,
      "transaction_count": 35,
      "accounts": ["1000", "2000"],
      "avg_amount": 2142.86
    }
  ],
  "statistical_calculations": {
    "average_transaction_amount": 500.00,
    "total_transactions": 1000,
    "unique_users": 25,
    "unique_accounts": 50,
    "min_transaction_amount": 10.00,
    "max_transaction_amount": 50000.00
  },
  "chart_data": {
    "gl_account_distribution": [...],
    "user_distribution": [...]
  },
  "processing_duration": 2.5,
  "status": "COMPLETED"
}
```

#### **GET** `/api/duplicate-analysis/{file_id}/`
Get duplicate analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "duplicates_found": 5,
  "duplicate_pairs": [
    {
      "transaction1": {
        "id": "uuid",
        "document_number": "DOC001",
        "amount": 1000.00,
        "posting_date": "2025-01-15",
        "user": "user1",
        "account": "1000"
      },
      "transaction2": {
        "id": "uuid",
        "document_number": "DOC002",
        "amount": 1000.00,
        "posting_date": "2025-01-15",
        "user": "user1",
        "account": "1000"
      },
      "similarity_score": 1.0,
      "risk_level": "HIGH"
    }
  ],
  "audit_recommendations": [
    "Review duplicate transactions for potential errors",
    "Verify if duplicates are intentional or data entry errors"
  ],
  "compliance_assessment": {
    "duplicate_percentage": 0.5,
    "risk_level": "HIGH"
  },
  "financial_statement_impact": {
    "potential_overstatement": 10,
    "impact_level": "HIGH"
  },
  "processing_duration": 1.8,
  "status": "COMPLETED"
}
```

#### **GET** `/api/backdated-analysis/{file_id}/`
Get backdated analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "backdated_transactions": [
    {
      "transaction_id": "uuid",
      "document_number": "DOC003",
      "amount": 2500.00,
      "posting_date": "2025-01-01",
      "entry_date": "2025-01-10",
      "days_difference": 9,
      "user": "user2",
      "account": "2000",
      "risk_level": "MEDIUM"
    }
  ],
  "backdated_by_user": {
    "user2": 2
  },
  "backdated_by_account": {
    "2000": 2
  },
  "audit_recommendations": [
    "Review backdated transactions for proper authorization",
    "Verify if backdating is allowed by company policy"
  ],
  "compliance_assessment": {
    "backdated_percentage": 0.2,
    "risk_level": "MEDIUM"
  },
  "processing_duration": 1.2,
  "status": "COMPLETED"
}
```

#### **GET** `/api/user-analysis/{file_id}/`
Get user analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "total_users": 25,
  "user_transaction_summary": [
    {
      "user": "user1",
      "total_amount": 75000.00,
      "transaction_count": 35,
      "accounts": ["1000", "2000"],
      "avg_amount": 2142.86
    }
  ],
  "user_anomalies": [
    {
      "user": "user3",
      "anomaly_type": "HIGH_ACTIVITY",
      "risk_level": "HIGH",
      "details": "User has 150 transactions totaling 2000000"
    }
  ],
  "user_risk_assessment": [
    {
      "user": "user1",
      "risk_level": "LOW",
      "risk_factors": []
    }
  ],
  "statistical_summary": {
    "total_users": 25,
    "high_risk_users": 2,
    "users_with_anomalies": 2
  },
  "processing_duration": 2.1,
  "status": "COMPLETED"
}
```

#### **GET** `/api/unusual-days-analysis/{file_id}/`
Get unusual days analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "unusual_days_transactions": [
    {
      "transaction_id": "uuid",
      "document_number": "DOC004",
      "amount": 1500.00,
      "posting_date": "2025-01-04",
      "day_of_week": "Saturday",
      "user": "user4",
      "account": "3000",
      "risk_level": "HIGH"
    }
  ],
  "unusual_days_by_user": {
    "user4": 1
  },
  "unusual_days_by_account": {
    "3000": 1
  },
  "audit_recommendations": [
    "Review weekend transactions for proper authorization",
    "Verify if weekend postings are allowed by company policy"
  ],
  "compliance_assessment": {
    "unusual_days_percentage": 0.1,
    "risk_level": "LOW"
  },
  "processing_duration": 1.5,
  "status": "COMPLETED"
}
```

#### **GET** `/api/closing-entries-analysis/{file_id}/`
Get closing entries analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "closing_entries_transactions": [
    {
      "transaction_id": "uuid",
      "document_number": "DOC005",
      "amount": 5000.00,
      "posting_date": "2025-01-31",
      "days_from_month_end": 0,
      "user": "user5",
      "account": "4000",
      "risk_level": "MEDIUM"
    }
  ],
  "closing_by_user": {
    "user5": 3
  },
  "closing_by_account": {
    "4000": 3
  },
  "audit_recommendations": [
    "Review closing entries for proper authorization",
    "Verify if closing entries are properly documented"
  ],
  "compliance_assessment": {
    "closing_entries_percentage": 0.8,
    "risk_level": "MEDIUM"
  },
  "processing_duration": 1.3,
  "status": "COMPLETED"
}
```

#### **GET** `/api/holiday-analysis/{file_id}/`
Get holiday analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "holiday_transactions": [
    {
      "transaction_id": "uuid",
      "document_number": "DOC006",
      "amount": 3000.00,
      "posting_date": "2025-01-01",
      "holiday_name": "New Year's Day",
      "user": "user6",
      "account": "5000",
      "risk_level": "HIGH"
    }
  ],
  "holiday_by_user": {
    "user6": 1
  },
  "holiday_by_account": {
    "5000": 1
  },
  "audit_recommendations": [
    "Review holiday transactions for proper authorization",
    "Verify if holiday postings are allowed by company policy"
  ],
  "compliance_assessment": {
    "holiday_percentage": 0.1,
    "risk_level": "LOW"
  },
  "processing_duration": 1.4,
  "status": "COMPLETED"
}
```

#### **GET** `/api/overall-analysis/{file_id}/`
Get overall analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "risk_document_id": "uuid",
  "comprehensive_analysis": {
    "data_quality_score": 85.5,
    "overall_risk_level": "MEDIUM",
    "total_anomalies": 21,
    "critical_issues": 2,
    "recommendations": [
      "Review duplicate transactions immediately",
      "Investigate high-risk user activities"
    ]
  },
  "analysis_breakdown": {
    "general_analysis": "COMPLETED",
    "duplicate_analysis": "COMPLETED",
    "backdated_analysis": "COMPLETED",
    "user_analysis": "COMPLETED",
    "unusual_days_analysis": "COMPLETED",
    "closing_entries_analysis": "COMPLETED",
    "holiday_analysis": "COMPLETED",
    "risk_analysis": "COMPLETED"
  },
  "processing_duration": 15.2,
  "status": "COMPLETED"
}
```

#### **GET** `/api/risk-analysis/{file_id}/`
Get risk analysis results.

**Response:**
```json
{
  "status": "success",
  "analysis_id": "uuid",
  "risk_scoring": {
    "overall_risk_score": 65.5,
    "risk_level": "MEDIUM",
    "risk_factors": [
      {
        "factor": "DUPLICATE_TRANSACTIONS",
        "weight": 0.3,
        "score": 80.0,
        "impact": "HIGH"
      }
    ]
  },
  "critical_risk_transactions": [
    {
      "transaction_id": "uuid",
      "risk_type": "DUPLICATE",
      "risk_score": 95.0,
      "amount": 1000.00,
      "user": "user1",
      "account": "1000"
    }
  ],
  "risk_by_user": {
    "user1": {
      "risk_score": 75.0,
      "risk_level": "HIGH",
      "anomaly_count": 3
    }
  },
  "risk_by_account": {
    "1000": {
      "risk_score": 60.0,
      "risk_level": "MEDIUM",
      "anomaly_count": 2
    }
  },
  "processing_duration": 3.2,
  "status": "COMPLETED"
}
```

### 3. **Job Management APIs**

#### **GET** `/api/jobs/`
Get list of processing jobs.

**Response:**
```json
{
  "status": "success",
  "jobs": [
    {
      "id": "uuid",
      "data_file_id": "uuid",
      "file_name": "string",
      "status": "COMPLETED",
      "created_at": "datetime",
      "started_at": "datetime",
      "completed_at": "datetime",
      "processing_duration": 45.2,
      "run_anomalies": true,
      "requested_anomalies": ["general", "duplicate", "backdated"]
    }
  ]
}
```

#### **GET** `/api/jobs/{job_id}/`
Get specific job details.

**Response:**
```json
{
  "status": "success",
  "job": {
    "id": "uuid",
    "data_file_id": "uuid",
    "file_name": "string",
    "status": "COMPLETED",
    "created_at": "datetime",
    "started_at": "datetime",
    "completed_at": "datetime",
    "processing_duration": 45.2,
    "run_anomalies": true,
    "requested_anomalies": ["general", "duplicate", "backdated"],
    "analytics_results": {
      "general_analysis": {
        "analysis_id": "uuid",
        "status": "COMPLETED",
        "processing_duration": 2.5
      },
      "duplicate_analysis": {
        "analysis_id": "uuid",
        "status": "COMPLETED",
        "processing_duration": 1.8
      },
      "ml_training": {
        "status": "started",
        "message": "ML training tasks initiated"
      }
    }
  }
}
```

### 4. **System Health APIs**

#### **GET** `/api/health/`
Get system health status.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "datetime",
  "services": {
    "database": "healthy",
    "redis": "healthy",
    "celery_worker": "healthy",
    "celery_beat": "healthy"
  },
  "statistics": {
    "total_files": 5,
    "total_jobs": 8,
    "completed_jobs": 8,
    "pending_jobs": 0,
    "failed_jobs": 0
  }
}
```

---

## 🤖 **ML Model Training Status**

### ✅ **ML Training Now Included**
The auto-processing now includes ML model training for:

1. **Rule-based Models** (`train_rule_based_models`)
   - Duplicate detection rules
   - Backdated transaction rules
   - User anomaly rules
   - Unusual days rules
   - Closing entries rules
   - Holiday detection rules
   - Risk scoring rules

2. **Individual Analysis Models**
   - `train_duplicate_analysis_model`
   - `train_backdated_analysis_model`
   - `train_user_analysis_model`
   - `train_unusual_days_analysis_model`
   - `train_closing_entries_analysis_model`
   - `train_holiday_analysis_model`
   - `train_overall_risk_analysis_model`

### 📊 **Training Process**
- **Triggered**: Automatically after each file analysis
- **Execution**: Background tasks (non-blocking)
- **Status**: Tracked in job analytics_results
- **Duration**: Varies based on data size and complexity

---

## 🔄 **Auto-Processing Workflow**

### **Every Minute (Celery Beat)**
1. **Find Pending Jobs**: Look for jobs with status 'PENDING' or 'QUEUED'
2. **Process Jobs**: Up to 5 jobs simultaneously
3. **Run All Analyses**: 
   - General Analysis
   - Duplicate Analysis
   - Backdated Analysis
   - User Analysis
   - Unusual Days Analysis
   - Closing Entries Analysis
   - Holiday Analysis
   - Overall Analysis
   - Risk Analysis
4. **Start ML Training**: Background training tasks
5. **Update Job Status**: Mark as 'COMPLETED'

### **Every 5 Minutes (Monitoring)**
1. **Monitor Processing Jobs**: Check for stuck jobs
2. **Reset Stuck Jobs**: Jobs processing for >30 minutes
3. **Generate Statistics**: Overall system health

---

## 📝 **Error Handling**

### **API Error Response Format**
```json
{
  "status": "error",
  "error_code": "ANALYSIS_FAILED",
  "message": "Detailed error message",
  "timestamp": "datetime",
  "job_id": "uuid (if applicable)"
}
```

### **Common Error Codes**
- `FILE_NOT_FOUND`: Requested file doesn't exist
- `ANALYSIS_NOT_COMPLETED`: Analysis still in progress
- `INVALID_FILE_FORMAT`: Unsupported file type
- `PROCESSING_FAILED`: Analysis processing failed
- `DATABASE_ERROR`: Database connection issues

---

## 🚀 **Usage Examples**

### **Complete File Analysis Workflow**
```bash
# 1. Upload file
curl -X POST http://localhost:8000/api/upload/ \
  -F "file=@data.csv" \
  -F "run_anomalies=true" \
  -F "requested_anomalies=general,duplicate,backdated,user,unusual_days,closing_entries,holiday"

# 2. Check job status
curl http://localhost:8000/api/jobs/{job_id}/

# 3. Get analysis results
curl http://localhost:8000/api/file-analysis-statistics/{file_id}/

# 4. Get specific analysis
curl http://localhost:8000/api/general-analysis/{file_id}/
```

---

## 📊 **Performance Metrics**

### **Expected Processing Times**
- **Small files (<1K transactions)**: 30-60 seconds
- **Medium files (1K-10K transactions)**: 2-5 minutes
- **Large files (>10K transactions)**: 5-15 minutes

### **ML Training Times**
- **Rule-based models**: 1-3 minutes
- **Individual models**: 2-5 minutes each
- **Total training time**: 10-20 minutes (background)

---

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Database
DATABASE_URL=postgresql://user:pass@db:5432/analytics

# Redis (Celery)
REDIS_URL=redis://redis:6379/0

# File Upload
FILE_UPLOAD_TEMP_DIR=/tmp/uploads
MAX_FILE_SIZE=100MB

# Processing
CELERY_WORKER_CONCURRENCY=4
CELERY_BEAT_SCHEDULE_INTERVAL=60
```

---

## 📞 **Support**

For issues or questions:
1. Check system health: `GET /api/health/`
2. Review job status: `GET /api/jobs/{job_id}/`
3. Check Celery worker logs: `docker-compose logs celery_worker`
4. Monitor auto-processing: `docker-compose logs celery_beat`

---

**🎉 System is now fully operational with all analysis types working and ML training included!** 