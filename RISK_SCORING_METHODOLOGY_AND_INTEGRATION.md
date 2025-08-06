# Risk Scoring Methodology and Integration

## Overview

The analytics system implements a comprehensive risk scoring methodology that evaluates transactions across specialized analysis dimensions and integrates the results into an overall risk assessment. This document explains how risk scores are calculated in each analysis type and how they are linked together, including the enhanced SAPGLPosting anomaly tracking system.

## **SYNC ANALYSIS TYPES AND RISK SCORING**

### **Complete Analysis Ecosystem**

The system implements **8 comprehensive analysis types** that run synchronously and contribute to the overall risk assessment:

#### **1. General Analysis** (`run_general_analysis_sync`)
**Purpose**: Basic transaction statistics and overview
**Risk Contribution**: Foundation for other analyses
**Data Stored**: `GeneralAnalysisResult`

**Key Metrics:**
- Trial balance summary (total debits, credits, balance)
- GL account summaries with transaction counts and amounts
- User summaries with activity patterns
- Statistical calculations (mean, standard deviation, min/max amounts)
- Chart data for visualizations

**Risk Scoring Impact:**
- Provides baseline transaction statistics
- Identifies unusual transaction patterns
- Supports other analysis types with foundational data

#### **2. Duplicate Analysis** (`run_duplicate_analysis_sync`)
**Purpose**: Detect duplicate transactions across 6 types
**Risk Contribution**: High risk (80 points)
**Data Stored**: `DuplicateAnalysisResult`

**Duplicate Types and Risk Scores:**
```
Type 6: 95 points (Account + Effective Date + Posted Date + User + Source + Amount)
Type 5: 90 points (Account + Effective Date + Amount)
Type 4: 85 points (Account + Posted Date + Amount)
Type 3: 80 points (Account + User + Amount)
Type 2: 75 points (Account + Source + Amount)
Type 1: 70 points (Account + Amount)
```

**Detection Logic:**
```python
# Simple duplicate detection based on amount, date, and account
transaction_dict = {}
for t in transactions:
    key = (float(t.amount_local_currency), t.posting_date, t.gl_account)
    if key in transaction_dict:
        # Found duplicate - assign risk score based on type
        duplicate_pairs.append({
            'transaction1': {...},
            'transaction2': {...},
            'similarity_score': 1.0,
            'risk_level': 'HIGH'
        })
```

#### **3. Backdated Analysis** (`run_backdated_analysis_sync`)
**Purpose**: Detect transactions posted after document date
**Risk Contribution**: High risk (70 points)
**Data Stored**: `BackdatedAnalysisResult`

**Risk Score Calculation:**
```python
if days_difference > 30:
    risk_score = 100.0  # Critical
elif days_difference > 14:
    risk_score = 85.0   # High
elif days_difference > 7:
    risk_score = 70.0   # Medium
else:
    risk_score = 50.0   # Low
```

**Detection Logic:**
```python
backdated_threshold = timedelta(days=7)
for t in transactions:
    if t.posting_date and t.document_date:
        days_difference = (t.posting_date - t.document_date).days
        if days_difference > 0:  # Backdated transaction
            backdated_transactions.append({
                'transaction_id': str(t.id),
                'days_difference': days_difference,
                'risk_level': 'HIGH' if days_difference > 30 else 'MEDIUM'
            })
```

#### **4. User Analysis** (`run_user_analysis_sync`)
**Purpose**: Analyze user behavior patterns and anomalies
**Risk Contribution**: Medium risk (50 points)
**Data Stored**: `UserAnalysisResult`

**Anomaly Detection:**
```python
# Simple anomaly detection: users with very high amounts or many transactions
if data['total_amount'] > 1000000 or data['transaction_count'] > 100:
    user_anomalies.append({
        'user': user,
        'anomaly_type': 'HIGH_ACTIVITY',
        'risk_level': 'HIGH',
        'details': f"User has {data['transaction_count']} transactions totaling {data['total_amount']}"
    })
```

**Risk Factors:**
- Transaction volume anomalies
- Amount anomalies
- Account usage patterns
- Temporal anomalies
- User role violations

#### **5. Unusual Days Analysis** (`run_unusual_days_analysis_sync`)
**Purpose**: Detect weekend/unusual day transactions
**Risk Contribution**: Medium risk (40 points)
**Data Stored**: `UnusualDaysAnalysisResult`

**Detection Logic:**
```python
for t in transactions:
    if t.posting_date:
        # Check if posting is on weekend (Saturday = 5, Sunday = 6)
        if t.posting_date.weekday() >= 5:
            unusual_days_transactions.append({
                'transaction_id': str(t.id),
                'day_of_week': t.posting_date.strftime('%A'),
                'risk_level': 'HIGH'
            })
```

**Risk Factors:**
- Weekend postings (base risk: 50 points)
- Month-end weekend postings (+10 points)
- Year-end weekend postings (+10 points)
- Debit transactions on weekend (+10 points)

#### **6. Closing Entries Analysis** (`run_closing_entries_analysis_sync`)
**Purpose**: Detect month-end closing transactions
**Risk Contribution**: Medium risk (30 points)
**Data Stored**: `ClosingEntriesAnalysisResult`

**Detection Logic:**
```python
for t in transactions:
    if t.posting_date:
        # Consider transactions in last 3 days of month as potential closing entries
        last_day_of_month = (t.posting_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days_from_month_end = (last_day_of_month - t.posting_date).days
        
        if days_from_month_end <= 3:
            closing_entries_transactions.append({
                'transaction_id': str(t.id),
                'days_from_month_end': days_from_month_end,
                'risk_level': 'MEDIUM' if days_from_month_end <= 1 else 'LOW'
            })
```

**Risk Factors:**
- Post-close entries (+25 points)
- Year-end closing entries (+10 points)
- Debit closing entries (+10 points)
- Month-end closing (+5 points)

#### **7. Holiday Analysis** (`run_holiday_analysis_sync`)
**Purpose**: Detect transactions on Saudi Arabian holidays
**Risk Contribution**: High risk (60 points)
**Data Stored**: `HolidayAnalysisResult`

**Detection Logic:**
```python
# Use holiday_utils to get dynamic Saudi Arabian holidays
from .holiday_utils import get_holidays, is_holiday

holidays = get_holidays('saudiarabian', start_date, end_date, include_observances=True)
holiday_dates = {h.date for h in holidays}
holiday_info = {h.date: {'name': h.name, 'type': h.holiday_type} for h in holidays}

for t in transactions:
    if t.posting_date:
        posting_date_str = t.posting_date.strftime('%Y-%m-%d')
        if posting_date_str in holiday_dates:
            holiday_name = holiday_info.get(posting_date_str, {}).get('name', 'Saudi Holiday')
            holiday_transactions.append({
                'transaction_id': str(t.id),
                'holiday_name': holiday_name,
                'risk_level': 'HIGH'
            })
```

**Risk Factors:**
- Holiday postings (base risk: 60 points)
- Year-end holiday postings (+10 points)
- Debit transactions on holidays (+10 points)

#### **8. Overall Analysis** (`run_overall_analysis_sync`)
**Purpose**: Comprehensive analysis combining all analysis types
**Risk Contribution**: Integrates all analysis results
**Data Stored**: `OverallAnalysisResult`

**Integration Logic:**
```python
# Use the real OverallAnalyzer to run comprehensive analysis
overall_analyzer = OverallAnalyzer()
analysis_results = overall_analyzer.run_overall_analysis(data_file, job)
```

**Key Features:**
- Combines all analysis results
- Generates comprehensive risk assessment
- Creates risk scoring document
- Provides audit recommendations

### **PHASE 1: INDIVIDUAL ANALYSIS RISK SCORING**

#### **1. Duplicate Analysis Risk Scoring (70-95 points)**

**Risk Factors & Point Allocation:**
- **Account Matching**: Base factor (required for all types)
- **Date Matching**: Effective date + Posted date (Type 5, 6)
- **User Matching**: Same user posting (Type 3, 6)
- **Source Matching**: Same source system (Type 2, 6)
- **Amount Matching**: Same transaction amount (all types)
- **Temporal Proximity**: Short time intervals
- **Pattern Consistency**: Repeated patterns

**Risk Score by Duplicate Type:**
```
Type 6: 95 points (Account + Effective Date + Posted Date + User + Source + Amount)
Type 5: 90 points (Account + Effective Date + Amount)
Type 4: 85 points (Account + Posted Date + Amount)
Type 3: 80 points (Account + User + Amount)
Type 2: 75 points (Account + Source + Amount)
Type 1: 70 points (Account + Amount)
```

**Risk Levels:**
- **Low Risk (70-74)**: Type 1 duplicates
- **Medium Risk (75-84)**: Type 2-3 duplicates
- **High Risk (85-94)**: Type 4-5 duplicates
- **Critical Risk (95-100)**: Type 6 duplicates

#### **2. Backdated Analysis Risk Scoring (50-100 points)**

**Risk Factors & Point Allocation:**
- **Days Difference**: Primary factor
  - 1-7 days: 50 points
  - 8-14 days: 70 points
  - 15-30 days: 85 points
  - >30 days: 100 points
- **Month-End Backdating**: Additional risk factor
- **Quarter-End Backdating**: Additional risk factor
- **Year-End Backdating**: Additional risk factor
- **User Backdating Patterns**: Users frequently posting backdated entries
- **Account Backdating Patterns**: Specific accounts with backdated entries
- **Temporal Clustering**: Multiple backdated entries in short periods

**Risk Score Calculation:**
```python
if days_difference > 30:
    risk_score = 100.0  # Critical
elif days_difference > 14:
    risk_score = 85.0   # High
elif days_difference > 7:
    risk_score = 70.0   # Medium
else:
    risk_score = 50.0   # Low
```

**Risk Levels:**
- **Low Risk (50-69)**: 1-7 days difference
- **Medium Risk (70-84)**: 8-14 days difference
- **High Risk (85-99)**: 15-30 days difference
- **Critical Risk (100)**: >30 days difference

#### **3. User Analysis Risk Scoring (0-100 points)**

**Risk Factors & Point Allocation:**
- **Transaction Volume**: Users with unusually high transaction counts
- **Account Usage Patterns**: Users posting to unusual/restricted accounts (+15 points)
- **Temporal Anomalies**: Users posting at unusual times (+10 points)
- **Pattern Deviations**: Users deviating from normal patterns (+15 points)
- **User Role Violations**: Users posting outside typical role
- **Geographic Anomalies**: Users posting from unusual locations
- **Session Patterns**: Unusual login/logout patterns
- **Batch Processing**: Users posting large batches

**Risk Score Calculation:**
```python
base_risk = user_anomaly_score
if user_unusual_accounts > threshold:
    base_risk += 15.0
if user_temporal_anomalies:
    base_risk += 10.0
if user_pattern_deviation > threshold:
    base_risk += 15.0
final_risk = min(base_risk, 100.0)
```

**Risk Levels:**
- **Low Risk (0-30)**: Normal user activity
- **Medium Risk (31-60)**: Some unusual patterns
- **High Risk (61-85)**: Multiple anomalies
- **Critical Risk (86-100)**: Critical behavior issues

#### **4. Unusual Days Analysis Risk Scoring (50-100 points)**

**Risk Factors & Point Allocation:**
- **Weekend Postings**: Base risk (50 points)
- **Holiday Postings**: Additional risk factor
- **Unusual Day Patterns**: Statistically unusual days
- **User Weekend Activity**: Users posting frequently on weekends
- **Temporal Anomalies**: Transactions at unusual times
- **Month-End Weekend**: Weekend postings near month-end (+10 points)
- **Year-End Weekend**: Weekend postings near year-end (+10 points)
- **Pattern Clustering**: Multiple weekend postings by same user

**Risk Score Calculation:**
```python
base_risk = 50.0  # Base weekend posting risk
if transaction.transaction_type == 'DEBIT':
    base_risk += 10.0  # Debit transactions on weekend
if transaction.posting_date.day >= 25:
    base_risk += 10.0  # Month-end weekend posting
if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
    base_risk += 10.0  # Year-end weekend posting
final_risk = min(base_risk, 100.0)
```

**Risk Levels:**
- **Low Risk (50-69)**: Basic weekend posting
- **Medium Risk (70-84)**: Weekend posting with additional factors
- **High Risk (85-99)**: Weekend posting with multiple risk factors
- **Critical Risk (100)**: Critical weekend posting patterns

#### **5. Closing Entries Analysis Risk Scoring (30-100 points)**

**Risk Factors & Point Allocation:**
- **Post-Close Entries**: Entries posted after month-end (+25 points)
- **Closing Window Violations**: Entries outside standard closing windows
- **Unusual Closing Patterns**: Unusual patterns during closing periods
- **User Closing Activity**: Users posting frequently during closing
- **Temporal Anomalies**: Transactions at unusual times during closing
- **Account Closing Patterns**: Specific accounts with closing entries
- **Batch Closing**: Large batches of closing entries
- **Year-End Closing**: Entries during year-end closing periods (+10 points)

**Risk Score Calculation:**
```python
base_risk = 30.0  # Base closing entry risk
if is_post_close_entry(transaction.posting_date):
    base_risk += 25.0  # Post-close entry
if transaction.transaction_type == 'DEBIT':
    base_risk += 10.0  # Debit closing entry
if transaction.posting_date.day >= 25:
    base_risk += 5.0   # Month-end closing
if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
    base_risk += 10.0  # Year-end closing
final_risk = min(base_risk, 100.0)
```

**Risk Levels:**
- **Low Risk (30-49)**: Basic closing entry
- **Medium Risk (50-69)**: Closing entry with additional factors
- **High Risk (70-89)**: Post-close entry or closing with multiple factors
- **Critical Risk (90-100)**: Critical closing entry patterns

#### **6. Holiday Analysis Risk Scoring (60-100 points)**

**Risk Factors & Point Allocation:**
- **Holiday Postings**: Base risk (60 points)
- **Holiday Type**: Public holidays vs. observances
- **User Holiday Activity**: Users posting frequently on holidays
- **Temporal Anomalies**: Transactions at unusual times on holidays
- **Account Holiday Patterns**: Specific accounts with holiday entries
- **Batch Holiday**: Large batches of holiday entries
- **Year-End Holiday**: Holiday postings near year-end (+10 points)

**Risk Score Calculation:**
```python
base_risk = 60.0  # Base holiday posting risk
if transaction.transaction_type == 'DEBIT':
    base_risk += 10.0  # Debit transactions on holiday
if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
    base_risk += 10.0  # Year-end holiday posting
final_risk = min(base_risk, 100.0)
```

**Risk Levels:**
- **Low Risk (60-69)**: Basic holiday posting
- **Medium Risk (70-84)**: Holiday posting with additional factors
- **High Risk (85-99)**: Holiday posting with multiple risk factors
- **Critical Risk (100)**: Critical holiday posting patterns

### **PHASE 2: SIMPLIFIED RISK INTEGRATION**

#### **Current Implementation: Direct Risk Score Assignment**

The current system uses a simplified approach where risk scores are directly assigned based on detected anomalies:

```python
def calculate_transaction_risk_score(transaction, analysis_results):
    risk_score = 0.0
    anomaly_types = []
    
    # Check for duplicate (high risk)
    if transaction in duplicate_list:
        risk_score += 80.0
        anomaly_types.append('duplicate')
    
    # Check for backdated (high risk)
    if transaction in backdated_list:
        risk_score += 70.0
        anomaly_types.append('backdated')
    
    # Check for holiday (high risk)
    if transaction in holiday_list:
        risk_score += 60.0
        anomaly_types.append('holiday')
    
    # Check for user anomalies (medium risk)
    if transaction.user_name in user_anomalies:
        risk_score += 50.0
        anomaly_types.append('user_anomaly')
    
    # Check for unusual days (medium risk)
    if transaction in unusual_days_list:
        risk_score += 40.0
        anomaly_types.append('unusual_days')
    
    # Check for closing entries (medium risk)
    if transaction in closing_entries_list:
        risk_score += 30.0
        anomaly_types.append('closing_entries')
    
    return min(risk_score, 100.0), anomaly_types
```

#### **Risk Score Weights:**

| Anomaly Type | Risk Points | Risk Level | Rationale |
|--------------|-------------|------------|-----------|
| **Duplicate** | 80 points | High Risk | Critical nature of duplicate transactions |
| **Backdated** | 70 points | High Risk | Significant audit concern |
| **Holiday** | 60 points | High Risk | Unusual posting on holidays |
| **User Anomaly** | 50 points | Medium Risk | User behavior anomalies |
| **Unusual Days** | 40 points | Medium Risk | Weekend/unusual day postings |
| **Closing Entries** | 30 points | Medium Risk | Month-end closing entries |

### **PHASE 3: SAPGLPosting ANOMALY TRACKING**

#### **Enhanced Transaction-Level Anomaly Tracking**

The system now updates individual `SAPGLPosting` transactions with comprehensive anomaly identification:

```python
# Update transaction risk score and anomaly flags
transaction.overall_risk_score = risk_score

# Update specific anomaly flags
anomaly_types = []

# Check for duplicate (high risk)
if duplicate_analysis and duplicate_analysis.duplicate_list:
    for duplicate in duplicate_analysis.duplicate_list:
        if (str(transaction.id) in [duplicate.get('transaction1', {}).get('id'), 
                                   duplicate.get('transaction2', {}).get('id')]):
            transaction.is_duplicate = True
            transaction.duplicate_risk_score = 80.0
            anomaly_types.append('duplicate')
            break

# Check for backdated (high risk)
if backdated_analysis and backdated_analysis.backdated_entries:
    for backdated in backdated_analysis.backdated_entries:
        if str(transaction.id) == backdated.get('transaction_id'):
            transaction.is_backdated = True
            transaction.backdated_risk_score = 70.0
            transaction.backdated_days = backdated.get('days_difference', 0)
            anomaly_types.append('backdated')
            break

# Check for holiday (high risk)
if holiday_analysis and holiday_analysis.holiday_postings:
    for holiday in holiday_analysis.holiday_postings:
        if str(transaction.id) == holiday.get('transaction_id'):
            transaction.is_holiday_posting = True
            transaction.holiday_name = holiday.get('holiday_name', 'Unknown')
            anomaly_types.append('holiday')
            break

# Update anomaly types list and summary
transaction.anomaly_types = anomaly_types
transaction.anomaly_analysis_summary = {
    'risk_score': risk_score,
    'anomaly_types': anomaly_types,
    'is_duplicate': transaction.is_duplicate,
    'is_backdated': transaction.is_backdated,
    'is_holiday_posting': transaction.is_holiday_posting,
    'holiday_name': transaction.holiday_name,
    'backdated_days': transaction.backdated_days
}

transaction.save()
```

#### **SAPGLPosting Anomaly Fields:**

**Risk Scoring Fields:**
- **`overall_risk_score`** (Float): Overall risk score (0-100)
- **`anomaly_types`** (JSON List): List of detected anomaly types
- **`anomaly_analysis_summary`** (JSON): Comprehensive anomaly summary

**Specific Anomaly Flags:**
- **`is_duplicate`** (Boolean): Flagged as duplicate transaction
- **`duplicate_type`** (String): Type of duplicate (type_1, type_2, etc.)
- **`duplicate_risk_score`** (Float): Risk score for duplicate detection (0-100)
- **`duplicate_analysis_details`** (JSON): Detailed duplicate analysis results

- **`is_backdated`** (Boolean): Flagged as backdated transaction
- **`backdated_days`** (Integer): Number of days between document date and posting date
- **`backdated_risk_score`** (Float): Risk score for backdated detection (0-100)
- **`backdated_analysis_details`** (JSON): Detailed backdated analysis results

- **`is_holiday_posting`** (Boolean): Flagged as holiday posting
- **`holiday_name`** (String): Name of the holiday (e.g., "Christmas", "Veterans Day")
- **`holiday_type`** (String): Type of holiday (Public holiday, Observance, etc.)
- **`holiday_analysis_details`** (JSON): Detailed holiday analysis results

### **PHASE 4: OVERALL RISK SCORING**

#### **Weighted Overall Risk Score Calculation**

The overall risk score is calculated based on the percentage of high and critical risk transactions:

```python
def calculate_overall_risk_score(risk_distribution, total_transactions):
    high_risk_count = risk_distribution.get('high_risk', 0)
    critical_risk_count = risk_distribution.get('critical_risk', 0)
    
    high_risk_percentage = (high_risk_count + critical_risk_count) / total_transactions * 100 if total_transactions > 0 else 0
    critical_risk_percentage = critical_risk_count / total_transactions * 100 if total_transactions > 0 else 0
    
    # Calculate weighted risk score
    if critical_risk_percentage > 0:
        # If there are critical risk transactions, score should be HIGH
        overall_risk_score = 75.0 + (critical_risk_percentage * 0.5)  # Base 75 + critical risk bonus
    elif high_risk_percentage > 5:
        # If more than 5% are high risk, score should be HIGH
        overall_risk_score = 60.0 + (high_risk_percentage * 0.3)  # Base 60 + high risk bonus
    elif high_risk_percentage > 1:
        # If more than 1% are high risk, score should be MEDIUM
        overall_risk_score = 40.0 + (high_risk_percentage * 0.5)  # Base 40 + high risk bonus
    else:
        # Use average risk score for low risk scenarios
        overall_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
    
    # Cap risk score at 100
    overall_risk_score = min(overall_risk_score, 100.0)
    
    return overall_risk_score
```

#### **Risk Level Classification:**

```python
def get_risk_level(overall_risk_score):
    if overall_risk_score >= 80:
        return 'CRITICAL'  # Risk Level 3
    elif overall_risk_score >= 60:
        return 'HIGH'      # Risk Level 2
    elif overall_risk_score >= 30:
        return 'MEDIUM'    # Risk Level 1
    else:
        return 'LOW'       # Risk Level 0
```

**Risk Level Thresholds:**
- **Critical Risk**: ≥80 points (Risk Level 3)
- **High Risk**: 60-79 points (Risk Level 2)
- **Medium Risk**: 30-59 points (Risk Level 1)
- **Low Risk**: 0-29 points (Risk Level 0)

### **SYNC ANALYSIS INTEGRATION PATTERNS**

#### **Analysis Execution Flow:**

```python
# Complete sync analysis pipeline
def run_complete_analysis_sync(job_id):
    """Run all analysis types synchronously"""
    
    analysis_results = {}
    
    # 1. General Analysis (Foundation)
    general_result = run_general_analysis_sync(job_id)
    analysis_results['general_analysis'] = general_result
    
    # 2. Duplicate Analysis (High Risk)
    duplicate_result = run_duplicate_analysis_sync(job_id)
    analysis_results['duplicate_analysis'] = duplicate_result
    
    # 3. Backdated Analysis (High Risk)
    backdated_result = run_backdated_analysis_sync(job_id)
    analysis_results['backdated_analysis'] = backdated_result
    
    # 4. User Analysis (Medium Risk)
    user_result = run_user_analysis_sync(job_id)
    analysis_results['user_analysis'] = user_result
    
    # 5. Unusual Days Analysis (Medium Risk)
    unusual_days_result = run_unusual_days_analysis_sync(job_id)
    analysis_results['unusual_days_analysis'] = unusual_days_result
    
    # 6. Closing Entries Analysis (Medium Risk)
    closing_entries_result = run_closing_entries_analysis_sync(job_id)
    analysis_results['closing_entries_analysis'] = closing_entries_result
    
    # 7. Holiday Analysis (High Risk)
    holiday_result = run_holiday_analysis_sync(job_id)
    analysis_results['holiday_analysis'] = holiday_result
    
    # 8. Overall Analysis (Integration)
    overall_result = run_overall_analysis_sync(job_id)
    analysis_results['overall_analysis'] = overall_result
    
    # 9. Risk Analysis (Final Integration)
    risk_result = run_risk_analysis_sync(job_id)
    analysis_results['risk_analysis'] = risk_result
    
    return analysis_results
```

#### **Database Storage Pattern:**

Each analysis type stores results in dedicated tables:

```python
# Analysis Result Models
class GeneralAnalysisResult(models.Model):
    # Basic transaction statistics and overview
    trial_balance_summary = models.JSONField()
    gl_account_summaries = models.JSONField()
    user_summaries = models.JSONField()
    statistical_calculations = models.JSONField()

class DuplicateAnalysisResult(models.Model):
    # Duplicate detection results
    duplicate_list = models.JSONField()
    breakdowns = models.JSONField()
    compliance_assessment = models.JSONField()

class BackdatedAnalysisResult(models.Model):
    # Backdated transaction detection
    backdated_entries = models.JSONField()
    backdated_by_user = models.JSONField()
    backdated_by_account = models.JSONField()

class UserAnalysisResult(models.Model):
    # User behavior analysis
    user_transaction_summary = models.JSONField()
    user_anomalies = models.JSONField()
    user_risk_assessment = models.JSONField()

class UnusualDaysAnalysisResult(models.Model):
    # Weekend/unusual day detection
    weekend_postings = models.JSONField()
    unusual_days = models.JSONField()

class ClosingEntriesAnalysisResult(models.Model):
    # Month-end closing detection
    closing_entries = models.JSONField()
    post_close_analysis = models.JSONField()

class HolidayAnalysisResult(models.Model):
    # Holiday posting detection
    holiday_postings = models.JSONField()
    holiday_by_fs_line = models.JSONField()
    holiday_by_account = models.JSONField()

class OverallAnalysisResult(models.Model):
    # Comprehensive integration
    transaction_summary = models.JSONField()
    flagged_transactions = models.JSONField()
    risk_assessment = models.JSONField()

class RiskScoringDocument(models.Model):
    # Final risk scoring document
    methodology_overview = models.JSONField()
    risk_factors = models.JSONField()
    risk_distributions = models.JSONField()
    recommendations = models.JSONField()
```

### **CURRENT IMPLEMENTATION EXAMPLES**

#### **Example 1: Holiday Transaction (60 points)**

```json
{
  "transaction_id": "TXN-001",
  "overall_risk_score": 60.0,
  "is_holiday_posting": true,
  "holiday_name": "Christmas",
  "anomaly_types": ["holiday"],
  "anomaly_analysis_summary": {
    "risk_score": 60.0,
    "anomaly_types": ["holiday"],
    "is_duplicate": false,
    "is_backdated": false,
    "is_holiday_posting": true,
    "holiday_name": "Christmas",
    "backdated_days": 0
  },
  "final_classification": "HIGH"
}
```

#### **Example 2: Multiple Anomalies Transaction (70 points)**

```json
{
  "transaction_id": "TXN-002",
  "overall_risk_score": 70.0,
  "is_holiday_posting": false,
  "anomaly_types": ["unusual_days", "closing_entries"],
  "anomaly_analysis_summary": {
    "risk_score": 70.0,
    "anomaly_types": ["unusual_days", "closing_entries"],
    "is_duplicate": false,
    "is_backdated": false,
    "is_holiday_posting": false,
    "holiday_name": null,
    "backdated_days": 0
  },
  "final_classification": "HIGH"
}
```

#### **Example 3: Critical Risk Transaction (80+ points)**

```json
{
  "transaction_id": "TXN-003",
  "overall_risk_score": 80.0,
  "is_duplicate": true,
  "is_backdated": true,
  "is_holiday_posting": true,
  "holiday_name": "New Year's Day",
  "anomaly_types": ["duplicate", "backdated", "holiday"],
  "anomaly_analysis_summary": {
    "risk_score": 80.0,
    "anomaly_types": ["duplicate", "backdated", "holiday"],
    "is_duplicate": true,
    "is_backdated": true,
    "is_holiday_posting": true,
    "holiday_name": "New Year's Day",
    "backdated_days": 15
  },
  "final_classification": "CRITICAL"
}
```

### **CURRENT STATISTICS (10,000 transactions)**

#### **Risk Distribution:**
- **Total Transactions**: 10,000
- **Low Risk**: 6,025 transactions (60.25%)
- **Medium Risk**: 3,437 transactions (34.37%)
- **High Risk**: 513 transactions (5.13%)
- **Critical Risk**: 25 transactions (0.25%)

#### **Anomaly Distribution:**
- **Holiday Postings**: 175 transactions (1.75%)
  - Christmas: 31 transactions
  - Veterans Day: 28 transactions
  - Independence Day: 31 transactions
  - New Year's Day: 27 transactions
- **Unusual Days (Weekend)**: 2,867 transactions (28.67%)
- **Closing Entries**: 1,321 transactions (13.21%)
- **User Anomalies**: 7 users
- **Duplicate Entries**: 0
- **Backdated Entries**: 0

#### **Overall Risk Score:**
- **Current Score**: 75.1/100 (HIGH)
- **Risk Level**: HIGH
- **Total Anomalies**: 4,370 (43.7%)

### **QUERY EXAMPLES**

#### **Find Transactions by Anomaly Type:**
```python
# Find holiday transactions
holiday_transactions = SAPGLPosting.objects.filter(is_holiday_posting=True)

# Find transactions with specific holiday
christmas_transactions = SAPGLPosting.objects.filter(holiday_name='Christmas')

# Find by anomaly type
holiday_transactions = SAPGLPosting.objects.filter(anomaly_types__contains=['holiday'])
duplicate_transactions = SAPGLPosting.objects.filter(anomaly_types__contains=['duplicate'])
backdated_transactions = SAPGLPosting.objects.filter(anomaly_types__contains=['backdated'])

# Find high-risk transactions
high_risk_transactions = SAPGLPosting.objects.filter(overall_risk_score__gte=60)
```

#### **Risk Analysis Queries:**
```python
# Get risk distribution
risk_distribution = {
    'low': SAPGLPosting.objects.filter(overall_risk_score__lt=30).count(),
    'medium': SAPGLPosting.objects.filter(overall_risk_score__gte=30, overall_risk_score__lt=60).count(),
    'high': SAPGLPosting.objects.filter(overall_risk_score__gte=60, overall_risk_score__lt=80).count(),
    'critical': SAPGLPosting.objects.filter(overall_risk_score__gte=80).count()
}

# Get average risk score
avg_risk_score = SAPGLPosting.objects.aggregate(Avg('overall_risk_score'))['overall_risk_score__avg']
```

### **BENEFITS OF CURRENT IMPLEMENTATION**

1. **Comprehensive Analysis Coverage**: 8 different analysis types covering all major risk factors
2. **Simplified Risk Scoring**: Direct assignment based on detected anomalies
3. **Transaction-Level Tracking**: Each transaction marked with specific anomaly flags
4. **Comprehensive Anomaly Summary**: Complete anomaly analysis for each transaction
5. **Easy Querying**: Filter transactions by anomaly type, risk level, or specific flags
6. **Real-time Updates**: Anomaly fields updated during risk analysis
7. **Audit Trail**: Complete record of detected anomalies for each transaction
8. **Weighted Overall Scoring**: Overall risk score based on percentage of high/critical risk transactions
9. **Business Rule Compliance**: Follows specific audit requirements
10. **Synchronous Processing**: All analyses run synchronously for immediate results

### **FUTURE ENHANCEMENTS**

1. **Dynamic Weighting**: Adjust weights based on historical patterns
2. **Machine Learning Integration**: Use ML for risk score optimization
3. **Real-time Scoring**: Implement real-time risk assessment
4. **Custom Thresholds**: Allow user-defined risk thresholds
5. **Advanced Analytics**: Add predictive risk modeling
6. **Analysis-Specific Tuning**: Fine-tune individual analysis contributions
7. **Industry-Specific Rules**: Add industry-specific risk factors
8. **Pattern-Based Analysis**: Enhance structural pattern recognition
9. **Enhanced Holiday Detection**: Support for multiple countries and custom holidays
10. **Advanced User Behavior Analysis**: Machine learning-based user anomaly detection

This comprehensive risk scoring methodology provides a robust, auditable, and business-rule-compliant approach to transaction risk assessment with enhanced transaction-level anomaly tracking across all 8 analysis types. 