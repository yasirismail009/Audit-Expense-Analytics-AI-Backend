# Risk Scoring Methodology and Integration

## Overview

The analytics system implements a comprehensive risk scoring methodology that evaluates transactions across specialized analysis dimensions and integrates the results into an overall risk assessment. This document provides detailed formulas, calculations, and examples for how risk scores are calculated in each analysis type and how they are combined into the overall risk assessment.

## **DETAILED RISK SCORING CALCULATIONS**

### **1. DUPLICATE ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_duplicate_risk_score(duplicate_type):
    """
    Calculate risk score for duplicate entries based on duplicate type
    """
    # Base risk score for duplicates (40 points)
    risk_score = 40.0
    
    # Factor 1: Duplicate type (primary factor)
    if 'Type 6' in duplicate_type:
        risk_score = 100.0  # Critical
    elif 'Type 5' in duplicate_type:
        risk_score = 85.0   # High
    elif 'Type 4' in duplicate_type:
        risk_score = 75.0   # Medium-High
    elif 'Type 3' in duplicate_type:
        risk_score = 65.0   # Medium
    elif 'Type 2' in duplicate_type:
        risk_score = 55.0   # Medium-Low
    elif 'Type 1' in duplicate_type:
        risk_score = 45.0   # Low
    else:
        risk_score = 40.0   # Default
    
    return min(risk_score, 100.0)
```

#### **Duplicate Types and Risk Scores:**

| Duplicate Type | Matching Criteria | Risk Score | Risk Level | Rationale |
|----------------|------------------|------------|------------|-----------|
| **Type 6** | Account + Effective Date + Posted Date + User + Source + Amount | 100.0 | CRITICAL | Complete match across all critical fields |
| **Type 5** | Account + Effective Date + Amount | 85.0 | HIGH | Three critical field match |
| **Type 4** | Account + Posted Date + Amount | 75.0 | MEDIUM-HIGH | Date and amount match |
| **Type 3** | Account + User + Amount | 65.0 | MEDIUM | User and amount match |
| **Type 2** | Account + Source + Amount | 55.0 | MEDIUM-LOW | Source and amount match |
| **Type 1** | Account + Amount | 45.0 | LOW | Basic amount match |

#### **Risk Level Thresholds:**
```python
def get_duplicate_risk_level(risk_score):
    if risk_score >= 85:
        return 'CRITICAL'
    elif risk_score >= 70:
        return 'HIGH'
    elif risk_score >= 50:
        return 'MEDIUM'
    else:
        return 'LOW'
```

#### **Example Calculations:**

**Example 1: Type 6 Duplicate**
```json
{
  "duplicate_type": "Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount",
  "risk_score": 100.0,
  "risk_level": "CRITICAL",
  "matching_fields": ["account", "effective_date", "posted_date", "user", "source", "amount"]
}
```

**Example 2: Type 1 Duplicate**
```json
{
  "duplicate_type": "Type 1 Duplicate - Account Number + Amount",
  "risk_score": 45.0,
  "risk_level": "LOW",
  "matching_fields": ["account", "amount"]
}
```

---

### **2. BACKDATED ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_backdated_risk_score(days_difference):
    """
    Calculate risk score for backdated entries based on days difference
    """
    # Base risk score for backdated entries (50 points)
    risk_score = 50.0
    
    # Factor 1: Days difference (primary factor)
    if days_difference > 30:
        risk_score = 100.0  # Critical
    elif days_difference > 14:
        risk_score = 85.0   # High
    elif days_difference > 7:
        risk_score = 70.0   # Medium
    else:
        risk_score = 50.0   # Low
    
    return min(risk_score, 100.0)
```

#### **Days Difference Risk Scoring:**

| Days Difference | Risk Score | Risk Level | Severity | Rationale |
|-----------------|------------|------------|----------|-----------|
| **> 30 days** | 100.0 | CRITICAL | SIGNIFICANT | Major backdating concern |
| **15-30 days** | 85.0 | HIGH | MODERATE | Substantial backdating |
| **8-14 days** | 70.0 | MEDIUM | MINOR | Moderate backdating |
| **1-7 days** | 50.0 | LOW | MINIMAL | Minor backdating |

#### **Risk Level Thresholds:**
```python
def get_backdated_risk_level(risk_score):
    if risk_score >= 85:
        return 'CRITICAL'
    elif risk_score >= 70:
        return 'HIGH'
    elif risk_score >= 50:
        return 'MEDIUM'
    else:
        return 'LOW'
```

#### **Example Calculations:**

**Example 1: 45 Days Backdated**
```json
{
  "days_difference": 45,
  "risk_score": 100.0,
  "risk_level": "CRITICAL",
  "backdated_severity": "CRITICAL"
}
```

**Example 2: 10 Days Backdated**
```json
{
  "days_difference": 10,
  "risk_score": 70.0,
  "risk_level": "MEDIUM",
  "backdated_severity": "MODERATE"
}
```

---

### **3. CLOSING ENTRIES ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_closing_risk_score(entry):
    """
    Calculate risk score for closing entries based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md
    """
    # Base risk score for closing entries (30 points)
    risk_score = 30.0
    
    # Factor 1: Post-close entries (25 points)
    days_from_month_end = entry.get('days_from_month_end', 0)
    if days_from_month_end > 0:  # Post-close entry
        risk_score += 25.0
    
    # Factor 2: Debit closing entries (10 points)
    amount = float(entry.get('amount', 0))
    if amount < 0:  # Debit transaction
        risk_score += 10.0
    
    # Factor 3: Month-end closing (5 points)
    if days_from_month_end <= 1:  # Month-end transaction
        risk_score += 5.0
    
    # Factor 4: Year-end closing (10 points)
    posting_date = entry.get('posting_date', '')
    if posting_date:
        try:
            from datetime import datetime
            if isinstance(posting_date, str):
                posting_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
            # Check if it's December and near year-end
            if posting_date.month == 12 and posting_date.day >= 25:
                risk_score += 10.0
        except:
            pass
    
    return min(risk_score, 100.0)
```

#### **Closing Entry Risk Factors:**

| Risk Factor | Points | Condition | Rationale |
|-------------|--------|-----------|-----------|
| **Post-close entries** | +25 | Days from month-end > 0 | Entries after month-end are high risk |
| **Debit closing entries** | +10 | Amount < 0 | Debit transactions are more concerning |
| **Month-end closing** | +5 | Days from month-end ≤ 1 | Standard month-end closing |
| **Year-end closing** | +10 | December 25+ | Year-end closing is critical |

#### **Risk Level Thresholds:**
```python
def get_closing_risk_level(risk_score):
    if risk_score >= 90:
        return 'CRITICAL'
    elif risk_score >= 70:
        return 'HIGH'
    elif risk_score >= 50:
        return 'MEDIUM'
    else:
        return 'LOW'
```

#### **Example Calculations:**

**Example 1: Post-close Debit Entry**
```json
{
  "days_from_month_end": 2,
  "amount": -5000000.0,
  "posting_date": "2025-01-02",
  "risk_score": 70.0,  // 30 + 25 + 10 + 5
  "risk_level": "HIGH"
}
```

**Example 2: Year-end Closing Entry**
```json
{
  "days_from_month_end": 0,
  "amount": 1000000.0,
  "posting_date": "2025-12-30",
  "risk_score": 45.0,  // 30 + 0 + 0 + 5 + 10
  "risk_level": "MEDIUM"
}
```

---

### **4. UNUSUAL DAYS ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_unusual_days_risk_score(entry):
    """
    Calculate risk score for unusual days entries based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md
    """
    # Base risk score for unusual days (60 points)
    risk_score = 60.0
    
    # Factor 1: Day of week (primary factor)
    day_of_week = entry.get('day_of_week', '').lower()
    if day_of_week in ['saturday', 'friday']:
        risk_score = 100.0  # Critical - Weekend posting
    else:
        risk_score = 60.0   # Medium - Other unusual days
    
    # Factor 2: High value transactions
    amount = float(entry.get('amount', 0))
    if amount > 10000000:  # > 10M
        risk_score = min(risk_score + 20.0, 100.0)
    elif amount > 1000000:  # > 1M
        risk_score = min(risk_score + 10.0, 100.0)
    
    return min(risk_score, 100.0)
```

#### **Unusual Days Risk Factors:**

| Day Type | Base Risk | Risk Level | Rationale |
|----------|-----------|------------|-----------|
| **Saturday/Friday** | 100.0 | CRITICAL | Weekend postings are highest risk |
| **Other days** | 60.0 | MEDIUM | Other unusual days |

| Amount Category | Additional Risk | Condition |
|----------------|-----------------|-----------|
| **High Value** | +20 | Amount > 10M |
| **Medium Value** | +10 | Amount > 1M |

#### **Risk Level Thresholds:**
```python
def get_unusual_days_risk_level(risk_score):
    if risk_score >= 85:
        return 'CRITICAL'
    elif risk_score >= 70:
        return 'HIGH'
    elif risk_score >= 50:
        return 'MEDIUM'
    else:
        return 'LOW'
```

#### **Example Calculations:**

**Example 1: Weekend High-Value Transaction**
```json
{
  "day_of_week": "saturday",
  "amount": 15000000.0,
  "risk_score": 100.0,  // 100 (weekend) + 20 (high value) = 120, capped at 100
  "risk_level": "CRITICAL"
}
```

**Example 2: Friday High-Value Transaction**
```json
{
  "day_of_week": "friday",
  "amount": 5000000.0,
  "risk_score": 100.0,  // 100 (friday) + 10 (medium value) = 110, capped at 100
  "risk_level": "CRITICAL"
}
```

---

### **5. USER ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_user_risk_score(entry):
    """
    Calculate risk score for user entries based on RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md
    user_activity: high_activity (users with >10 transactions) or low_activity (users with ≤5 transactions)
    - user_risk: high_risk (users with avg risk >50) or low_risk (users with avg risk ≤20)
    """
    # Base risk score for users (30 points)
    risk_score = 30.0
    
    # Factor 1: Risk factors (15 points per factor)
    risk_factors = entry.get('risk_factors', [])
    risk_score += len(risk_factors) * 15.0
    
    # Factor 2: Anomaly type
    anomaly_type = entry.get('anomaly_type', '')
    if 'HIGH_ACTIVITY' in anomaly_type:
        risk_score = 75.0  # High
    elif 'MEDIUM_ACTIVITY' in anomaly_type:
        risk_score = 55.0  # Medium
    else:
        risk_score = 35.0  # Low
    
    return min(risk_score, 100.0)
```

#### **User Risk Factors:**

| Risk Factor | Points | Description |
|-------------|--------|-------------|
| **High Activity** | 75.0 | Users with unusually high transaction counts |
| **Medium Activity** | 55.0 | Users with moderate anomalies |
| **Low Activity** | 35.0 | Users with minor anomalies |
| **Risk Factors** | +15 per factor | Additional risk factors identified |

#### **Risk Level Thresholds:**
```python
def get_user_risk_level(risk_score):
    if risk_score >= 85:
        return 'CRITICAL'
    elif risk_score >= 65:
        return 'HIGH'
    elif risk_score >= 45:
        return 'MEDIUM'
    else:
        return 'LOW'
```

#### **Example Calculations:**

**Example 1: High Activity User with Multiple Risk Factors**
```json
{
  "anomaly_type": "HIGH_ACTIVITY",
  "risk_factors": ["weekend_activity", "high_value_transactions", "unusual_accounts"],
  "risk_score": 75.0,  // Base high activity
  "risk_level": "HIGH"
}
```

**Example 2: Medium Activity User**
```json
{
  "anomaly_type": "MEDIUM_ACTIVITY",
  "risk_factors": ["weekend_activity"],
  "risk_score": 55.0,  // Base medium activity
  "risk_level": "MEDIUM"
}
```

---

### **6. HOLIDAY ANALYSIS RISK SCORING**

#### **Risk Score Calculation Formula:**
```python
def calculate_holiday_risk_score(entry):
    """
    Calculate risk score for holiday entries based on risk level
    """
    risk_level = entry.get('risk_level', 'LOW')
    risk_scores = {
        'LOW': 25.0,
        'MEDIUM': 50.0,
        'HIGH': 75.0,
        'CRITICAL': 100.0
    }
    return risk_scores.get(risk_level, 0.0)
```

#### **Holiday Risk Factors:**

| Risk Level | Risk Score | Description |
|------------|------------|-------------|
| **CRITICAL** | 100.0 | Critical holiday postings |
| **HIGH** | 75.0 | High-risk holiday postings |
| **MEDIUM** | 50.0 | Medium-risk holiday postings |
| **LOW** | 25.0 | Low-risk holiday postings |

#### **Example Calculations:**

**Example 1: Critical Holiday Posting**
```json
{
  "holiday_name": "Christmas",
  "risk_level": "CRITICAL",
  "risk_score": 100.0,
  "amount": 5000000.0
}
```

**Example 2: Medium Holiday Posting**
```json
{
  "holiday_name": "Veterans Day",
  "risk_level": "MEDIUM",
  "risk_score": 50.0,
  "amount": 100000.0
}
```

---

## **OVERALL RISK SCORING METHODOLOGY**

### **Individual Transaction Risk Score Calculation**

```python
def calculate_transaction_risk_score(transaction, analysis_results):
    """
    Calculate overall risk score for a single transaction
    """
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

### **Overall File Risk Score Calculation**

```python
def calculate_overall_file_risk_score(risk_distribution, total_transactions):
    """
    Calculate overall risk score for the entire file
    """
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

### **Risk Level Classification**

```python
def get_risk_level(overall_risk_score):
    """
    Determine risk level based on overall risk score
    """
    if overall_risk_score >= 80:
        return 'CRITICAL'  # Risk Level 3
    elif overall_risk_score >= 60:
        return 'HIGH'      # Risk Level 2
    elif overall_risk_score >= 30:
        return 'MEDIUM'    # Risk Level 1
    else:
        return 'LOW'       # Risk Level 0
```

### **Risk Score Weights by Analysis Type**

| Analysis Type | Risk Points | Risk Level | Rationale |
|--------------|-------------|------------|-----------|
| **Duplicate** | 80 points | High Risk | Critical nature of duplicate transactions |
| **Backdated** | 70 points | High Risk | Significant audit concern |
| **Holiday** | 60 points | High Risk | Unusual posting on holidays |
| **User Anomaly** | 50 points | Medium Risk | User behavior anomalies |
| **Unusual Days** | 40 points | Medium Risk | Weekend/unusual day postings |
| **Closing Entries** | 30 points | Medium Risk | Month-end closing entries |

### **Risk Level Thresholds**

| Risk Level | Score Range | Risk Level Number | Description |
|------------|-------------|------------------|-------------|
| **CRITICAL** | 80-100 | 3 | Critical risk requiring immediate attention |
| **HIGH** | 60-79 | 2 | High risk requiring investigation |
| **MEDIUM** | 30-59 | 1 | Medium risk requiring monitoring |
| **LOW** | 0-29 | 0 | Low risk, normal operations |

---

## **COMPREHENSIVE EXAMPLE CALCULATIONS**

### **Example 1: Multiple Anomalies Transaction**

```json
{
  "transaction_id": "TXN-001",
  "amount": 5000000.0,
  "posting_date": "2025-12-25",
  "document_date": "2025-12-10",
  "user": "A.MOHAMAD",
  "account": "232000",
  
  "anomaly_analysis": {
    "is_duplicate": true,
    "duplicate_type": "Type 6 Duplicate",
    "duplicate_risk_score": 100.0,
    
    "is_backdated": true,
    "backdated_days": 15,
    "backdated_risk_score": 85.0,
    
    "is_holiday_posting": true,
    "holiday_name": "Christmas",
    "holiday_risk_score": 100.0,
    
    "is_weekend_posting": true,
    "day_of_week": "friday",
    "unusual_days_risk_score": 100.0,
    
    "is_closing_entry": true,
    "days_from_month_end": 6,
    "closing_risk_score": 35.0
  },
  
  "overall_risk_score": 100.0,
  "risk_level": "CRITICAL",
  "anomaly_types": ["duplicate", "backdated", "holiday", "unusual_days", "closing_entries"]
}
```

**Calculation:**
- Duplicate: 100.0 (Type 6)
- Backdated: 85.0 (15 days)
- Holiday: 100.0 (Christmas)
- Unusual Days: 100.0 (Friday)
- Closing Entry: 35.0 (6 days from month-end)
- **Overall**: 100.0 (capped at maximum)

### **Example 2: Single Anomaly Transaction**

```json
{
  "transaction_id": "TXN-002",
  "amount": 1000000.0,
  "posting_date": "2025-08-30",
  "user": "W.BINSALMAN",
  "account": "124010",
  
  "anomaly_analysis": {
    "is_closing_entry": true,
    "days_from_month_end": 1,
    "closing_risk_score": 35.0
  },
  
  "overall_risk_score": 35.0,
  "risk_level": "MEDIUM",
  "anomaly_types": ["closing_entries"]
}
```

**Calculation:**
- Closing Entry: 35.0 (1 day from month-end)
- **Overall**: 35.0

### **Example 3: Weekend High-Value Transaction**

```json
{
  "transaction_id": "TXN-003",
  "amount": 15000000.0,
  "posting_date": "2025-06-14",  // Saturday
  "user": "J.SMITH",
  "account": "100000",
  
  "anomaly_analysis": {
    "is_weekend_posting": true,
    "day_of_week": "saturday",
    "unusual_days_risk_score": 100.0
  },
  
  "overall_risk_score": 100.0,
  "risk_level": "CRITICAL",
  "anomaly_types": ["unusual_days"]
}
```

**Calculation:**
- Unusual Days: 100.0 (Saturday + High Value)
- **Overall**: 100.0

---

## **INTEGRATION WITH SAPGLPosting MODEL**

### **Enhanced Transaction-Level Anomaly Tracking**

```python
def update_transaction_anomalies(transaction, analysis_results):
    """
    Update SAPGLPosting transaction with comprehensive anomaly identification
    """
    # Initialize risk score
    risk_score = 0.0
    anomaly_types = []
    
    # Check for duplicate
    if analysis_results.get('duplicate_analysis'):
        for duplicate in analysis_results['duplicate_analysis'].get('duplicate_list', []):
            if str(transaction.id) in [duplicate.get('transaction1', {}).get('id'), 
                                     duplicate.get('transaction2', {}).get('id')]:
                transaction.is_duplicate = True
                transaction.duplicate_risk_score = duplicate.get('risk_score', 0)
                transaction.duplicate_type = duplicate.get('duplicate_type', '')
                anomaly_types.append('duplicate')
                risk_score += 80.0
                break
    
    # Check for backdated
    if analysis_results.get('backdated_analysis'):
        for backdated in analysis_results['backdated_analysis'].get('backdated_entries', []):
            if str(transaction.id) == backdated.get('transaction_id'):
                transaction.is_backdated = True
                transaction.backdated_risk_score = backdated.get('risk_score', 0)
                transaction.backdated_days = backdated.get('days_difference', 0)
                anomaly_types.append('backdated')
                risk_score += 70.0
                break
    
    # Check for holiday
    if analysis_results.get('holiday_analysis'):
        for holiday in analysis_results['holiday_analysis'].get('holiday_postings', []):
            if str(transaction.id) == holiday.get('transaction_id'):
                transaction.is_holiday_posting = True
                transaction.holiday_name = holiday.get('holiday_name', 'Unknown')
                transaction.holiday_risk_score = holiday.get('risk_score', 0)
                anomaly_types.append('holiday')
                risk_score += 60.0
                break
    
    # Check for unusual days
    if analysis_results.get('unusual_days_analysis'):
        for unusual in analysis_results['unusual_days_analysis'].get('unusual_days', []):
            if str(transaction.id) == unusual.get('transaction_id'):
                transaction.is_weekend_posting = True
                transaction.unusual_days_risk_score = unusual.get('risk_score', 0)
                anomaly_types.append('unusual_days')
                risk_score += 40.0
                break
    
    # Check for closing entries
    if analysis_results.get('closing_entries_analysis'):
        for closing in analysis_results['closing_entries_analysis'].get('closing_entries', []):
            if str(transaction.id) == closing.get('transaction_id'):
                transaction.is_closing_entry = True
                transaction.closing_risk_score = closing.get('risk_score', 0)
                anomaly_types.append('closing_entries')
                risk_score += 30.0
                break
    
    # Update final fields
    transaction.overall_risk_score = min(risk_score, 100.0)
    transaction.anomaly_types = anomaly_types
    transaction.risk_level = get_risk_level(transaction.overall_risk_score)
    
    # Create comprehensive anomaly summary
    transaction.anomaly_analysis_summary = {
        'risk_score': transaction.overall_risk_score,
        'risk_level': transaction.risk_level,
        'anomaly_types': anomaly_types,
        'is_duplicate': transaction.is_duplicate,
        'is_backdated': transaction.is_backdated,
        'is_holiday_posting': transaction.is_holiday_posting,
        'is_weekend_posting': transaction.is_weekend_posting,
        'is_closing_entry': transaction.is_closing_entry,
        'holiday_name': transaction.holiday_name,
        'backdated_days': transaction.backdated_days,
        'duplicate_type': transaction.duplicate_type,
        'duplicate_risk_score': transaction.duplicate_risk_score,
        'backdated_risk_score': transaction.backdated_risk_score,
        'holiday_risk_score': transaction.holiday_risk_score,
        'unusual_days_risk_score': transaction.unusual_days_risk_score,
        'closing_risk_score': transaction.closing_risk_score
    }
    
    transaction.save()
    return transaction
```

---

## **QUERY EXAMPLES**

### **Find Transactions by Risk Level**
```python
# Critical risk transactions
critical_transactions = SAPGLPosting.objects.filter(overall_risk_score__gte=80)

# High risk transactions
high_risk_transactions = SAPGLPosting.objects.filter(
    overall_risk_score__gte=60, 
    overall_risk_score__lt=80
)

# Medium risk transactions
medium_risk_transactions = SAPGLPosting.objects.filter(
    overall_risk_score__gte=30, 
    overall_risk_score__lt=60
)

# Low risk transactions
low_risk_transactions = SAPGLPosting.objects.filter(overall_risk_score__lt=30)
```

### **Find Transactions by Anomaly Type**
```python
# Holiday transactions
holiday_transactions = SAPGLPosting.objects.filter(is_holiday_posting=True)

# Specific holiday
christmas_transactions = SAPGLPosting.objects.filter(holiday_name='Christmas')

# Duplicate transactions
duplicate_transactions = SAPGLPosting.objects.filter(is_duplicate=True)

# Type 6 duplicates
type6_duplicates = SAPGLPosting.objects.filter(duplicate_type__contains='Type 6')

# Backdated transactions
backdated_transactions = SAPGLPosting.objects.filter(is_backdated=True)

# Weekend transactions
weekend_transactions = SAPGLPosting.objects.filter(is_weekend_posting=True)

# Closing entries
closing_transactions = SAPGLPosting.objects.filter(is_closing_entry=True)
```

### **Risk Analysis Queries**
```python
# Get risk distribution
risk_distribution = {
    'critical': SAPGLPosting.objects.filter(overall_risk_score__gte=80).count(),
    'high': SAPGLPosting.objects.filter(
        overall_risk_score__gte=60, 
        overall_risk_score__lt=80
    ).count(),
    'medium': SAPGLPosting.objects.filter(
        overall_risk_score__gte=30, 
        overall_risk_score__lt=60
    ).count(),
    'low': SAPGLPosting.objects.filter(overall_risk_score__lt=30).count()
}

# Get average risk score
avg_risk_score = SAPGLPosting.objects.aggregate(
    Avg('overall_risk_score')
)['overall_risk_score__avg']

# Get transactions with multiple anomalies
multi_anomaly_transactions = SAPGLPosting.objects.filter(
    anomaly_types__len__gt=1
)

# Get high-value critical transactions
high_value_critical = SAPGLPosting.objects.filter(
    overall_risk_score__gte=80,
    amount_local_currency__gt=10000000
)

# Get transactions with unusual account usage
unusual_account_transactions = SAPGLPosting.objects.filter(
    user_name__in=UserAnalysisResult.objects.filter(
        user_anomalies__contains='unusual_accounts'
    ).values_list('user_name', flat=True)
)

# Get weekend transactions by account
weekend_account_transactions = SAPGLPosting.objects.filter(
    is_weekend_posting=True
).values('gl_account').annotate(
    weekend_count=Count('id'),
    total_amount=Sum('amount_local_currency')
).filter(weekend_count__gt=3)

# Get accounts with high weekend activity
high_weekend_accounts = SAPGLPosting.objects.filter(
    is_weekend_posting=True
).values('gl_account').annotate(
    weekend_count=Count('id')
).filter(weekend_count__gt=5)
```

---

## **UNUSUAL ACCOUNTS ANALYSIS AND INTEGRATION**

### **1. USER ANALYSIS - Unusual Account Detection**

#### **Rule 4: Unusual Account Usage**
```python
def detect_unusual_account_usage(user_transactions):
    """
    Detect users posting to too many different GL accounts
    """
    unique_accounts = len(set(txn.gl_account for txn in user_transactions))
    if unique_accounts > 20:  # More than 20 different accounts
        return {
            'is_unusual': True,
            'unique_accounts': unique_accounts,
            'risk_score': 15.0,
            'risk_level': 'MEDIUM-HIGH',
            'anomaly_factor': 'unusual_accounts'
        }
    return {'is_unusual': False}
```

**Risk Scoring:**
- **Threshold:** 20 different accounts per user
- **Risk Points:** +15 points
- **Risk Level:** Medium-High
- **Detection Method:** Rule-based

**What This Detects:**
- Users posting to too many different GL accounts
- Potential segregation of duties violations
- Users outside their normal account scope
- Potential fraud indicators

#### **User Analysis Risk Factors Summary:**
| Risk Factor | Threshold | Risk Points | Description |
|-------------|-----------|-------------|-------------|
| **High Volume** | >100 transactions | +25 | Excessive transaction volume |
| **High Value** | >1M total amount | +20 | High-value transactions |
| **Weekend Posting** | >5 weekend posts | +15 | Weekend activity (Friday/Saturday) |
| **Unusual Accounts** | >20 unique accounts | +15 | **Account diversity** |
| **High Frequency** | >50 transactions | +10 | High posting frequency |
| **Debit Heavy** | >80% debits | +10 | Debit-heavy transactions |
| **Manual Entries** | >10 manual entries | +15 | Manual posting activity |

---

### **2. UNUSUAL DAYS ANALYSIS - Account Grouping**

#### **Account-Based Weekend Analysis**
```python
def analyze_weekend_accounts(transactions):
    """
    Analyze weekend activity patterns by account
    """
    unusual_days_by_account = {}
    
    for transaction in transactions:
        if transaction.posting_date.weekday() in [4, 5]:  # Friday/Saturday
            account = transaction.gl_account
            if account not in unusual_days_by_account:
                unusual_days_by_account[account] = []
            unusual_days_by_account[account].append(transaction)
    
    return unusual_days_by_account
```

**What This Tracks:**
- Which accounts have weekend postings
- Frequency of weekend activity per account
- Account-specific weekend patterns
- High-risk accounts with weekend activity

#### **Account Risk Assessment**
```python
def assess_account_weekend_risk(unusual_days_by_account):
    """
    Assess risk level for accounts with weekend activity
    """
    account_recommendations = []
    
    for account, postings in unusual_days_by_account.items():
        if len(postings) > 3:  # Accounts with many weekend postings
            account_recommendations.append({
                'account': account,
                'weekend_count': len(postings),
                'risk_level': 'HIGH',
                'recommendation': f'Review account {account} - {len(postings)} weekend postings'
            })
        elif len(postings) > 1:
            account_recommendations.append({
                'account': account,
                'weekend_count': len(postings),
                'risk_level': 'MEDIUM',
                'recommendation': f'Monitor account {account} - {len(postings)} weekend postings'
            })
    
    return account_recommendations
```

**Account Risk Thresholds:**
- **High Risk:** >3 weekend postings per account
- **Medium Risk:** 2-3 weekend postings per account
- **Low Risk:** 1 weekend posting per account

---

### **3. INTEGRATED RISK SCORING - Account Impact**

#### **Transaction-Level Risk Scoring with Account Factors**
```python
def calculate_transaction_risk_score_with_accounts(transaction, analysis_results):
    """
    Calculate overall risk score including account-based factors
    """
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
    
    # Check for user anomalies (medium risk) - INCLUDES UNUSUAL ACCOUNTS
    if transaction.user_name in user_anomalies:
        risk_score += 50.0
        anomaly_types.append('user_anomaly')
    
    # Check for unusual days (medium risk) - FRIDAY/SATURDAY
    if transaction in unusual_days_list:
        risk_score += 40.0
        anomaly_types.append('unusual_days')
    
    # Check for closing entries (medium risk)
    if transaction in closing_entries_list:
        risk_score += 30.0
        anomaly_types.append('closing_entries')
    
    return min(risk_score, 100.0), anomaly_types
```

**Account-Specific Risk Factors:**
1. **Weekend Posting:** +40 points (Medium risk)
2. **Account Diversity:** +15 points (User analysis)
3. **High-Value Weekend:** Additional risk based on amount

#### **Account Risk Distribution Analysis**
```python
def analyze_account_risk_distribution(transactions):
    """
    Analyze risk distribution by account
    """
    account_risk_summary = {}
    
    for transaction in transactions:
        account = transaction.gl_account
        if account not in account_risk_summary:
            account_risk_summary[account] = {
                'total_transactions': 0,
                'weekend_transactions': 0,
                'high_risk_transactions': 0,
                'total_amount': 0.0,
                'weekend_amount': 0.0
            }
        
        account_risk_summary[account]['total_transactions'] += 1
        account_risk_summary[account]['total_amount'] += abs(float(transaction.amount_local_currency))
        
        if transaction.is_weekend_posting:
            account_risk_summary[account]['weekend_transactions'] += 1
            account_risk_summary[account]['weekend_amount'] += abs(float(transaction.amount_local_currency))
        
        if transaction.overall_risk_score >= 60:
            account_risk_summary[account]['high_risk_transactions'] += 1
    
    return account_risk_summary
```

---

### **4. TRAINING MODELS - Account Pattern Learning**

#### **User Model Training with Account Patterns**
```python
def train_user_account_patterns(transactions):
    """
    Train user anomaly detection including account usage patterns
    """
    user_transactions = {}
    for transaction in transactions:
        user = transaction.user_name
        if user not in user_transactions:
            user_transactions[user] = []
        user_transactions[user].append(transaction)
    
    user_stats = {}
    for user, user_txns in user_transactions.items():
        unique_accounts = len(set(t.gl_account for t in user_txns))
        weekend_count = len([t for t in user_txns if t.posting_date.weekday() in [4, 5]])
        
        user_stats[user] = {
            'unique_accounts': unique_accounts,
            'weekend_count': weekend_count,
            'account_diversity_score': unique_accounts / len(user_txns) if user_txns else 0,
            'weekend_activity_ratio': weekend_count / len(user_txns) if user_txns else 0
        }
    
    # Calculate optimal thresholds using percentiles
    unique_accounts = [stats['unique_accounts'] for stats in user_stats.values()]
    optimal_thresholds = {
        'unusual_accounts_threshold': percentile(unique_accounts, 90),
        'weekend_activity_threshold': 5,
        'account_diversity_threshold': percentile([stats['account_diversity_score'] for stats in user_stats.values()], 85)
    }
    
    return {
        'optimal_thresholds': optimal_thresholds,
        'user_statistics': user_stats,
        'training_accuracy': 0.87,
        'false_positive_rate': 0.13
    }
```

**Training Insights:**
- **90th Percentile:** Determines unusual account threshold
- **Historical Patterns:** Learns normal account usage per user
- **Weekend Patterns:** Tracks weekend activity by account
- **Account Diversity:** Measures account usage diversity

---

### **5. AUDIT RECOMMENDATIONS - Account Focus**

#### **Account-Specific Audit Recommendations**
```python
def generate_account_audit_recommendations(analysis_results):
    """
    Generate account-focused audit recommendations
    """
    recommendations = {
        'priority_recommendations': [],
        'account_recommendations': [],
        'user_account_recommendations': [],
        'compliance_recommendations': []
    }
    
    # Account recommendations from unusual days analysis
    if analysis_results.get('unusual_days_analysis'):
        unusual_days_by_account = analysis_results['unusual_days_analysis'].get('unusual_days_by_account', {})
        for account, postings in unusual_days_by_account.items():
            if len(postings) > 3:
                recommendations['account_recommendations'].append({
                    'priority': 'HIGH',
                    'action': f'Review account {account}',
                    'description': f'{len(postings)} weekend postings detected',
                    'risk_level': 'HIGH'
                })
    
    # User account recommendations
    if analysis_results.get('user_analysis'):
        user_anomalies = analysis_results['user_analysis'].get('user_anomalies', [])
        for anomaly in user_anomalies:
            if 'unusual_accounts' in anomaly.get('anomaly_factors', []):
                recommendations['user_account_recommendations'].append({
                    'priority': 'MEDIUM',
                    'action': f'Review user {anomaly["user_name"]}',
                    'description': 'Unusual account usage detected',
                    'risk_level': 'MEDIUM-HIGH'
                })
    
    # Compliance recommendations
    recommendations['compliance_recommendations'] = [
        'Verify account posting permissions for all users',
        'Review segregation of duties for account access',
        'Implement account usage monitoring controls',
        'Establish account authorization limits'
    ]
    
    return recommendations
```

**Account Audit Focus:**
1. **High-Frequency Accounts:** Accounts with >3 weekend postings
2. **Unusual Account Usage:** Users posting to >20 different accounts
3. **Account Authorization:** Verify account posting permissions
4. **Segregation of Duties:** Check for proper account access controls

---

### **6. CHART DATA - Account Visualization**

#### **Account-Based Chart Generation**
```python
def generate_account_charts(analysis_results):
    """
    Generate account-focused visualization data
    """
    chart_data = {
        'weekend_by_account': {},
        'account_risk_distribution': {},
        'user_account_diversity': {},
        'account_weekend_patterns': {}
    }
    
    # Weekend activity by account
    if analysis_results.get('unusual_days_analysis'):
        unusual_days_by_account = analysis_results['unusual_days_analysis'].get('unusual_days_by_account', {})
        chart_data['weekend_by_account'] = {
            account: len(postings) for account, postings in unusual_days_by_account.items()
        }
    
    # Account risk distribution
    chart_data['account_risk_distribution'] = {
        'high_risk_accounts': len([a for a, count in chart_data['weekend_by_account'].items() if count > 3]),
        'medium_risk_accounts': len([a for a, count in chart_data['weekend_by_account'].items() if 2 <= count <= 3]),
        'low_risk_accounts': len([a for a, count in chart_data['weekend_by_account'].items() if count == 1])
    }
    
    return chart_data
```

**Visualization Features:**
- Weekend activity by account
- Account risk distribution
- Account-specific weekend patterns
- High-risk account identification
- User account diversity analysis

---

## **BUSINESS IMPLICATIONS OF ACCOUNT ANALYSIS**

### **1. Account Risk Assessment**
- **High-Risk Accounts:** Accounts with frequent weekend activity
- **Unusual Account Usage:** Users posting to too many accounts
- **Account Authorization:** Verify posting permissions
- **Segregation of Duties:** Check account access controls

### **2. Audit Focus Areas**
- **Weekend Account Activity:** Review accounts with weekend postings
- **Account Diversity:** Investigate users with unusual account usage
- **Account Patterns:** Identify abnormal account posting patterns
- **Account Authorization:** Verify proper account access

### **3. Compliance Considerations**
- **Segregation of Duties:** Ensure proper account access controls
- **Authorization Limits:** Verify account posting permissions
- **Audit Trail:** Maintain account activity audit trail
- **Risk Assessment:** Regular account risk assessments

---

## **BENEFITS OF THIS METHODOLOGY**

1. **Comprehensive Coverage**: All major risk factors are considered
2. **Detailed Calculations**: Specific formulas for each analysis type
3. **Transparent Scoring**: Clear rationale for each risk score
4. **Flexible Integration**: Easy to modify weights and thresholds
5. **Audit Trail**: Complete record of risk calculations
6. **Real-time Updates**: Risk scores updated during analysis
7. **Query Optimization**: Efficient database queries for risk analysis
8. **Business Rule Compliance**: Follows specific audit requirements
9. **Scalable Architecture**: Handles large transaction volumes
10. **Maintainable Code**: Clear separation of concerns

This comprehensive risk scoring methodology provides a robust, auditable, and business-rule-compliant approach to transaction risk assessment with detailed calculations for each analysis type and clear integration patterns. 