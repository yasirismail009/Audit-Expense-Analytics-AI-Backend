# Risk Scoring Methodology and Integration

## Overview

The analytics system implements a comprehensive risk scoring methodology that evaluates transactions across multiple dimensions and integrates the results into an overall risk assessment. This document explains how risk scores are calculated in each analysis type and how they are linked together.

## Risk Scoring Framework

### Core Principles
1. **Multi-Dimensional Assessment**: Risk is evaluated across multiple analysis types
2. **Weighted Integration**: Different analysis types contribute to overall risk based on their significance
3. **Scalable Scoring**: Risk scores range from 0-100, with higher scores indicating higher risk
4. **Business Rule Compliance**: Risk calculations follow specific audit requirements

## Individual Analysis Risk Scoring

### 1. General Analysis Risk Scoring

**Purpose**: Identify transactions with unusual patterns or characteristics that may indicate risk.

**Risk Factors**:
- **High Value Transactions**: Transactions above threshold amounts
- **Unusual Account Patterns**: Transactions in non-standard accounts
- **User Activity Patterns**: Unusual user behavior
- **Temporal Patterns**: Transactions at unusual times or dates
- **Text Analysis**: Presence of suspicious text patterns

**Risk Score Calculation**:
```python
# Base risk score from ML model (0-100)
base_risk = ml_prediction_score

# Additional risk factors
if transaction.is_high_value:
    base_risk += 15.0
if transaction.has_arabic_text:
    base_risk += 10.0
if transaction.amount_local_currency > 10000:
    base_risk += 10.0

# Cap at 100
final_risk = min(base_risk, 100.0)
```

**Risk Levels**:
- **Low Risk (0-30)**: Normal transactions
- **Medium Risk (31-60)**: Some unusual characteristics
- **High Risk (61-85)**: Multiple risk indicators
- **Critical Risk (86-100)**: High-risk patterns detected

### 2. Duplicate Analysis Risk Scoring

**Purpose**: Identify duplicate transactions based on business rules with hierarchical classification.

**Risk Score by Duplicate Type**:
```python
duplicate_types = {
    'type_6': { 'risk_score': 95 },  # Account + Effective Date + Posted Date + User + Source + Amount
    'type_5': { 'risk_score': 90 },  # Account + Effective Date + Amount
    'type_4': { 'risk_score': 85 },  # Account + Posted Date + Amount
    'type_3': { 'risk_score': 80 },  # Account + User + Amount
    'type_2': { 'risk_score': 75 },  # Account + Source + Amount
    'type_1': { 'risk_score': 70 }   # Account + Amount
}
```

**Risk Levels**:
- **Low Risk (70-74)**: Type 1 duplicates
- **Medium Risk (75-84)**: Type 2-3 duplicates
- **High Risk (85-94)**: Type 4-5 duplicates
- **Critical Risk (95-100)**: Type 6 duplicates

**Business Logic**:
- Most specific duplicate type (Type 6) gets highest risk score
- Less specific types get progressively lower scores
- Each transaction pair classified under only one type

### 3. Backdated Analysis Risk Scoring

**Purpose**: Identify transactions where posting date is after document date.

**Risk Score by Days Difference**:
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

**Risk Levels**:
- **Low Risk (50-69)**: 1-7 days difference
- **Medium Risk (70-84)**: 8-14 days difference
- **High Risk (85-99)**: 15-30 days difference
- **Critical Risk (100)**: >30 days difference

**Business Logic**:
- Longer delays indicate higher risk
- Both date fields must be present
- All entries with posting_date > document_date are flagged

## Overall Analysis Risk Integration

### Risk Score Aggregation

The overall analysis combines risk scores from all individual analyses using a weighted approach:

```python
def calculate_overall_risk_score(transaction, general_results, duplicate_results, backdated_results):
    risk_score = 0.0
    
    # Base risk from general analysis
    general_entry = next((g for g in general_results if g['transaction_id'] == str(transaction.id)), None)
    if general_entry:
        general_risk = general_entry.get('risk_score', 0.0)
        # Scale down general risk to prevent over-weighting (max 30 points)
        risk_score += min(general_risk / 3.33, 30.0)  # Scale factor to cap at 30
    
    # Risk from duplicate analysis
    duplicate_found = any(d.get('transaction1', {}).get('id') == str(transaction.id) for d in duplicate_results)
    if duplicate_found:
        duplicate_entry = next((d for d in duplicate_results if d.get('transaction1', {}).get('id') == str(transaction.id)), None)
        if duplicate_entry:
            risk_score += duplicate_entry.get('risk_score', 25.0) / 4.0  # Scale down duplicate risk
    
    # Risk from backdated analysis
    backdated_found = any(b.get('transaction_id') == str(transaction.id) for b in backdated_results)
    if backdated_found:
        backdated_entry = next((b for b in backdated_results if b.get('transaction_id') == str(transaction.id)), None)
        if backdated_entry:
            risk_score += backdated_entry.get('risk_score', 25.0) / 4.0  # Scale down backdated risk
    
    # Additional risk factors
    if transaction.data_file and transaction.data_file.fiscal_year:
        # Check if transaction fiscal year matches file fiscal year
        if transaction.fiscal_year != transaction.data_file.fiscal_year:
            risk_score += 15.0  # Fiscal year mismatch
        
        # Check if transaction date is within audit period
        if transaction.data_file.audit_start_date and transaction.data_file.audit_end_date:
            if transaction.posting_date and (transaction.posting_date < transaction.data_file.audit_start_date or transaction.posting_date > transaction.data_file.audit_end_date):
                risk_score += 20.0  # Transaction outside audit period
    
    return min(risk_score, 100.0)  # Cap at 100
```

### Risk Level Classification

```python
def get_risk_level(risk_score):
    if risk_score >= 80:
        return 'Critical'
    elif risk_score >= 60:
        return 'High'
    elif risk_score >= 30:
        return 'Medium'
    else:
        return 'Low'
```

## Risk Analysis Model

### Purpose
The risk analysis model provides the final risk assessment by analyzing the overall analysis results and categorizing transactions into risk levels.

### Risk Level Classification
```python
def classify_risk_level(overall_risk_score):
    if overall_risk_score >= 80:
        return 3  # Critical Risk
    elif overall_risk_score >= 60:
        return 2  # High Risk
    elif overall_risk_score >= 30:
        return 1  # Medium Risk
    else:
        return 0  # Low Risk
```

### Risk Score Calculation
```python
# Risk analysis uses overall risk scores directly
risk_score = overall_risk_score  # 0-100 scale
risk_level = classify_risk_level(risk_score)
```

## Risk Score Integration Flow

### 1. Individual Analysis Phase
```
Transaction Data
    ↓
┌─────────────────┬─────────────────┬─────────────────┐
│ General Analysis│Duplicate Analysis│Backdated Analysis│
│ Risk Score: 0-100│ Risk Score: 70-95│ Risk Score: 50-100│
└─────────────────┴─────────────────┴─────────────────┘
    ↓
Individual Risk Scores Generated
```

### 2. Overall Analysis Phase
```
Individual Risk Scores
    ↓
Risk Score Aggregation
    ↓
Overall Risk Score (0-100)
    ↓
Risk Level Classification (Low/Medium/High/Critical)
```

### 3. Risk Analysis Phase
```
Overall Risk Scores
    ↓
Final Risk Assessment
    ↓
Risk Level Assignment (0-3)
    ↓
Comprehensive Risk Report
```

## Risk Score Weighting and Scaling

### Weighting Strategy
1. **General Analysis**: Scaled down by 3.33x (max 30 points)
2. **Duplicate Analysis**: Scaled down by 4x (max ~24 points)
3. **Backdated Analysis**: Scaled down by 4x (max ~25 points)
4. **Fiscal Year Validation**: 35 points max
   - Fiscal year mismatch: 15 points
   - Transaction outside audit period: 20 points

### Scaling Rationale
- **Duplicate Risk**: High base scores (70-95) scaled down to prevent over-weighting
- **Backdated Risk**: High base scores (50-100) scaled down for balance
- **General Risk**: Moderate base contribution (30 points) for ML-based detection
- **Fiscal Year Validation**: Critical validation factors (35 points max) for data integrity and audit compliance

## Risk Score Examples

### Example 1: High-Risk Transaction
```python
# Transaction with multiple risk factors
transaction = {
    'amount': 15000,
    'fiscal_year': 2024,
    'file_fiscal_year': 2025,
    'posting_date': '2024-12-15',
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
general_risk = 75.0      # ML detected anomaly
duplicate_risk = 95.0    # Type 6 duplicate
backdated_risk = 85.0    # 20 days difference

# Overall Risk Calculation
overall_risk = (75.0/3.33) + (95.0/4.0) + (85.0/4.0) + 15.0 + 20.0
overall_risk = 22.5 + 23.75 + 21.25 + 15.0 + 20.0 = 102.5
overall_risk = min(102.5, 100.0) = 100.0  # Capped at 100

# Final Classification
risk_level = 'Critical'  # 100 >= 80
```

### Example 2: Medium-Risk Transaction
```python
# Transaction with moderate risk factors
transaction = {
    'amount': 5000,
    'fiscal_year': 2025,
    'file_fiscal_year': 2025,
    'posting_date': '2025-06-15',
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
general_risk = 45.0      # Minor anomaly
duplicate_risk = 0.0     # No duplicate
backdated_risk = 70.0    # 10 days difference

# Overall Risk Calculation
overall_risk = (45.0/3.33) + 0.0 + (70.0/4.0) + 0.0 + 0.0
overall_risk = 13.5 + 0.0 + 17.5 + 0.0 + 0.0 = 31.0

# Final Classification
risk_level = 'Medium'  # 31.0 >= 30
```

### Example 3: Low-Risk Transaction
```python
# Normal transaction
transaction = {
    'amount': 1000,
    'fiscal_year': 2025,
    'file_fiscal_year': 2025,
    'posting_date': '2025-03-15',
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
general_risk = 15.0      # Normal pattern
duplicate_risk = 0.0     # No duplicate
backdated_risk = 0.0     # No backdated

# Overall Risk Calculation
overall_risk = 0.0 + 0.0 + 0.0 + 0.0 + 0.0 = 0.0

# Final Classification
risk_level = 'Low'  # 0 < 30
```

## Risk Score Validation and Quality Assurance

### Validation Checks
1. **Score Range Validation**: All scores must be 0-100
2. **Consistency Checks**: Risk levels must align with scores
3. **Business Rule Compliance**: Scores must follow defined rules
4. **Integration Validation**: Overall scores must be properly aggregated

### Quality Metrics
- **Score Distribution**: Monitor distribution across risk levels
- **Anomaly Detection**: Identify unusual score patterns
- **Threshold Analysis**: Validate risk level thresholds
- **Performance Monitoring**: Track score calculation performance

## Risk Score Reporting

### Individual Analysis Reports
```json
{
    "general_analysis": {
        "risk_scores": [45, 75, 30, 90],
        "risk_levels": ["Medium", "High", "Low", "Critical"],
        "average_risk": 60.0
    },
    "duplicate_analysis": {
        "duplicate_types": {"type_1": 5, "type_6": 2},
        "risk_scores": [70, 95, 85],
        "total_duplicates": 7
    },
    "backdated_analysis": {
        "risk_levels": {"critical": 1, "high": 3, "medium": 2, "low": 1},
        "average_days_difference": 15.2
    }
}
```

### Overall Risk Report
```json
{
    "overall_risk_assessment": {
        "total_transactions": 1000,
        "risk_distribution": {
            "low": 600,
            "medium": 250,
            "high": 120,
            "critical": 30
        },
        "average_risk_score": 35.5,
        "high_risk_amount": 2500000.0
    }
}
```

## Benefits of This Risk Scoring Approach

1. **Comprehensive Assessment**: Multi-dimensional risk evaluation
2. **Business Rule Compliance**: Follows specific audit requirements
3. **Scalable Framework**: Handles varying transaction volumes
4. **Transparent Logic**: Clear calculation methodology
5. **Flexible Integration**: Easy to modify or extend
6. **Audit Trail**: Complete documentation of risk calculations
7. **Performance Optimized**: Efficient calculation algorithms

## Future Enhancements

1. **Dynamic Weighting**: Adjust weights based on historical patterns
2. **Machine Learning Integration**: Use ML for risk score optimization
3. **Real-time Scoring**: Implement real-time risk assessment
4. **Custom Thresholds**: Allow user-defined risk thresholds
5. **Advanced Analytics**: Add predictive risk modeling 