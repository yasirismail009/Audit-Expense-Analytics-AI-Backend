# Comprehensive Risk Scoring Guide

## Table of Contents
1. [Overview](#overview)
2. [Risk Scoring Framework](#risk-scoring-framework)
3. [Individual Analysis Risk Scoring](#individual-analysis-risk-scoring)
4. [Overall Risk Integration](#overall-risk-integration)
5. [Risk Level Classification](#risk-level-classification)
6. [Complete Risk Calculation Examples](#complete-risk-calculation-examples)
7. [Risk Score Validation](#risk-score-validation)
8. [Implementation Guidelines](#implementation-guidelines)
9. [Risk Reporting](#risk-reporting)
10. [Performance Optimization](#performance-optimization)

## Overview

The analytics system implements a comprehensive risk scoring methodology that evaluates financial transactions across multiple specialized analysis dimensions. This guide provides complete details on how risk scores are calculated, integrated, and classified.

### Core Principles
- **Specialized Analysis Focus**: Risk evaluated through targeted analysis types
- **Weighted Integration**: Different analysis types contribute based on significance
- **Scalable Scoring**: Risk scores range from 0-100, higher scores = higher risk
- **Business Rule Compliance**: Follows specific audit requirements
- **Structural Pattern Focus**: Analysis based on transaction patterns, not monetary amounts

## Risk Scoring Framework

### Analysis Types and Weighting
| Analysis Type | Max Points | Weight | Scaling Factor | Risk Factor Weight |
|---------------|------------|--------|----------------|-------------------|
| Duplicate Analysis | 25 | 25% | ÷4.0 | 25% |
| Backdated Analysis | 20 | 20% | ÷5.0 | 20% |
| User Analysis | 20 | 20% | ÷5.0 | 20% |
| Unusual Days Analysis | 15 | 15% | ÷6.67 | 15% |
| Closing Entries Analysis | 15 | 15% | ÷6.67 | 15% |
| Fiscal Year Validation | 5 | 5% | Direct | 5% |

### Weight Calculation Procedure

#### Step 1: Risk Factor Identification
For each analysis type, identify the specific risk factors that contribute to the overall risk score:

**Duplicate Analysis Risk Factors:**
- Type 6 Duplicate (Account + Effective Date + Posted Date + User + Source + Amount): 95 points
- Type 5 Duplicate (Account + Effective Date + Amount): 90 points
- Type 4 Duplicate (Account + Posted Date + Amount): 85 points
- Type 3 Duplicate (Account + User + Amount): 80 points
- Type 2 Duplicate (Account + Source + Amount): 75 points
- Type 1 Duplicate (Account + Amount): 70 points

**Backdated Analysis Risk Factors:**
- Days Difference > 30: 100 points (Critical)
- Days Difference 15-30: 85 points (High)
- Days Difference 8-14: 70 points (Medium)
- Days Difference 1-7: 50 points (Low)

**User Analysis Risk Factors:**
- Unusual Account Access: +15 points
- Temporal Anomalies: +10 points
- Pattern Deviation: +15 points
- Base User Anomaly Score: 0-100 points

**Unusual Days Analysis Risk Factors:**
- Weekend Posting: 50 points (Base)
- Debit Transaction on Weekend: +10 points
- Month-End Weekend Posting: +10 points
- Year-End Weekend Posting: +10 points

**Closing Entries Analysis Risk Factors:**
- Post-Close Entry: +25 points
- Debit Closing Entry: +10 points
- Month-End Closing: +5 points
- Year-End Closing: +10 points
- Base Closing Entry: 30 points

#### Step 2: Weight Assignment Formula
The weight for each risk factor is calculated using the following formula:

```
Risk Factor Weight = (Individual Risk Factor Score / Total Possible Score) × Analysis Type Weight
```

**Example Calculation for Duplicate Analysis:**
- Analysis Type Weight: 25%
- Type 6 Duplicate Score: 95 points
- Total Possible Score: 95 points (maximum for duplicate)
- Risk Factor Weight: (95/95) × 25% = 25%

**Example Calculation for Backdated Analysis:**
- Analysis Type Weight: 20%
- 35 Days Difference Score: 100 points
- Total Possible Score: 100 points (maximum for backdated)
- Risk Factor Weight: (100/100) × 20% = 20%

#### Step 3: Contribution Calculation Formula
The contribution of each risk factor to the overall risk score is calculated as:

```
Contribution = (Risk Factor Score × Analysis Type Weight) / Scaling Factor
```

**Scaling Factor Purpose:**
- Ensures no single analysis type dominates the overall score
- Maintains balanced risk assessment across all analysis types
- Prevents scores from exceeding 100 points
- Provides consistent weighting across different analysis scales

### Risk Level Thresholds
| Risk Level | Score Range | Description |
|------------|-------------|-------------|
| Critical | 80-100 | Multiple high-risk factors combined |
| High | 60-79 | Significant anomalies requiring immediate attention |
| Medium | 30-59 | Moderate anomalies requiring review |
| Low | 0-29 | Minor anomalies or normal variations |

## Individual Analysis Risk Scoring

### 1. Duplicate Analysis Risk Scoring

**Purpose**: Identify duplicate transactions based on business rules with hierarchical classification.

**Duplicate Types and Risk Scores**:
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

**Risk Factors**:
- Account Matching: Same GL account number
- Date Matching: Same effective date and/or posting date
- User Matching: Same user posting the transaction
- Source Matching: Same source system or document type
- Amount Matching: Same transaction amount
- Temporal Proximity: Transactions posted within short time intervals
- Pattern Consistency: Repeated transaction patterns

### 2. Backdated Analysis Risk Scoring

**Purpose**: Identify transactions where posting date is after document date.

**Risk Score Calculation**:
```python
def calculate_backdated_risk(days_difference):
    if days_difference > 30:
        return 100.0  # Critical
    elif days_difference > 14:
        return 85.0   # High
    elif days_difference > 7:
        return 70.0   # Medium
    else:
        return 50.0   # Low
```

**Risk Factors**:
- Days Difference: Gap between document date and posting date
- Month-End Backdating: Backdated entries near month-end
- Quarter-End Backdating: Backdated entries near quarter-end
- Year-End Backdating: Backdated entries near year-end
- User Backdating Patterns: Users frequently posting backdated entries
- Account Backdating Patterns: Specific accounts with backdated entries
- Temporal Clustering: Multiple backdated entries in short periods

### 3. User Analysis Risk Scoring

**Purpose**: Identify user behavior anomalies and unusual user activity patterns.

**Risk Score Calculation**:
```python
def calculate_user_risk(user_anomaly_score, unusual_accounts, temporal_anomalies, pattern_deviation):
    base_risk = user_anomaly_score
    
    if unusual_accounts > threshold:
        base_risk += 15.0
    if temporal_anomalies:
        base_risk += 10.0
    if pattern_deviation > threshold:
        base_risk += 15.0
    
    return min(base_risk, 100.0)
```

**Risk Factors**:
- Transaction Volume: Users with unusually high transaction counts
- Account Usage Patterns: Users posting to unusual or restricted accounts
- Temporal Anomalies: Users posting at unusual times
- Pattern Deviations: Users deviating from normal posting patterns
- User Role Violations: Users posting outside their typical role
- Geographic Anomalies: Users posting from unusual locations
- Session Patterns: Unusual login/logout patterns
- Batch Processing: Users posting large batches of transactions

### 4. Unusual Days Analysis Risk Scoring

**Purpose**: Identify transactions posted on unusual days, including weekends.

**Risk Score Calculation**:
```python
def calculate_unusual_days_risk(transaction):
    base_risk = 50.0  # Base risk for weekend posting
    
    if transaction.transaction_type == 'DEBIT':
        base_risk += 10.0  # Debit transactions on weekend
    if transaction.posting_date.day >= 25:
        base_risk += 10.0  # Month-end weekend posting
    if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
        base_risk += 10.0  # Year-end weekend posting
    
    return min(base_risk, 100.0)
```

**Risk Factors**:
- Weekend Postings: Transactions posted on Friday/Saturday
- Holiday Postings: Transactions posted on official holidays
- Unusual Day Patterns: Transactions on statistically unusual days
- User Weekend Activity: Users posting frequently on weekends
- Temporal Anomalies: Transactions at unusual times
- Month-End Weekend: Weekend postings near month-end
- Year-End Weekend: Weekend postings near year-end
- Pattern Clustering: Multiple weekend postings by same user

### 5. Closing Entries Analysis Risk Scoring

**Purpose**: Identify journal entries posted during month closing periods.

**Risk Score Calculation**:
```python
def calculate_closing_entries_risk(transaction):
    base_risk = 30.0  # Base risk for closing entry
    
    if is_post_close_entry(transaction.posting_date):
        base_risk += 25.0  # Post-close entry
    if transaction.transaction_type == 'DEBIT':
        base_risk += 10.0  # Debit closing entry
    if transaction.posting_date.day >= 25:
        base_risk += 5.0   # Month-end closing
    if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
        base_risk += 10.0  # Year-end closing
    
    return min(base_risk, 100.0)
```

**Risk Factors**:
- Post-Close Entries: Entries posted after month-end
- Closing Window Violations: Entries outside standard closing windows
- Unusual Closing Patterns: Unusual patterns during closing periods
- User Closing Activity: Users posting frequently during closing
- Temporal Anomalies: Transactions at unusual times during closing
- Account Closing Patterns: Specific accounts with closing entries
- Batch Closing: Large batches of closing entries
- Year-End Closing: Entries during year-end closing periods

## Overall Risk Integration

### Risk Score Aggregation Formula

#### Complete Risk Calculation Formula
```python
def calculate_overall_risk_score(transaction, analysis_results):
    risk_score = 0.0
    
    # Step 1: Calculate individual analysis contributions
    duplicate_contribution = calculate_duplicate_contribution(transaction, analysis_results)
    backdated_contribution = calculate_backdated_contribution(transaction, analysis_results)
    user_contribution = calculate_user_contribution(transaction, analysis_results)
    unusual_days_contribution = calculate_unusual_days_contribution(transaction, analysis_results)
    closing_entries_contribution = calculate_closing_entries_contribution(transaction, analysis_results)
    
    # Step 2: Apply scaling factors and add contributions
    risk_score += duplicate_contribution
    risk_score += backdated_contribution
    risk_score += user_contribution
    risk_score += unusual_days_contribution
    risk_score += closing_entries_contribution
    
    # Step 3: Add fiscal year validation factors
    risk_score += calculate_fiscal_year_validation(transaction)
    
    return min(risk_score, 100.0)

def calculate_duplicate_contribution(transaction, analysis_results):
    """Calculate duplicate analysis contribution with detailed weight calculation"""
    if not analysis_results.get('duplicate'):
        return 0.0
    
    duplicate_score = analysis_results['duplicate'].get('risk_score', 0.0)
    duplicate_type = analysis_results['duplicate'].get('duplicate_type', 'none')
    
    # Weight calculation based on duplicate type
    type_weights = {
        'type_6': 25.0,  # 95/95 * 25% = 25%
        'type_5': 23.7,  # 90/95 * 25% = 23.7%
        'type_4': 22.4,  # 85/95 * 25% = 22.4%
        'type_3': 21.1,  # 80/95 * 25% = 21.1%
        'type_2': 19.7,  # 75/95 * 25% = 19.7%
        'type_1': 18.4   # 70/95 * 25% = 18.4%
    }
    
    weight = type_weights.get(duplicate_type, 0.0)
    contribution = (duplicate_score * weight) / 4.0  # Scaling factor
    return min(contribution, 25.0)  # Cap at max points

def calculate_backdated_contribution(transaction, analysis_results):
    """Calculate backdated analysis contribution with detailed weight calculation"""
    if not analysis_results.get('backdated'):
        return 0.0
    
    days_difference = analysis_results['backdated'].get('days_difference', 0)
    
    # Risk score calculation based on days difference
    if days_difference > 30:
        risk_score = 100.0
        weight = 20.0  # 100/100 * 20% = 20%
    elif days_difference > 14:
        risk_score = 85.0
        weight = 17.0  # 85/100 * 20% = 17%
    elif days_difference > 7:
        risk_score = 70.0
        weight = 14.0  # 70/100 * 20% = 14%
    else:
        risk_score = 50.0
        weight = 10.0  # 50/100 * 20% = 10%
    
    contribution = (risk_score * weight) / 5.0  # Scaling factor
    return min(contribution, 20.0)  # Cap at max points

def calculate_user_contribution(transaction, analysis_results):
    """Calculate user analysis contribution with detailed weight calculation"""
    if not analysis_results.get('user'):
        return 0.0
    
    base_score = analysis_results['user'].get('user_anomaly_score', 0.0)
    unusual_accounts = analysis_results['user'].get('unusual_accounts', 0)
    temporal_anomalies = analysis_results['user'].get('temporal_anomalies', False)
    pattern_deviation = analysis_results['user'].get('pattern_deviation', 0.0)
    
    # Calculate total user risk score
    total_score = base_score
    if unusual_accounts > 5:  # Threshold
        total_score += 15.0
    if temporal_anomalies:
        total_score += 10.0
    if pattern_deviation > 0.5:  # Threshold
        total_score += 15.0
    
    total_score = min(total_score, 100.0)  # Cap at 100
    weight = (total_score / 100.0) * 20.0  # 20% analysis weight
    
    contribution = (total_score * weight) / 5.0  # Scaling factor
    return min(contribution, 20.0)  # Cap at max points

def calculate_unusual_days_contribution(transaction, analysis_results):
    """Calculate unusual days analysis contribution with detailed weight calculation"""
    if not analysis_results.get('unusual_days'):
        return 0.0
    
    base_score = 50.0  # Base weekend posting risk
    additional_factors = analysis_results['unusual_days'].get('additional_factors', [])
    
    # Add risk factors
    if 'debit_transaction' in additional_factors:
        base_score += 10.0
    if 'month_end_posting' in additional_factors:
        base_score += 10.0
    if 'year_end_posting' in additional_factors:
        base_score += 10.0
    
    total_score = min(base_score, 100.0)  # Cap at 100
    weight = (total_score / 100.0) * 15.0  # 15% analysis weight
    
    contribution = (total_score * weight) / 6.67  # Scaling factor
    return min(contribution, 15.0)  # Cap at max points

def calculate_closing_entries_contribution(transaction, analysis_results):
    """Calculate closing entries analysis contribution with detailed weight calculation"""
    if not analysis_results.get('closing_entries'):
        return 0.0
    
    base_score = 30.0  # Base closing entry risk
    additional_factors = analysis_results['closing_entries'].get('additional_factors', [])
    
    # Add risk factors
    if 'post_close_entry' in additional_factors:
        base_score += 25.0
    if 'debit_closing_entry' in additional_factors:
        base_score += 10.0
    if 'month_end_closing' in additional_factors:
        base_score += 5.0
    if 'year_end_closing' in additional_factors:
        base_score += 10.0
    
    total_score = min(base_score, 100.0)  # Cap at 100
    weight = (total_score / 100.0) * 15.0  # 15% analysis weight
    
    contribution = (total_score * weight) / 6.67  # Scaling factor
    return min(contribution, 15.0)  # Cap at max points

def calculate_fiscal_year_validation(transaction):
    """Calculate fiscal year validation contribution"""
    risk_score = 0.0
    
    # Fiscal year mismatch (3 points)
    if transaction.fiscal_year != transaction.data_file.fiscal_year:
        risk_score += 3.0
    
    # Audit period violation (2 points)
    if transaction.posting_date < transaction.data_file.audit_start_date or \
       transaction.posting_date > transaction.data_file.audit_end_date:
        risk_score += 2.0
    
    return risk_score
```

#### Weight Calculation Matrix
| Risk Factor | Base Score | Max Score | Analysis Weight | Calculated Weight | Scaling Factor | Max Contribution |
|-------------|------------|-----------|----------------|-------------------|----------------|------------------|
| Type 6 Duplicate | 95 | 95 | 25% | 25.0% | ÷4.0 | 23.75 |
| Type 5 Duplicate | 90 | 95 | 25% | 23.7% | ÷4.0 | 22.28 |
| Type 4 Duplicate | 85 | 95 | 25% | 22.4% | ÷4.0 | 21.00 |
| Type 3 Duplicate | 80 | 95 | 25% | 21.1% | ÷4.0 | 19.75 |
| Type 2 Duplicate | 75 | 95 | 25% | 19.7% | ÷4.0 | 18.50 |
| Type 1 Duplicate | 70 | 95 | 25% | 18.4% | ÷4.0 | 17.25 |
| Backdated >30 days | 100 | 100 | 20% | 20.0% | ÷5.0 | 20.00 |
| Backdated 15-30 days | 85 | 100 | 20% | 17.0% | ÷5.0 | 17.00 |
| Backdated 8-14 days | 70 | 100 | 20% | 14.0% | ÷5.0 | 14.00 |
| Backdated 1-7 days | 50 | 100 | 20% | 10.0% | ÷5.0 | 10.00 |
| User Anomaly 100% | 100 | 100 | 20% | 20.0% | ÷5.0 | 20.00 |
| User Anomaly 75% | 75 | 100 | 20% | 15.0% | ÷5.0 | 15.00 |
| User Anomaly 50% | 50 | 100 | 20% | 10.0% | ÷5.0 | 10.00 |
| Weekend + Debit + Year-end | 80 | 100 | 15% | 12.0% | ÷6.67 | 12.00 |
| Weekend + Debit | 60 | 100 | 15% | 9.0% | ÷6.67 | 9.00 |
| Basic Weekend | 50 | 100 | 15% | 7.5% | ÷6.67 | 7.50 |
| Post-close + Debit + Year-end | 80 | 100 | 15% | 12.0% | ÷6.67 | 12.00 |
| Post-close + Debit | 65 | 100 | 15% | 9.75% | ÷6.67 | 9.75 |
| Basic Post-close | 55 | 100 | 15% | 8.25% | ÷6.67 | 8.25 |

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

## Complete Risk Calculation Examples

### Example 1: Critical Risk Transaction

**Transaction Details**:
```json
{
  "transaction_id": "TXN-001",
  "gl_account": "5000",
  "user_name": "john.doe",
  "document_date": "2024-01-15",
  "posting_date": "2024-12-28",
  "transaction_type": "DEBIT",
  "source": "SAP_FI",
  "fiscal_year": 2024,
  "file_fiscal_year": 2025
}
```

**Individual Analysis Results**:
- Duplicate Analysis: Type 6 duplicate (95.0 points)
- Backdated Analysis: 35 days difference (100.0 points)
- User Analysis: Critical user anomaly (95.0 points)
- Unusual Days Analysis: Weekend posting (100.0 points)
- Closing Entries Analysis: Post-close entry (90.0 points)

**Overall Risk Calculation**:
```
Step 1: Individual Analysis Contributions
- Duplicate Analysis: Type 6 (95 points) → Weight: 25.0% → Contribution: (95 × 25.0%) ÷ 4.0 = 23.75 points
- Backdated Analysis: 35 days (100 points) → Weight: 20.0% → Contribution: (100 × 20.0%) ÷ 5.0 = 20.0 points
- User Analysis: Critical anomaly (95 points) → Weight: 20.0% → Contribution: (95 × 20.0%) ÷ 5.0 = 19.0 points
- Unusual Days Analysis: Weekend + Debit + Year-end (80 points) → Weight: 12.0% → Contribution: (80 × 12.0%) ÷ 6.67 = 15.0 points
- Closing Entries Analysis: Post-close + Debit + Year-end (80 points) → Weight: 12.0% → Contribution: (80 × 12.0%) ÷ 6.67 = 13.5 points

Step 2: Fiscal Year Validation
- Fiscal Year Mismatch: 3.0 points
- Audit Period Violation: 2.0 points

Step 3: Total Calculation
Total Risk Score: 23.75 + 20.0 + 19.0 + 15.0 + 13.5 + 3.0 + 2.0 = 96.25 points
Final Classification: Critical (96.25 ≥ 80)
```

### Example 2: High Risk Transaction

**Transaction Details**:
```json
{
  "transaction_id": "TXN-002",
  "gl_account": "4000",
  "user_name": "jane.smith",
  "document_date": "2024-11-10",
  "posting_date": "2024-11-25",
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025
}
```

**Individual Analysis Results**:
- Duplicate Analysis: No duplicate (0.0 points)
- Backdated Analysis: 15 days difference (85.0 points)
- User Analysis: User anomaly (75.0 points)
- Unusual Days Analysis: No unusual days (0.0 points)
- Closing Entries Analysis: No closing entry (0.0 points)

**Overall Risk Calculation**:
```
Step 1: Individual Analysis Contributions
- Duplicate Analysis: No duplicate → Contribution: 0.0 points
- Backdated Analysis: 15 days (85 points) → Weight: 17.0% → Contribution: (85 × 17.0%) ÷ 5.0 = 17.0 points
- User Analysis: User anomaly (75 points) → Weight: 15.0% → Contribution: (75 × 15.0%) ÷ 5.0 = 15.0 points
- Unusual Days Analysis: No unusual days → Contribution: 0.0 points
- Closing Entries Analysis: No closing entry → Contribution: 0.0 points

Step 2: Fiscal Year Validation
- Fiscal Year Mismatch: 0.0 points
- Audit Period Violation: 0.0 points

Step 3: Total Calculation
Total Risk Score: 0.0 + 17.0 + 15.0 + 0.0 + 0.0 + 0.0 + 0.0 = 32.0 points
Final Classification: Medium (30 ≤ 32.0 < 60)
```

### Example 3: Medium Risk Transaction

**Transaction Details**:
```json
{
  "transaction_id": "TXN-003",
  "gl_account": "3000",
  "user_name": "mike.wilson",
  "document_date": "2024-12-15",
  "posting_date": "2024-12-22",
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025
}
```

**Individual Analysis Results**:
- Duplicate Analysis: No duplicate (0.0 points)
- Backdated Analysis: 7 days difference (70.0 points)
- User Analysis: Minor user anomaly (40.0 points)
- Unusual Days Analysis: No unusual days (0.0 points)
- Closing Entries Analysis: No closing entry (0.0 points)

**Overall Risk Calculation**:
```
Step 1: Individual Analysis Contributions
- Duplicate Analysis: No duplicate → Contribution: 0.0 points
- Backdated Analysis: 7 days (70 points) → Weight: 14.0% → Contribution: (70 × 14.0%) ÷ 5.0 = 14.0 points
- User Analysis: Minor anomaly (40 points) → Weight: 8.0% → Contribution: (40 × 8.0%) ÷ 5.0 = 8.0 points
- Unusual Days Analysis: No unusual days → Contribution: 0.0 points
- Closing Entries Analysis: No closing entry → Contribution: 0.0 points

Step 2: Fiscal Year Validation
- Fiscal Year Mismatch: 0.0 points
- Audit Period Violation: 0.0 points

Step 3: Total Calculation
Total Risk Score: 0.0 + 14.0 + 8.0 + 0.0 + 0.0 + 0.0 + 0.0 = 22.0 points
Final Classification: Low (22.0 < 30)
```

### Example 4: Low Risk Transaction

**Transaction Details**:
```json
{
  "transaction_id": "TXN-004",
  "gl_account": "1000",
  "user_name": "tom.anderson",
  "document_date": "2024-12-20",
  "posting_date": "2024-12-20",
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025
}
```

**Individual Analysis Results**:
- Duplicate Analysis: No duplicate (0.0 points)
- Backdated Analysis: No backdating (0.0 points)
- User Analysis: Normal user activity (10.0 points)
- Unusual Days Analysis: No unusual days (0.0 points)
- Closing Entries Analysis: No closing entry (0.0 points)

**Overall Risk Calculation**:
```
Step 1: Individual Analysis Contributions
- Duplicate Analysis: No duplicate → Contribution: 0.0 points
- Backdated Analysis: No backdating → Contribution: 0.0 points
- User Analysis: Normal activity (10 points) → Weight: 2.0% → Contribution: (10 × 2.0%) ÷ 5.0 = 2.0 points
- Unusual Days Analysis: No unusual days → Contribution: 0.0 points
- Closing Entries Analysis: No closing entry → Contribution: 0.0 points

Step 2: Fiscal Year Validation
- Fiscal Year Mismatch: 0.0 points
- Audit Period Violation: 0.0 points

Step 3: Total Calculation
Total Risk Score: 0.0 + 0.0 + 2.0 + 0.0 + 0.0 + 0.0 + 0.0 = 2.0 points
Final Classification: Low (2.0 < 30)
```

## Risk Score Validation

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

## Detailed Weight Calculation Procedures

### Procedure 1: Risk Factor Weight Determination

#### Step 1: Identify Risk Factor Base Score
For each risk factor, determine the base score based on the specific condition:

**Duplicate Analysis Base Scores:**
```python
def get_duplicate_base_score(duplicate_type):
    base_scores = {
        'type_6': 95,  # Account + Effective Date + Posted Date + User + Source + Amount
        'type_5': 90,  # Account + Effective Date + Amount
        'type_4': 85,  # Account + Posted Date + Amount
        'type_3': 80,  # Account + User + Amount
        'type_2': 75,  # Account + Source + Amount
        'type_1': 70   # Account + Amount
    }
    return base_scores.get(duplicate_type, 0)
```

**Backdated Analysis Base Scores:**
```python
def get_backdated_base_score(days_difference):
    if days_difference > 30:
        return 100.0  # Critical
    elif days_difference > 14:
        return 85.0   # High
    elif days_difference > 7:
        return 70.0   # Medium
    else:
        return 50.0   # Low
```

#### Step 2: Calculate Risk Factor Weight
Use the formula: `Risk Factor Weight = (Base Score / Maximum Possible Score) × Analysis Type Weight`

**Example Calculations:**
```python
# Duplicate Analysis (Analysis Type Weight: 25%)
type_6_weight = (95 / 95) × 25% = 25.0%
type_5_weight = (90 / 95) × 25% = 23.7%
type_4_weight = (85 / 95) × 25% = 22.4%

# Backdated Analysis (Analysis Type Weight: 20%)
backdated_35_days_weight = (100 / 100) × 20% = 20.0%
backdated_15_days_weight = (85 / 100) × 20% = 17.0%
backdated_7_days_weight = (70 / 100) × 20% = 14.0%
```

### Procedure 2: Contribution Calculation

#### Step 1: Apply Scaling Factor
Use the formula: `Contribution = (Risk Factor Score × Risk Factor Weight) / Scaling Factor`

**Scaling Factors by Analysis Type:**
- Duplicate Analysis: ÷4.0
- Backdated Analysis: ÷5.0
- User Analysis: ÷5.0
- Unusual Days Analysis: ÷6.67
- Closing Entries Analysis: ÷6.67

#### Step 2: Cap at Maximum Points
Ensure contribution doesn't exceed the maximum points for each analysis type:
- Duplicate Analysis: 25 points max
- Backdated Analysis: 20 points max
- User Analysis: 20 points max
- Unusual Days Analysis: 15 points max
- Closing Entries Analysis: 15 points max

### Procedure 3: Composite Risk Factor Calculation

#### Step 1: Multiple Risk Factors in Single Analysis
When multiple risk factors apply to the same analysis, calculate the composite score:

**User Analysis Example:**
```python
def calculate_composite_user_score(base_score, unusual_accounts, temporal_anomalies, pattern_deviation):
    composite_score = base_score
    
    # Add additional risk factors
    if unusual_accounts > 5:
        composite_score += 15.0
    if temporal_anomalies:
        composite_score += 10.0
    if pattern_deviation > 0.5:
        composite_score += 15.0
    
    return min(composite_score, 100.0)  # Cap at 100
```

#### Step 2: Weight Calculation for Composite Score
```python
def calculate_composite_weight(composite_score, analysis_weight):
    return (composite_score / 100.0) × analysis_weight
```

### Procedure 4: Fiscal Year Validation Weight

#### Step 1: Direct Point Assignment
Fiscal year validation uses direct point assignment without scaling:

```python
def calculate_fiscal_year_weight(transaction):
    weight = 0.0
    
    # Fiscal year mismatch: 3 points
    if transaction.fiscal_year != transaction.data_file.fiscal_year:
        weight += 3.0
    
    # Audit period violation: 2 points
    if transaction.posting_date < transaction.data_file.audit_start_date or \
       transaction.posting_date > transaction.data_file.audit_end_date:
        weight += 2.0
    
    return weight
```

### Formula Summary

#### Primary Risk Calculation Formula
```
Overall Risk Score = Σ(Individual Analysis Contributions) + Fiscal Year Validation
```

#### Individual Analysis Contribution Formula
```
Contribution = min((Risk Factor Score × Risk Factor Weight) / Scaling Factor, Max Points)
```

#### Risk Factor Weight Formula
```
Risk Factor Weight = (Base Score / Maximum Possible Score) × Analysis Type Weight
```

#### Composite Risk Factor Formula
```
Composite Score = min(Σ(Individual Risk Factors), 100)
Composite Weight = (Composite Score / 100) × Analysis Type Weight
```

### Weight Calculation Examples

#### Example 1: Type 6 Duplicate Transaction
```python
# Base score calculation
base_score = 95  # Type 6 duplicate

# Weight calculation
analysis_weight = 25%  # Duplicate analysis weight
max_possible_score = 95  # Maximum for duplicate analysis
risk_factor_weight = (95 / 95) × 25% = 25.0%

# Contribution calculation
scaling_factor = 4.0
contribution = (95 × 25.0%) / 4.0 = 23.75 points
final_contribution = min(23.75, 25.0) = 23.75 points
```

#### Example 2: Backdated Transaction (35 days)
```python
# Base score calculation
base_score = 100  # 35 days difference

# Weight calculation
analysis_weight = 20%  # Backdated analysis weight
max_possible_score = 100  # Maximum for backdated analysis
risk_factor_weight = (100 / 100) × 20% = 20.0%

# Contribution calculation
scaling_factor = 5.0
contribution = (100 × 20.0%) / 5.0 = 20.0 points
final_contribution = min(20.0, 20.0) = 20.0 points
```

#### Example 3: User Anomaly with Multiple Factors
```python
# Composite score calculation
base_score = 60
unusual_accounts = 8  # > 5 threshold
temporal_anomalies = True
pattern_deviation = 0.7  # > 0.5 threshold

composite_score = 60 + 15 + 10 + 15 = 100
composite_score = min(100, 100) = 100

# Weight calculation
analysis_weight = 20%  # User analysis weight
risk_factor_weight = (100 / 100) × 20% = 20.0%

# Contribution calculation
scaling_factor = 5.0
contribution = (100 × 20.0%) / 5.0 = 20.0 points
final_contribution = min(20.0, 20.0) = 20.0 points
```

## Implementation Guidelines

### Database Schema
```sql
-- Risk scoring results table
CREATE TABLE risk_scoring_results (
    id UUID PRIMARY KEY,
    transaction_id UUID REFERENCES sapgl_posting(id),
    duplicate_risk_score DECIMAL(5,2),
    backdated_risk_score DECIMAL(5,2),
    user_risk_score DECIMAL(5,2),
    unusual_days_risk_score DECIMAL(5,2),
    closing_entries_risk_score DECIMAL(5,2),
    overall_risk_score DECIMAL(5,2),
    risk_level VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Risk analysis summary table
CREATE TABLE risk_analysis_summary (
    id UUID PRIMARY KEY,
    data_file_id UUID REFERENCES data_file(id),
    total_transactions INTEGER,
    critical_risk_count INTEGER,
    high_risk_count INTEGER,
    medium_risk_count INTEGER,
    low_risk_count INTEGER,
    average_risk_score DECIMAL(5,2),
    created_at TIMESTAMP
);
```

### Python Implementation
```python
class RiskScoringEngine:
    def __init__(self):
        self.duplicate_analyzer = DuplicateAnalyzer()
        self.backdated_analyzer = BackdatedAnalyzer()
        self.user_analyzer = UserAnalyzer()
        self.unusual_days_analyzer = UnusualDaysAnalyzer()
        self.closing_entries_analyzer = ClosingEntriesAnalyzer()
    
    def calculate_risk_scores(self, transactions):
        results = []
        
        for transaction in transactions:
            # Individual analysis
            duplicate_score = self.duplicate_analyzer.analyze(transaction)
            backdated_score = self.backdated_analyzer.analyze(transaction)
            user_score = self.user_analyzer.analyze(transaction)
            unusual_days_score = self.unusual_days_analyzer.analyze(transaction)
            closing_entries_score = self.closing_entries_analyzer.analyze(transaction)
            
            # Overall risk calculation
            overall_score = self.calculate_overall_risk(
                duplicate_score, backdated_score, user_score,
                unusual_days_score, closing_entries_score, transaction
            )
            
            risk_level = self.classify_risk_level(overall_score)
            
            results.append({
                'transaction_id': transaction.id,
                'duplicate_risk_score': duplicate_score,
                'backdated_risk_score': backdated_score,
                'user_risk_score': user_score,
                'unusual_days_risk_score': unusual_days_score,
                'closing_entries_risk_score': closing_entries_score,
                'overall_risk_score': overall_score,
                'risk_level': risk_level
            })
        
        return results
    
    def calculate_overall_risk(self, duplicate, backdated, user, unusual_days, closing_entries, transaction):
        risk_score = 0.0
        
        # Weighted contributions
        risk_score += min(duplicate / 4.0, 25.0)
        risk_score += min(backdated / 5.0, 20.0)
        risk_score += min(user / 5.0, 20.0)
        risk_score += min(unusual_days / 6.67, 15.0)
        risk_score += min(closing_entries / 6.67, 15.0)
        
        # Additional factors
        if transaction.fiscal_year != transaction.data_file.fiscal_year:
            risk_score += 3.0
        
        if transaction.posting_date < transaction.data_file.audit_start_date or \
           transaction.posting_date > transaction.data_file.audit_end_date:
            risk_score += 2.0
        
        return min(risk_score, 100.0)
    
    def classify_risk_level(self, risk_score):
        if risk_score >= 80:
            return 'Critical'
        elif risk_score >= 60:
            return 'High'
        elif risk_score >= 30:
            return 'Medium'
        else:
            return 'Low'
```

## Risk Reporting

### Individual Transaction Report
```json
{
  "transaction_id": "TXN-001",
  "risk_assessment": {
    "overall_risk_score": 96.25,
    "risk_level": "Critical",
    "risk_factors": [
      "duplicate_transaction",
      "backdated_entry",
      "user_anomaly",
      "weekend_posting",
      "post_close_entry"
    ],
    "analysis_breakdown": {
      "duplicate_analysis": {
        "score": 95.0,
        "contribution": 23.75,
        "type": "type_6"
      },
      "backdated_analysis": {
        "score": 100.0,
        "contribution": 20.0,
        "days_difference": 35
      },
      "user_analysis": {
        "score": 95.0,
        "contribution": 19.0,
        "anomalies": ["weekend_posting", "backdated_entry"]
      },
      "unusual_days_analysis": {
        "score": 100.0,
        "contribution": 15.0,
        "factors": ["weekend_posting", "debit_transaction"]
      },
      "closing_entries_analysis": {
        "score": 90.0,
        "contribution": 13.5,
        "type": "post_close_entry"
      }
    },
    "recommendations": [
      "IMMEDIATE_REVIEW",
      "FREEZE_ACCOUNT",
      "MANAGEMENT_NOTIFICATION"
    ]
  }
}
```

### Summary Report
```json
{
  "risk_summary": {
    "total_transactions": 1000,
    "risk_distribution": {
      "critical": 70,
      "high": 180,
      "medium": 250,
      "low": 500
    },
    "average_risk_score": 42.5,
    "analysis_contributions": {
      "duplicate_analysis": 18.2,
      "backdated_analysis": 12.8,
      "user_analysis": 11.3,
      "unusual_days_analysis": 8.2,
      "closing_entries_analysis": 7.8,
      "fiscal_year_validation": 2.7
    },
    "high_risk_transactions": [
      {
        "transaction_id": "TXN-001",
        "risk_score": 96.25,
        "risk_level": "Critical"
      }
    ],
    "recommendations": {
      "immediate_actions": [
        "Review all critical risk transactions",
        "Investigate high-risk user patterns",
        "Validate duplicate transaction pairs"
      ],
      "follow_up_actions": [
        "Implement enhanced controls",
        "Conduct user training",
        "Review system access permissions"
      ]
    }
  }
}
```

## Performance Optimization

### Caching Strategy
```python
class RiskScoreCache:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour
    
    def get_cached_score(self, transaction_id):
        if transaction_id in self.cache:
            cached_data = self.cache[transaction_id]
            if time.time() - cached_data['timestamp'] < self.cache_ttl:
                return cached_data['score']
        return None
    
    def cache_score(self, transaction_id, score):
        self.cache[transaction_id] = {
            'score': score,
            'timestamp': time.time()
        }
```

### Batch Processing
```python
def process_risk_scores_batch(transactions, batch_size=1000):
    results = []
    
    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i + batch_size]
        batch_results = calculate_risk_scores_batch(batch)
        results.extend(batch_results)
    
    return results
```

### Parallel Processing
```python
from concurrent.futures import ThreadPoolExecutor

def calculate_risk_scores_parallel(transactions, max_workers=4):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(calculate_single_risk_score, t) for t in transactions]
        results = [future.result() for future in futures]
    
    return results
```

## Conclusion

This comprehensive risk scoring guide provides a complete framework for implementing and maintaining risk assessment in financial transaction analysis. The methodology ensures:

1. **Accuracy**: Precise risk calculation based on business rules
2. **Scalability**: Efficient processing of large transaction volumes
3. **Transparency**: Clear calculation methodology and documentation
4. **Flexibility**: Adaptable to different business requirements
5. **Compliance**: Adherence to audit and regulatory standards

The risk scoring system provides actionable insights for audit teams, enabling them to focus on high-risk transactions while maintaining efficient processing of normal business activities. 