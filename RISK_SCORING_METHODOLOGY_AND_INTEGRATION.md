# Risk Scoring Methodology and Integration
u2D7OQ*@n
## Overview

The analytics system implements a comprehensive risk scoring methodology that evaluates transactions across specialized analysis dimensions and integrates the results into an overall risk assessment. This document explains how risk scores are calculated in each analysis type and how they are linked together.

## Comprehensive Risk Scoring Methodology - Detailed Factor Analysis

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

### **PHASE 2: OVERALL ANALYSIS RISK INTEGRATION**

#### **Risk Score Aggregation Formula**

The overall risk score combines all specialized analyses using weighted scaling:

```python
def calculate_overall_risk_score(transaction, duplicate_results, backdated_results, user_results, unusual_days_results, closing_entries_results):
    risk_score = 0.0
    
    # 1. Duplicate Analysis Contribution (max 25 points)
    # Scaled down by 4x: risk_score / 4.0
    duplicate_contribution = min(duplicate_risk_score / 4.0, 25.0)
    
    # 2. Backdated Analysis Contribution (max 20 points)
    # Scaled down by 5x: risk_score / 5.0
    backdated_contribution = min(backdated_risk_score / 5.0, 20.0)
    
    # 3. User Analysis Contribution (max 20 points)
    # Scaled down by 5x: risk_score / 5.0
    user_contribution = min(user_risk_score / 5.0, 20.0)
    
    # 4. Unusual Days Analysis Contribution (max 15 points)
    # Scaled down by 6.67x: risk_score / 6.67
    unusual_days_contribution = min(unusual_days_risk_score / 6.67, 15.0)
    
    # 5. Closing Entries Analysis Contribution (max 15 points)
    # Scaled down by 6.67x: risk_score / 6.67
    closing_entries_contribution = min(closing_entries_risk_score / 6.67, 15.0)
    
    # 6. Fiscal Year Validation (max 5 points)
    fiscal_year_mismatch = 3.0 if transaction.fiscal_year != file_fiscal_year else 0.0
    audit_period_violation = 2.0 if transaction outside audit period else 0.0
    
    total_risk_score = (duplicate_contribution + backdated_contribution + 
                       user_contribution + unusual_days_contribution + 
                       closing_entries_contribution + fiscal_year_mismatch + 
                       audit_period_violation)
    
    return min(total_risk_score, 100.0)
```

#### **Weighting Strategy & Point Allocation**

| Analysis Type | Max Points | Scaling Factor | Rationale |
|---------------|------------|----------------|-----------|
| **Duplicate Analysis** | 25 points | ÷4.0 | Highest weight - critical nature of duplicates |
| **Backdated Analysis** | 20 points | ÷5.0 | High weight - significant audit concern |
| **User Analysis** | 20 points | ÷5.0 | High weight - user behavior anomalies |
| **Unusual Days Analysis** | 15 points | ÷6.67 | Moderate weight - specialized analysis |
| **Closing Entries Analysis** | 15 points | ÷6.67 | Moderate weight - specialized analysis |
| **Fiscal Year Validation** | 5 points | Direct | Lower weight but critical for data integrity |

**Total Maximum Points: 100**

### **PHASE 3: RISK LEVEL CLASSIFICATION**

#### **Final Risk Level Assignment**

```python
def get_risk_level(overall_risk_score):
    if overall_risk_score >= 80:
        return 'Critical'  # Risk Level 3
    elif overall_risk_score >= 60:
        return 'High'      # Risk Level 2
    elif overall_risk_score >= 30:
        return 'Medium'    # Risk Level 1
    else:
        return 'Low'       # Risk Level 0
```

**Risk Level Thresholds:**
- **Critical Risk**: ≥80 points (Risk Level 3)
- **High Risk**: 60-79 points (Risk Level 2)
- **Medium Risk**: 30-59 points (Risk Level 1)
- **Low Risk**: 0-29 points (Risk Level 0)

### **DETAILED CALCULATION EXAMPLES**

#### **Example 1: Critical Risk Transaction (96.25 points)**

**Individual Analysis Scores:**
- Duplicate Analysis: 95.0 points (Type 6 duplicate)
- Backdated Analysis: 100.0 points (35 days difference)
- User Analysis: 95.0 points (critical user anomaly)
- Unusual Days Analysis: 100.0 points (weekend + year-end + debit)
- Closing Entries Analysis: 90.0 points (post-close entry)

**Overall Risk Calculation:**
```
Duplicate Contribution: 95.0 ÷ 4.0 = 23.75 points
Backdated Contribution: 100.0 ÷ 5.0 = 20.0 points
User Contribution: 95.0 ÷ 5.0 = 19.0 points
Unusual Days Contribution: 100.0 ÷ 6.67 = 15.0 points
Closing Entries Contribution: 90.0 ÷ 6.67 = 13.5 points
Fiscal Year Mismatch: 3.0 points
Audit Period Violation: 2.0 points

Total Risk Score: 23.75 + 20.0 + 19.0 + 15.0 + 13.5 + 3.0 + 2.0 = 96.25
Final Classification: Critical (96.25 ≥ 80)
```

#### **Example 2: High Risk Transaction (84.25 points)**

**Individual Analysis Scores:**
- Duplicate Analysis: 80.0 points (Type 3 duplicate)
- Backdated Analysis: 70.0 points (9 days difference)
- User Analysis: 95.0 points (critical user anomaly)
- Unusual Days Analysis: 90.0 points (weekend + year-end + debit)
- Closing Entries Analysis: 85.0 points (post-close entry)

**Overall Risk Calculation:**
```
Duplicate Contribution: 80.0 ÷ 4.0 = 20.0 points
Backdated Contribution: 70.0 ÷ 5.0 = 14.0 points
User Contribution: 95.0 ÷ 5.0 = 19.0 points
Unusual Days Contribution: 90.0 ÷ 6.67 = 13.5 points
Closing Entries Contribution: 85.0 ÷ 6.67 = 12.75 points
Fiscal Year Mismatch: 3.0 points
Audit Period Violation: 2.0 points

Total Risk Score: 20.0 + 14.0 + 19.0 + 13.5 + 12.75 + 3.0 + 2.0 = 84.25
Final Classification: High (84.25 ≥ 60)
```

#### **Example 3: Medium Risk Transaction (31.5 points)**

**Individual Analysis Scores:**
- Duplicate Analysis: 0.0 points (no duplicate)
- Backdated Analysis: 50.0 points (1 day difference)
- User Analysis: 55.0 points (moderate user anomaly)
- Unusual Days Analysis: 70.0 points (weekend + debit)
- Closing Entries Analysis: 0.0 points (not closing entry)

**Overall Risk Calculation:**
```
Duplicate Contribution: 0.0 ÷ 4.0 = 0.0 points
Backdated Contribution: 50.0 ÷ 5.0 = 10.0 points
User Contribution: 55.0 ÷ 5.0 = 11.0 points
Unusual Days Contribution: 70.0 ÷ 6.67 = 10.5 points
Closing Entries Contribution: 0.0 ÷ 6.67 = 0.0 points
Fiscal Year Mismatch: 0.0 points
Audit Period Violation: 0.0 points

Total Risk Score: 0.0 + 10.0 + 11.0 + 10.5 + 0.0 + 0.0 + 0.0 = 31.5
Final Classification: Medium (31.5 ≥ 30)
```

### **KEY INSIGHTS FROM THE RISK SCORING METHODOLOGY**

#### **1. Hierarchical Risk Structure**
- Each analysis type has its own risk scoring logic
- Risk scores are then aggregated using weighted scaling
- Final classification follows clear threshold-based rules

#### **2. Balanced Weighting System**
- Prevents any single analysis from dominating the overall score
- Duplicate analysis gets highest weight (25 points max)
- Backdated and user analysis get high weight (20 points max each)
- Unusual days and closing entries get moderate weight (15 points max each)

#### **3. Business Rule Compliance**
- All risk calculations follow specific audit requirements
- Focus on structural patterns rather than monetary amounts
- Hierarchical classification for duplicate types
- Temporal analysis for backdating and unusual days

#### **4. Scalable Framework**
- Handles varying transaction volumes efficiently
- Clear separation between individual and overall analysis
- Transparent calculation methodology
- Easy to modify or extend with new risk factors

This comprehensive risk scoring methodology provides a robust, auditable, and business-rule-compliant approach to transaction risk assessment across multiple specialized analysis dimensions.

## Risk Scoring Framework

### Core Principles
1. **Specialized Analysis Focus**: Risk is evaluated through targeted analysis types
2. **Weighted Integration**: Different analysis types contribute to overall risk based on their significance
3. **Scalable Scoring**: Risk scores range from 0-100, with higher scores indicating higher risk
4. **Business Rule Compliance**: Risk calculations follow specific audit requirements
5. **Structural Pattern Focus**: Analysis focuses on transaction patterns rather than monetary amounts

## Individual Analysis Risk Scoring

### 1. Duplicate Analysis Risk Scoring

**Purpose**: Identify duplicate transactions based on business rules with hierarchical classification.

**Detailed Risk Factors**:
- **Account Matching**: Same GL account number
- **Date Matching**: Same effective date and/or posting date
- **User Matching**: Same user posting the transaction
- **Source Matching**: Same source system or document type
- **Amount Matching**: Same transaction amount
- **Temporal Proximity**: Transactions posted within short time intervals
- **Pattern Consistency**: Repeated transaction patterns

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

### 2. Backdated Analysis Risk Scoring

**Purpose**: Identify transactions where posting date is after document date.

**Detailed Risk Factors**:
- **Days Difference**: Gap between document date and posting date
- **Month-End Backdating**: Backdated entries near month-end
- **Quarter-End Backdating**: Backdated entries near quarter-end
- **Year-End Backdating**: Backdated entries near year-end
- **User Backdating Patterns**: Users frequently posting backdated entries
- **Account Backdating Patterns**: Specific accounts with backdated entries
- **Temporal Clustering**: Multiple backdated entries in short periods

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

### 3. User Analysis Risk Scoring

**Purpose**: Identify user behavior anomalies and unusual user activity patterns.

**Detailed Risk Factors**:
- **Transaction Volume**: Users with unusually high transaction counts
- **Account Usage Patterns**: Users posting to unusual or restricted accounts
- **Temporal Anomalies**: Users posting at unusual times (weekends, holidays, off-hours)
- **Pattern Deviations**: Users deviating from normal posting patterns
- **User Role Violations**: Users posting outside their typical role
- **Geographic Anomalies**: Users posting from unusual locations
- **Session Patterns**: Unusual login/logout patterns
- **Batch Processing**: Users posting large batches of transactions

**Risk Score Calculation**:
```python
# Base risk score from user behavior analysis
base_risk = user_anomaly_score

# Additional risk factors
if user_unusual_accounts > threshold:
    base_risk += 15.0
if user_temporal_anomalies:
    base_risk += 10.0
if user_pattern_deviation > threshold:
    base_risk += 15.0

# Cap at 100
final_risk = min(base_risk, 100.0)
```

**Risk Levels**:
- **Low Risk (0-30)**: Normal user activity
- **Medium Risk (31-60)**: Some unusual user patterns
- **High Risk (61-85)**: Multiple user anomalies
- **Critical Risk (86-100)**: Critical user behavior issues

### 4. Unusual Days Analysis Risk Scoring

**Purpose**: Identify transactions posted on unusual days, including weekends (Friday/Saturday).

**Detailed Risk Factors**:
- **Weekend Postings**: Transactions posted on Friday/Saturday
- **Holiday Postings**: Transactions posted on official holidays
- **Unusual Day Patterns**: Transactions on statistically unusual days
- **User Weekend Activity**: Users posting frequently on weekends
- **Temporal Anomalies**: Transactions at unusual times
- **Month-End Weekend**: Weekend postings near month-end
- **Year-End Weekend**: Weekend postings near year-end
- **Pattern Clustering**: Multiple weekend postings by same user

**Risk Score Calculation**:
```python
# Base risk for weekend posting
base_risk = 50.0

# Additional risk factors
if transaction.transaction_type == 'DEBIT':
    base_risk += 10.0  # Debit transactions on weekend
if transaction.posting_date.day >= 25:
    base_risk += 10.0  # Month-end weekend posting
if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
    base_risk += 10.0  # Year-end weekend posting

# Cap at 100
final_risk = min(base_risk, 100.0)
```

**Risk Levels**:
- **Low Risk (50-69)**: Basic weekend posting
- **Medium Risk (70-84)**: Weekend posting with additional factors
- **High Risk (85-99)**: Weekend posting with multiple risk factors
- **Critical Risk (100)**: Critical weekend posting patterns

### 5. Closing Entries Analysis Risk Scoring

**Purpose**: Identify journal entries posted during month closing periods with configurable windows.

**Detailed Risk Factors**:
- **Post-Close Entries**: Entries posted after month-end
- **Closing Window Violations**: Entries outside standard closing windows
- **Unusual Closing Patterns**: Unusual patterns during closing periods
- **User Closing Activity**: Users posting frequently during closing
- **Temporal Anomalies**: Transactions at unusual times during closing
- **Account Closing Patterns**: Specific accounts with closing entries
- **Batch Closing**: Large batches of closing entries
- **Year-End Closing**: Entries during year-end closing periods

**Risk Score Calculation**:
```python
# Base risk for closing entry
base_risk = 30.0

# Additional risk factors
if is_post_close_entry(transaction.posting_date):
    base_risk += 25.0  # Post-close entry
if transaction.transaction_type == 'DEBIT':
    base_risk += 10.0  # Debit closing entry
if transaction.posting_date.day >= 25:
    base_risk += 5.0   # Month-end closing
if transaction.posting_date.month == 12 and transaction.posting_date.day >= 25:
    base_risk += 10.0  # Year-end closing

# Cap at 100
final_risk = min(base_risk, 100.0)
```

**Risk Levels**:
- **Low Risk (30-49)**: Basic closing entry
- **Medium Risk (50-69)**: Closing entry with additional factors
- **High Risk (70-89)**: Post-close entry or closing with multiple factors
- **Critical Risk (90-100)**: Critical closing entry patterns

## Actual Data Examples

### Critical Risk Transactions

#### Example 1: Critical Duplicate + Backdated + Weekend
```json
{
  "transaction_id": "TXN-001",
  "gl_account": "5000",
  "user_name": "john.doe",
  "document_date": "2024-01-15",
  "posting_date": "2024-12-28",  // Weekend + 35 days backdated
  "transaction_type": "DEBIT",
  "source": "SAP_FI",
  "fiscal_year": 2024,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "type_6",
      "risk_score": 95.0,
      "duplicate_transaction": "TXN-002"
    },
    "backdated_analysis": {
      "days_difference": 35,
      "risk_score": 100.0
    },
    "user_analysis": {
      "user_anomaly_score": 95.0,
      "unusual_patterns": ["weekend_posting", "backdated_entry", "high_volume"]
    },
    "unusual_days_analysis": {
      "weekend_posting": true,
      "risk_score": 100.0,
      "additional_factors": ["debit_transaction", "year_end_posting"]
    },
    "closing_entries_analysis": {
      "is_closing_entry": true,
      "is_post_close": true,
      "risk_score": 90.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 23.75,  // 95.0 / 4.0
    "backdated_contribution": 20.0,   // 100.0 / 5.0
    "user_contribution": 19.0,        // 95.0 / 5.0
    "unusual_days_contribution": 15.0, // 100.0 / 6.67
    "closing_entries_contribution": 13.5, // 90.0 / 6.67
    "fiscal_year_mismatch": 3.0,
    "audit_period_violation": 2.0,
    "total_risk_score": 96.25
  },
  
  "final_classification": "Critical"
}
```

#### Example 2: Critical User Anomaly + Multiple Risk Factors
```json
{
  "transaction_id": "TXN-003",
  "gl_account": "9999",  // Unusual account
  "user_name": "jane.smith",
  "document_date": "2024-12-20",
  "posting_date": "2024-12-29",  // Weekend + 9 days backdated
  "transaction_type": "DEBIT",
  "source": "MANUAL_ENTRY",
  "fiscal_year": 2024,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "type_3",
      "risk_score": 80.0,
      "duplicate_transaction": "TXN-004"
    },
    "backdated_analysis": {
      "days_difference": 9,
      "risk_score": 70.0
    },
    "user_analysis": {
      "user_anomaly_score": 95.0,
      "unusual_patterns": ["unusual_account", "weekend_posting", "manual_entry", "high_volume"]
    },
    "unusual_days_analysis": {
      "weekend_posting": true,
      "risk_score": 90.0,
      "additional_factors": ["debit_transaction", "year_end_posting"]
    },
    "closing_entries_analysis": {
      "is_closing_entry": true,
      "is_post_close": true,
      "risk_score": 85.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 20.0,   // 80.0 / 4.0
    "backdated_contribution": 14.0,   // 70.0 / 5.0
    "user_contribution": 19.0,        // 95.0 / 5.0
    "unusual_days_contribution": 13.5, // 90.0 / 6.67
    "closing_entries_contribution": 12.75, // 85.0 / 6.67
    "fiscal_year_mismatch": 3.0,
    "audit_period_violation": 2.0,
    "total_risk_score": 84.25
  },
  
  "final_classification": "High"
}
```

### High Risk Transactions

#### Example 1: High Backdated + User Anomaly
```json
{
  "transaction_id": "TXN-005",
  "gl_account": "4000",
  "user_name": "mike.wilson",
  "document_date": "2024-11-10",
  "posting_date": "2024-11-25",  // 15 days backdated
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 15,
      "risk_score": 85.0
    },
    "user_analysis": {
      "user_anomaly_score": 75.0,
      "unusual_patterns": ["backdated_entry", "unusual_timing"]
    },
    "unusual_days_analysis": {
      "weekend_posting": false,
      "risk_score": 0.0
    },
    "closing_entries_analysis": {
      "is_closing_entry": false,
      "risk_score": 0.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 17.0,   // 85.0 / 5.0
    "user_contribution": 15.0,        // 75.0 / 5.0
    "unusual_days_contribution": 0.0,
    "closing_entries_contribution": 0.0,
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 32.0
  },
  
  "final_classification": "Medium"
}
```

#### Example 2: High Weekend + Closing Entry
```json
{
  "transaction_id": "TXN-006",
  "gl_account": "6000",
  "user_name": "sarah.jones",
  "document_date": "2024-12-28",
  "posting_date": "2024-12-28",  // Weekend
  "transaction_type": "DEBIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 0,
      "risk_score": 0.0
    },
    "user_analysis": {
      "user_anomaly_score": 60.0,
      "unusual_patterns": ["weekend_posting"]
    },
    "unusual_days_analysis": {
      "weekend_posting": true,
      "risk_score": 85.0,
      "additional_factors": ["debit_transaction", "year_end_posting"]
    },
    "closing_entries_analysis": {
      "is_closing_entry": true,
      "is_post_close": false,
      "risk_score": 75.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 0.0,
    "user_contribution": 12.0,        // 60.0 / 5.0
    "unusual_days_contribution": 12.75, // 85.0 / 6.67
    "closing_entries_contribution": 11.25, // 75.0 / 6.67
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 36.0
  },
  
  "final_classification": "Medium"
}
```

### Medium Risk Transactions

#### Example 1: Medium Backdated + Basic Factors
```json
{
  "transaction_id": "TXN-007",
  "gl_account": "3000",
  "user_name": "david.brown",
  "document_date": "2024-12-15",
  "posting_date": "2024-12-22",  // 7 days backdated
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 7,
      "risk_score": 70.0
    },
    "user_analysis": {
      "user_anomaly_score": 40.0,
      "unusual_patterns": ["backdated_entry"]
    },
    "unusual_days_analysis": {
      "weekend_posting": false,
      "risk_score": 0.0
    },
    "closing_entries_analysis": {
      "is_closing_entry": false,
      "risk_score": 0.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 14.0,   // 70.0 / 5.0
    "user_contribution": 8.0,         // 40.0 / 5.0
    "unusual_days_contribution": 0.0,
    "closing_entries_contribution": 0.0,
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 22.0
  },
  
  "final_classification": "Low"
}
```

#### Example 2: Medium User Anomaly + Weekend
```json
{
  "transaction_id": "TXN-008",
  "gl_account": "2000",
  "user_name": "lisa.garcia",
  "document_date": "2024-12-27",
  "posting_date": "2024-12-28",  // Weekend
  "transaction_type": "DEBIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 1,
      "risk_score": 50.0
    },
    "user_analysis": {
      "user_anomaly_score": 55.0,
      "unusual_patterns": ["weekend_posting", "unusual_timing"]
    },
    "unusual_days_analysis": {
      "weekend_posting": true,
      "risk_score": 70.0,
      "additional_factors": ["debit_transaction"]
    },
    "closing_entries_analysis": {
      "is_closing_entry": false,
      "risk_score": 0.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 10.0,   // 50.0 / 5.0
    "user_contribution": 11.0,        // 55.0 / 5.0
    "unusual_days_contribution": 10.5, // 70.0 / 6.67
    "closing_entries_contribution": 0.0,
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 31.5
  },
  
  "final_classification": "Medium"
}
```

### Low Risk Transactions

#### Example 1: Normal Transaction
```json
{
  "transaction_id": "TXN-009",
  "gl_account": "1000",
  "user_name": "tom.anderson",
  "document_date": "2024-12-20",
  "posting_date": "2024-12-20",  // Same day
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 0,
      "risk_score": 0.0
    },
    "user_analysis": {
      "user_anomaly_score": 10.0,
      "unusual_patterns": []
    },
    "unusual_days_analysis": {
      "weekend_posting": false,
      "risk_score": 0.0
    },
    "closing_entries_analysis": {
      "is_closing_entry": false,
      "risk_score": 0.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 0.0,
    "user_contribution": 2.0,         // 10.0 / 5.0
    "unusual_days_contribution": 0.0,
    "closing_entries_contribution": 0.0,
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 2.0
  },
  
  "final_classification": "Low"
}
```

#### Example 2: Basic Closing Entry
```json
{
  "transaction_id": "TXN-010",
  "gl_account": "5000",
  "user_name": "emma.wilson",
  "document_date": "2024-12-31",
  "posting_date": "2024-12-31",  // Month-end
  "transaction_type": "CREDIT",
  "source": "SAP_FI",
  "fiscal_year": 2025,
  "file_fiscal_year": 2025,
  "audit_start_date": "2025-01-01",
  "audit_end_date": "2025-12-31",
  
  "analysis_results": {
    "duplicate_analysis": {
      "duplicate_type": "none",
      "risk_score": 0.0
    },
    "backdated_analysis": {
      "days_difference": 0,
      "risk_score": 0.0
    },
    "user_analysis": {
      "user_anomaly_score": 20.0,
      "unusual_patterns": ["closing_entry"]
    },
    "unusual_days_analysis": {
      "weekend_posting": false,
      "risk_score": 0.0
    },
    "closing_entries_analysis": {
      "is_closing_entry": true,
      "is_post_close": false,
      "risk_score": 50.0
    }
  },
  
  "overall_risk_calculation": {
    "duplicate_contribution": 0.0,
    "backdated_contribution": 0.0,
    "user_contribution": 4.0,         // 20.0 / 5.0
    "unusual_days_contribution": 0.0,
    "closing_entries_contribution": 7.5, // 50.0 / 6.67
    "fiscal_year_mismatch": 0.0,
    "audit_period_violation": 0.0,
    "total_risk_score": 11.5
  },
  
  "final_classification": "Low"
}
```

## Overall Analysis Risk Integration

### Risk Score Aggregation

The overall analysis combines risk scores from all specialized analyses using a weighted approach:

```python
def calculate_overall_risk_score(transaction, duplicate_results, backdated_results, user_results, unusual_days_results, closing_entries_results):
    risk_score = 0.0
    
    # Risk from duplicate analysis (max 25 points)
    duplicate_found = any(d.get('transaction1', {}).get('id') == str(transaction.id) for d in duplicate_results)
    if duplicate_found:
        duplicate_entry = next((d for d in duplicate_results if d.get('transaction1', {}).get('id') == str(transaction.id)), None)
        if duplicate_entry:
            risk_score += min(duplicate_entry.get('risk_score', 25.0) / 4.0, 25.0)
    
    # Risk from backdated analysis (max 20 points)
    backdated_found = any(b.get('transaction_id') == str(transaction.id) for b in backdated_results)
    if backdated_found:
        backdated_entry = next((b for b in backdated_results if b.get('transaction_id') == str(transaction.id)), None)
        if backdated_entry:
            risk_score += min(backdated_entry.get('risk_score', 25.0) / 5.0, 20.0)
    
    # Risk from user analysis (max 20 points)
    user_found = any(u.get('transaction_id') == str(transaction.id) for u in user_results)
    if user_found:
        user_entry = next((u for u in user_results if u.get('transaction_id') == str(transaction.id)), None)
        if user_entry:
            risk_score += min(user_entry.get('risk_score', 25.0) / 5.0, 20.0)
    
    # Risk from unusual days analysis (max 15 points)
    unusual_days_found = any(u.get('transaction_id') == str(transaction.id) for u in unusual_days_results)
    if unusual_days_found:
        unusual_days_entry = next((u for u in unusual_days_results if u.get('transaction_id') == str(transaction.id)), None)
        if unusual_days_entry:
            risk_score += min(unusual_days_entry.get('risk_score', 25.0) / 6.67, 15.0)
    
    # Risk from closing entries analysis (max 15 points)
    closing_entries_found = any(c.get('transaction_id') == str(transaction.id) for c in closing_entries_results)
    if closing_entries_found:
        closing_entries_entry = next((c for c in closing_entries_results if c.get('transaction_id') == str(transaction.id)), None)
        if closing_entries_entry:
            risk_score += min(closing_entries_entry.get('risk_score', 25.0) / 6.67, 15.0)
    
    # Additional risk factors (max 5 points)
    if transaction.data_file and transaction.data_file.fiscal_year:
        # Check if transaction fiscal year matches file fiscal year
        if transaction.fiscal_year != transaction.data_file.fiscal_year:
            risk_score += 3.0  # Fiscal year mismatch
        
        # Check if transaction date is within audit period
        if transaction.data_file.audit_start_date and transaction.data_file.audit_end_date:
            if transaction.posting_date and (transaction.posting_date < transaction.data_file.audit_start_date or transaction.posting_date > transaction.data_file.audit_end_date):
                risk_score += 2.0  # Transaction outside audit period
    
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

### 1. Specialized Analysis Phase
```
Transaction Data
    ↓
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│Duplicate Analysis│Backdated Analysis│  User Analysis  │Unusual Days     │Closing Entries  │
│ Risk Score: 70-95│ Risk Score: 50-100│ Risk Score: 0-100│ Risk Score: 50-100│ Risk Score: 30-100│
└─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
    ↓
Specialized Risk Scores Generated
```

### 2. Overall Analysis Phase
```
Specialized Risk Scores
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
1. **Duplicate Analysis**: Scaled down by 4x (max 25 points)
2. **Backdated Analysis**: Scaled down by 5x (max 20 points)
3. **User Analysis**: Scaled down by 5x (max 20 points)
4. **Unusual Days Analysis**: Scaled down by 6.67x (max 15 points)
5. **Closing Entries Analysis**: Scaled down by 6.67x (max 15 points)
6. **Fiscal Year Validation**: 5 points max
   - Fiscal year mismatch: 3 points
   - Transaction outside audit period: 2 points

### Scaling Rationale
- **Duplicate Risk**: Highest weight due to critical nature of duplicate transactions
- **Backdated & User Risk**: High weight for significant audit concerns
- **Unusual Days & Closing Entries Risk**: Moderate weight for specialized analysis
- **Fiscal Year Validation**: Lower weight but critical for data integrity

## Risk Score Examples

### Example 1: High-Risk Transaction
```python
# Transaction with multiple risk factors
transaction = {
    'fiscal_year': 2024,
    'file_fiscal_year': 2025,
    'posting_date': '2024-12-15',
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
duplicate_risk = 95.0    # Type 6 duplicate
backdated_risk = 85.0    # 20 days difference
user_risk = 80.0         # User anomaly
unusual_days_risk = 70.0 # Weekend posting
closing_entries_risk = 60.0 # Closing entry

# Overall Risk Calculation
overall_risk = (95.0/4.0) + (85.0/5.0) + (80.0/5.0) + (70.0/6.67) + (60.0/6.67) + 3.0 + 2.0
overall_risk = 23.75 + 17.0 + 16.0 + 10.5 + 9.0 + 3.0 + 2.0 = 81.25
overall_risk = min(81.25, 100.0) = 81.25

# Final Classification
risk_level = 'Critical'  # 81.25 >= 80
```

### Example 2: Medium-Risk Transaction
```python
# Transaction with moderate risk factors
transaction = {
    'fiscal_year': 2025,
    'file_fiscal_year': 2025,
    'posting_date': '2025-06-15',
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
duplicate_risk = 0.0     # No duplicate
backdated_risk = 70.0    # 10 days difference
user_risk = 0.0          # No user anomaly
unusual_days_risk = 0.0  # No unusual days
closing_entries_risk = 50.0 # Basic closing entry

# Overall Risk Calculation
overall_risk = 0.0 + (70.0/5.0) + 0.0 + 0.0 + (50.0/6.67) + 0.0 + 0.0
overall_risk = 0.0 + 14.0 + 0.0 + 0.0 + 7.5 + 0.0 + 0.0 = 21.5

# Final Classification
risk_level = 'Low'  # 21.5 < 30
```

### Example 3: Critical-Risk Transaction
```python
# Transaction with critical risk factors
transaction = {
    'fiscal_year': 2024,
    'file_fiscal_year': 2025,
    'posting_date': '2024-12-28',  # Weekend
    'audit_start_date': '2025-01-01',
    'audit_end_date': '2025-12-31'
}

# Individual Analysis Results
duplicate_risk = 95.0    # Type 6 duplicate
backdated_risk = 100.0   # 35 days difference
user_risk = 95.0         # Critical user anomaly
unusual_days_risk = 100.0 # Weekend posting
closing_entries_risk = 90.0 # Post-close entry

# Overall Risk Calculation
overall_risk = (95.0/4.0) + (100.0/5.0) + (95.0/5.0) + (100.0/6.67) + (90.0/6.67) + 3.0 + 2.0
overall_risk = 23.75 + 20.0 + 19.0 + 15.0 + 13.5 + 3.0 + 2.0 = 96.25
overall_risk = min(96.25, 100.0) = 96.25

# Final Classification
risk_level = 'Critical'  # 96.25 >= 80
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
    "duplicate_analysis": {
        "duplicate_types": {"type_1": 5, "type_6": 2},
        "risk_scores": [70, 95, 85],
        "total_duplicates": 7
    },
    "backdated_analysis": {
        "risk_levels": {"critical": 1, "high": 3, "medium": 2, "low": 1},
        "average_days_difference": 15.2
    },
    "user_analysis": {
        "user_anomalies": [80, 95, 60],
        "high_risk_users": 3,
        "average_user_risk": 78.3
    },
    "unusual_days_analysis": {
        "weekend_postings": [70, 100, 85],
        "weekend_transactions": 15,
        "average_weekend_risk": 85.0
    },
    "closing_entries_analysis": {
        "closing_entries": [60, 90, 75],
        "post_close_entries": 8,
        "average_closing_risk": 75.0
    }
}
```

### Overall Risk Report
```json
{
    "overall_risk_assessment": {
        "total_transactions": 1000,
        "risk_distribution": {
            "low": 500,
            "medium": 250,
            "high": 180,
            "critical": 70
        },
        "average_risk_score": 42.5,
        "analysis_contributions": {
            "duplicate_analysis": 18.2,
            "backdated_analysis": 12.8,
            "user_analysis": 11.3,
            "unusual_days_analysis": 8.2,
            "closing_entries_analysis": 7.8,
            "fiscal_year_validation": 2.7
        }
    }
}
```

## Benefits of This Risk Scoring Approach

1. **Specialized Focus**: Targeted analysis on specific risk areas
2. **Business Rule Compliance**: Follows specific audit requirements
3. **Scalable Framework**: Handles varying transaction volumes
4. **Transparent Logic**: Clear calculation methodology
5. **Flexible Integration**: Easy to modify or extend
6. **Audit Trail**: Complete documentation of risk calculations
7. **Performance Optimized**: Efficient calculation algorithms
8. **Balanced Weighting**: Prevents any single analysis from dominating
9. **Structural Pattern Focus**: Analysis based on transaction patterns rather than monetary amounts

## Future Enhancements

1. **Dynamic Weighting**: Adjust weights based on historical patterns
2. **Machine Learning Integration**: Use ML for risk score optimization
3. **Real-time Scoring**: Implement real-time risk assessment
4. **Custom Thresholds**: Allow user-defined risk thresholds
5. **Advanced Analytics**: Add predictive risk modeling
6. **Analysis-Specific Tuning**: Fine-tune individual analysis contributions
7. **Industry-Specific Rules**: Add industry-specific risk factors
8. **Pattern-Based Analysis**: Enhance structural pattern recognition 