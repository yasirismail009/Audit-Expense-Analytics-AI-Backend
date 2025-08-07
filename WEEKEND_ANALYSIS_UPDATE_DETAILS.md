# Weekend Analysis Update - Comprehensive Details

## **OVERVIEW OF CHANGES MADE**

### **1. Weekend Definition Update**
**Before:** Saturday/Sunday (weekday 5,6)
**After:** Friday/Saturday (weekday 4,5)

### **2. Files Updated**
- ✅ `RISK_SCORING_METHODOLOGY_AND_INTEGRATION.md` - Documentation
- ✅ `core/analytics.py` - Configuration
- ✅ `core/serializers.py` - Day type detection
- ✅ `core/sync_analysis.py` - Weekend detection logic
- ✅ `core/views.py` - Risk score calculations
- ✅ `core/tasks.py` - Already correctly implemented

---

## **DETAILED ANALYSIS OF UNUSUAL ACCOUNTS HANDLING**

### **1. USER ANALYSIS - Unusual Account Detection**

#### **Rule 4: Unusual Account Usage**
```python
# Rule 4: Unusual account usage
unique_accounts = len(set(txn.gl_account for txn in user_txns))
if unique_accounts > 20:  # More than 20 different accounts
    anomaly_score += 15
    anomaly_factors.append('unusual_accounts')
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
| **Weekend Posting** | >5 weekend posts | +15 | Weekend activity |
| **Unusual Accounts** | >20 unique accounts | +15 | **Account diversity** |
| **High Frequency** | >50 transactions | +10 | High posting frequency |
| **Debit Heavy** | >80% debits | +10 | Debit-heavy transactions |
| **Manual Entries** | >10 manual entries | +15 | Manual posting activity |

---

### **2. UNUSUAL DAYS ANALYSIS - Account Grouping**

#### **Account-Based Weekend Analysis**
```python
# Group by account
account = transaction.gl_account
if account not in unusual_days_by_account:
    unusual_days_by_account[account] = []
unusual_days_by_account[account].append(unusual_record)
```

**What This Tracks:**
- Which accounts have weekend postings
- Frequency of weekend activity per account
- Account-specific weekend patterns
- High-risk accounts with weekend activity

#### **Account Risk Assessment**
```python
# Account recommendations
for account, postings in unusual_days_by_account.items():
    if len(postings) > 3:  # Accounts with many weekend postings
        recommendations['account_recommendations'].append(
            f'Review account {account} - {len(postings)} weekend postings'
        )
```

**Account Risk Thresholds:**
- **High Risk:** >3 weekend postings per account
- **Medium Risk:** 2-3 weekend postings per account
- **Low Risk:** 1 weekend posting per account

---

### **3. INTEGRATED RISK SCORING - Account Impact**

#### **Transaction-Level Risk Scoring**
```python
# Check for unusual days (medium risk)
if unusual_days_analysis and unusual_days_analysis.weekend_postings:
    for unusual in unusual_days_analysis.weekend_postings:
        if str(transaction.id) == unusual.get('transaction_id'):
            risk_score += 40  # Weekend posting risk
            anomaly_types.append('unusual_days')
            break
```

**Account-Specific Risk Factors:**
1. **Weekend Posting:** +40 points (Medium risk)
2. **Account Diversity:** +15 points (User analysis)
3. **High-Value Weekend:** Additional risk based on amount

#### **Account Risk Distribution**
```python
# Group by account for analysis
unusual_days_by_account = {}
for transaction in transactions:
    if transaction.posting_date.weekday() in [4, 5]:  # Friday/Saturday
        account = transaction.gl_account
        if account not in unusual_days_by_account:
            unusual_days_by_account[account] = []
        unusual_days_by_account[account].append(transaction)
```

---

### **4. TRAINING MODELS - Account Pattern Learning**

#### **User Model Training**
```python
def _train_user_rules(self, transactions):
    # Calculate user statistics
    for user, user_txns in user_transactions.items():
        unique_accounts = len(set(t.gl_account for t in user_txns))
        weekend_count = len([t for t in user_txns if t.posting_date.weekday() in [4, 5]])
        
        user_stats[user] = {
            'unique_accounts': unique_accounts,
            'weekend_count': weekend_count,
            # ... other stats
        }
    
    # Calculate optimal thresholds
    unique_accounts = [stats['unique_accounts'] for stats in user_stats.values()]
    optimal_thresholds = {
        'unusual_accounts_threshold': self._percentile(unique_accounts, 90),
        'weekend_activity_threshold': 5
    }
```

**Training Insights:**
- **90th Percentile:** Determines unusual account threshold
- **Historical Patterns:** Learns normal account usage per user
- **Weekend Patterns:** Tracks weekend activity by account

---

### **5. AUDIT RECOMMENDATIONS - Account Focus**

#### **Account-Specific Recommendations**
```python
def _generate_unusual_days_audit_recommendations(self, weekend_postings, unusual_days_by_user, unusual_days_by_account):
    # Account recommendations
    for account, postings in unusual_days_by_account.items():
        if len(postings) > 3:  # Accounts with many weekend postings
            recommendations['account_recommendations'].append(
                f'Review account {account} - {len(postings)} weekend postings'
            )
```

**Account Audit Focus:**
1. **High-Frequency Accounts:** Accounts with >3 weekend postings
2. **Unusual Account Usage:** Users posting to >20 different accounts
3. **Account Authorization:** Verify account posting permissions
4. **Segregation of Duties:** Check for proper account access controls

---

### **6. CHART DATA - Account Visualization**

#### **Account-Based Charts**
```python
'weekend_by_account': _generate_weekend_by_account_chart(unusual_days_by_account),
'unusual_days_by_account': {account: len(transactions) for account, transactions in unusual_days_by_account.items()}
```

**Visualization Features:**
- Weekend activity by account
- Account risk distribution
- Account-specific weekend patterns
- High-risk account identification

---

## **COMPREHENSIVE RISK SCORING INTEGRATION**

### **Updated Risk Score Calculation**
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

### **Account Risk Weighting**
| Analysis Type | Risk Points | Account Impact | Description |
|--------------|-------------|----------------|-------------|
| **Duplicate** | 80 points | High | Account-specific duplicates |
| **Backdated** | 70 points | High | Account backdating patterns |
| **Holiday** | 60 points | Medium | Holiday account activity |
| **User Anomaly** | 50 points | **High** | **Unusual account usage** |
| **Unusual Days** | 40 points | Medium | **Weekend account activity** |
| **Closing Entries** | 30 points | Medium | Account closing patterns |

---

## **BUSINESS IMPLICATIONS**

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

## **TECHNICAL IMPLEMENTATION SUMMARY**

### **Updated Weekend Detection**
```python
# OLD: Saturday/Sunday
if day_of_week in [5, 6]:  # Saturday, Sunday

# NEW: Friday/Saturday  
if day_of_week in [4, 5]:  # Friday, Saturday
```

### **Account Analysis Integration**
1. **User Analysis:** Detects unusual account usage (>20 accounts)
2. **Unusual Days:** Groups weekend activity by account
3. **Risk Scoring:** Integrates account-based risk factors
4. **Audit Recommendations:** Account-specific recommendations
5. **Training Models:** Learns account usage patterns

### **Risk Score Impact**
- **Weekend Postings:** +40 points (Friday/Saturday)
- **Unusual Accounts:** +15 points (User analysis)
- **Combined Risk:** Up to 55 points for weekend + unusual accounts
- **Critical Threshold:** 80+ points for critical risk level

This comprehensive update ensures that unusual account usage is properly detected and integrated into the overall risk assessment framework, with specific focus on weekend activity patterns and account diversity analysis.
