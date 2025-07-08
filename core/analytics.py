import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
import xgboost as xgb
from datetime import datetime, timedelta
import warnings
import joblib
import os
from decimal import Decimal
from django.utils import timezone
from .models import Expense, ExpenseSheet, SheetAnalysis, ExpenseAnalysis
import json

class ExpenseSheetAnalyzer:
    """Analyzes expense sheets for fraud detection and trains models"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.models = {
            'isolation_forest': IsolationForest(contamination='auto', random_state=42),
            'random_forest': RandomForestClassifier(n_estimators=100, random_state=42)
        }
        self.model_path = 'trained_models/'
        os.makedirs(self.model_path, exist_ok=True)
        
        # Training configuration
        self.training_config = {
            'auto_train_threshold': 10,  # New sheets before auto-training
            'min_training_data': 50,     # Minimum expenses for training
            'retrain_interval_hours': 24, # Hours between retrains
            'performance_threshold': 0.1  # Anomaly rate threshold for retraining
        }
        
        # Try to load existing models
        self.load_models()
    
    def should_retrain(self):
        """Check if models should be retrained based on new data"""
        # Check if new sheets exist since last training
        last_training = getattr(self, '_last_training_time', None)
        if last_training is None:
            return True
        
        # Retrain if more than threshold new sheets added
        new_sheets_count = ExpenseSheet.objects.filter(
            uploaded_at__gt=last_training
        ).count()
        
        return new_sheets_count >= self.training_config['auto_train_threshold']
    
    def auto_train_if_needed(self):
        """Automatically train models if conditions are met"""
        if self.should_retrain():
            print("Auto-training models due to new data...")
            success = self.train_models()
            if success:
                self._last_training_time = timezone.now()
            return success
        return False
    
    def evaluate_model_performance(self):
        """Evaluate model performance and trigger retraining if needed"""
        # Calculate prediction accuracy on recent data
        recent_sheets = ExpenseSheet.objects.filter(
            uploaded_at__gte=timezone.now() - timedelta(days=30)
        )
        
        if len(recent_sheets) < 10:
            return False
        
        # Simple performance check - can be enhanced
        total_anomalies = 0
        total_expenses = 0
        
        for sheet in recent_sheets:
            df = self.prepare_sheet_data(sheet)
            if df is not None:
                total_expenses += len(df)
                # Count statistical anomalies as baseline
                amount_mean = df['amount'].mean()
                amount_std = df['amount'].std()
                anomalies = sum(abs(amount - amount_mean) > 2 * amount_std for amount in df['amount'])
                total_anomalies += anomalies
        
        anomaly_rate = total_anomalies / total_expenses if total_expenses > 0 else 0
        
        # Retrain if anomaly rate is too high or too low (indicating poor model fit)
        return anomaly_rate < 0.05 or anomaly_rate > 0.3

    def ensure_models_ready(self):
        """Ensure models are ready for prediction"""
        # Check if models are fitted
        models_ready = True
        
        for name, model in self.models.items():
            if name == 'isolation_forest':
                if not hasattr(model, 'estimators_'):
                    models_ready = False
                    print(f"Model {name} not fitted")
            elif name == 'random_forest':
                if not hasattr(model, 'estimators_'):
                    models_ready = False
                    print(f"Model {name} not fitted")
        
        if not models_ready:
            print("Some models not ready, will use statistical fallbacks")
        
        return models_ready
    
    def prepare_sheet_data(self, expense_sheet):
        """Convert expense sheet data to pandas DataFrame with features"""
        expenses = expense_sheet.expenses.all()
        
        if not expenses:
            return None
        
        # Convert to DataFrame
        data = []
        for expense in expenses:
            data.append({
                'date': expense.date,
                'category': expense.category,
                'subcategory': expense.subcategory,
                'description': expense.description,
                'employee': expense.employee,
                'department': expense.department,
                'amount': float(expense.amount),
                'currency': expense.currency,
                'payment_method': expense.payment_method,
                'vendor_supplier': expense.vendor_supplier,
                'receipt_number': expense.receipt_number,
                'status': expense.status,
                'approved_by': expense.approved_by,
                'notes': expense.notes or '',
            })
        
        df = pd.DataFrame(data)
        
        # Add engineered features
        df = self._add_features(df)
        
        return df
    
    def _add_features(self, df):
        """Add engineered features for fraud detection"""
        # Amount-based features
        df['amount_log'] = np.log1p(df['amount'])
        df['amount_zscore'] = (df['amount'] - df['amount'].mean()) / df['amount'].std()
        
        # Time-based features
        df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['day_of_month'] = pd.to_datetime(df['date']).dt.day
        
        # Employee frequency
        employee_counts = df['employee'].value_counts()
        df['employee_frequency'] = df['employee'].map(employee_counts)
        
        # Vendor frequency
        vendor_counts = df['vendor_supplier'].value_counts()
        df['vendor_frequency'] = df['vendor_supplier'].map(vendor_counts)
        
        # Category frequency
        category_counts = df['category'].value_counts()
        df['category_frequency'] = df['category'].map(category_counts)
        
        # Amount percentiles
        df['amount_percentile'] = df['amount'].rank(pct=True)
        
        # Intelligent duplicate detection features
        df['duplicate_description'] = df['description'].duplicated().astype(int)
        
        # Amount duplicates - only flag if same amount with same vendor or same employee
        df['duplicate_amount_same_vendor'] = df.groupby(['amount', 'vendor_supplier']).cumcount().astype(int)
        df['duplicate_amount_same_employee'] = df.groupby(['amount', 'employee']).cumcount().astype(int)
        df['duplicate_amount_same_date'] = df.groupby(['amount', 'date']).cumcount().astype(int)
        
        # Only flag as duplicate amount if it's suspicious (same vendor/employee/date)
        df['duplicate_amount'] = (
            ((df['duplicate_amount_same_vendor'] > 0) & (df['duplicate_amount_same_vendor'] <= 1)) |
            ((df['duplicate_amount_same_employee'] > 0) & (df['duplicate_amount_same_employee'] <= 1)) |
            ((df['duplicate_amount_same_date'] > 0) & (df['duplicate_amount_same_date'] <= 1))
        ).astype(int)
        
        # Vendor duplicates - only flag if same vendor with same amount or same employee
        df['duplicate_vendor_same_amount'] = df.groupby(['vendor_supplier', 'amount']).cumcount().astype(int)
        df['duplicate_vendor_same_employee'] = df.groupby(['vendor_supplier', 'employee']).cumcount().astype(int)
        
        df['duplicate_vendor'] = (
            ((df['duplicate_vendor_same_amount'] > 0) & (df['duplicate_vendor_same_amount'] <= 1)) |
            ((df['duplicate_vendor_same_employee'] > 0) & (df['duplicate_vendor_same_employee'] <= 1))
        ).astype(int)
        
        return df
    
    def encode_categorical_features(self, df, is_training=True):
        """Encode categorical features for model training"""
        categorical_columns = ['category', 'subcategory', 'employee', 'department', 
                              'currency', 'payment_method', 'vendor_supplier', 'status', 'approved_by']
        
        df_encoded = df.copy()
        
        for col in categorical_columns:
            if col in df.columns:
                if is_training:
                    le = LabelEncoder()
                    df_encoded[col + '_encoded'] = le.fit_transform(df[col].astype(str))
                    self.label_encoders[col] = le
                else:
                    if col in self.label_encoders:
                        # Handle unseen categories
                        le = self.label_encoders[col]
                        df_encoded[col + '_encoded'] = df[col].astype(str).map(
                            lambda x: le.transform([x])[0] if x in le.classes_ else -1
                        )
        
        return df_encoded
    
    def get_feature_columns(self):
        """Get list of feature columns for model training"""
        return [
            'amount_log', 'amount_zscore', 'day_of_week', 'month', 'day_of_month',
            'employee_frequency', 'vendor_frequency', 'category_frequency',
            'amount_percentile', 'duplicate_description', 'duplicate_amount', 'duplicate_vendor'
        ] + [col + '_encoded' for col in ['category', 'subcategory', 'employee', 'department', 
                                         'currency', 'payment_method', 'vendor_supplier', 'status', 'approved_by']]
    
    def analyze_sheet(self, expense_sheet):
        """Perform comprehensive analysis on an expense sheet"""
        print(f"Analyzing sheet: {expense_sheet.display_name}")
        
        # Auto-train if needed before analysis
        self.auto_train_if_needed()
        
        # Ensure models are ready
        self.ensure_models_ready()
        
        # Prepare data
        df = self.prepare_sheet_data(expense_sheet)
        if df is None or len(df) == 0:
            return None
        
        # Encode features
        df_encoded = self.encode_categorical_features(df, is_training=False)
        feature_cols = self.get_feature_columns()
        
        # Filter available features
        available_features = [col for col in feature_cols if col in df_encoded.columns]
        if not available_features:
            print("No features available for analysis")
            return None
        
        X = df_encoded[available_features].fillna(0)
        
        # Run anomaly detection
        results = self._run_anomaly_detection(X, df)
        
        # Calculate advanced metrics
        advanced_metrics = self.calculate_advanced_metrics(df, expense_sheet)
        print(f"Advanced metrics calculated: {len(advanced_metrics)} metrics")
        
        # Calculate sheet-level metrics
        sheet_metrics = self._calculate_sheet_metrics(df, results, advanced_metrics)
        
        # Create or update sheet analysis
        sheet_analysis = self._save_sheet_analysis(expense_sheet, sheet_metrics, results)
        
        # Create individual expense analyses
        self._save_expense_analyses(expense_sheet, df, results, sheet_analysis)
        
        return sheet_analysis
    
    def _run_anomaly_detection(self, X, df):
        """Run various anomaly detection algorithms"""
        results = {
            'isolation_forest_scores': [],
            'amount_anomalies': [],
            'timing_anomalies': [],
            'vendor_anomalies': [],
            'employee_anomalies': [],
            'duplicate_suspicions': []
        }
        
        # Isolation Forest
        try:
            if 'isolation_forest' in self.models:
                # Convert to numpy array and ensure no feature names
                X_no_names = X.values if hasattr(X, 'values') else np.array(X)
                
                # Check if model is fitted
                if hasattr(self.models['isolation_forest'], 'estimators_'):
                    iso_scores = self.models['isolation_forest'].score_samples(X_no_names)
                    results['isolation_forest_scores'] = iso_scores.tolist() if hasattr(iso_scores, 'tolist') else list(iso_scores)
                else:
                    # If model is not fitted, use a simple statistical approach
                    print("Isolation Forest not fitted, using statistical anomaly detection")
                    amount_mean = df['amount'].mean()
                    amount_std = df['amount'].std()
                    results['isolation_forest_scores'] = [
                        -abs(amount - amount_mean) / amount_std if amount_std > 0 else 0 
                        for amount in df['amount']
                    ]
        except Exception as e:
            print(f"Isolation Forest error: {e}")
            # Fallback to statistical approach
            amount_mean = df['amount'].mean()
            amount_std = df['amount'].std()
            results['isolation_forest_scores'] = [
                float(-abs(amount - amount_mean) / amount_std if amount_std > 0 else 0)
                for amount in df['amount']
            ]
        
        # Amount anomalies (statistical)
        amount_mean = df['amount'].mean()
        amount_std = df['amount'].std()
        results['amount_anomalies'] = [
            abs(amount - amount_mean) > 2 * amount_std for amount in df['amount']
        ]
        
        # Timing anomalies (multiple expenses on same day)
        daily_counts = df.groupby('date').size()
        results['timing_anomalies'] = [
            daily_counts.get(date, 0) > 3 for date in df['date']
        ]
        
        # Vendor anomalies (unusual vendors)
        vendor_counts = df['vendor_supplier'].value_counts()
        results['vendor_anomalies'] = [
            vendor_counts.get(vendor, 0) == 1 for vendor in df['vendor_supplier']
        ]
        
        # Employee anomalies (unusual employees)
        employee_counts = df['employee'].value_counts()
        results['employee_anomalies'] = [
            employee_counts.get(employee, 0) <= 1 for employee in df['employee']
        ]
        
        # Duplicate suspicions
        results['duplicate_suspicions'] = [
            bool(df['duplicate_description'].iloc[i] or 
                 df['duplicate_amount'].iloc[i] or 
                 df['duplicate_vendor'].iloc[i]) 
            for i in range(len(df))
        ]
        
        return results
    
    def _calculate_sheet_metrics(self, df, results, advanced_metrics):
        """Calculate overall sheet-level metrics"""
        total_expenses = len(df)
        total_amount = df['amount'].sum()
        
        # Count anomalies
        amount_anomalies = sum(results['amount_anomalies'])
        timing_anomalies = sum(results['timing_anomalies'])
        vendor_anomalies = sum(results['vendor_anomalies'])
        employee_anomalies = sum(results['employee_anomalies'])
        duplicate_suspicions = sum(results['duplicate_suspicions'])
        
        # Calculate overall fraud score
        anomaly_scores = []
        for i in range(total_expenses):
            score = 0
            if results['amount_anomalies'][i]: score += 25
            if results['timing_anomalies'][i]: score += 20
            if results['vendor_anomalies'][i]: score += 15
            if results['employee_anomalies'][i]: score += 15
            if results['duplicate_suspicions'][i]: score += 25
            anomaly_scores.append(min(score, 100))
        
        overall_fraud_score = np.mean(anomaly_scores) if anomaly_scores else 0
        
        # Determine risk level
        if overall_fraud_score >= 75:
            risk_level = 'CRITICAL'
        elif overall_fraud_score >= 50:
            risk_level = 'HIGH'
        elif overall_fraud_score >= 25:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        # Merge advanced metrics into analysis_details
        analysis_details = {
            'total_expenses': total_expenses,
            'total_amount': float(total_amount),
            'average_amount': float(df['amount'].mean()),
            'amount_std': float(df['amount'].std()),
            'unique_employees': df['employee'].nunique(),
            'unique_vendors': df['vendor_supplier'].nunique(),
            'unique_categories': df['category'].nunique(),
            'date_range': {
                'start': df['date'].min().isoformat(),
                'end': df['date'].max().isoformat()
            }
        }
        
        # Add all advanced metrics to analysis_details
        if isinstance(advanced_metrics, dict):
            analysis_details.update(advanced_metrics)
        
        # Ensure all values are JSON serializable
        def make_json_serializable(obj):
            if isinstance(obj, dict):
                return {k: make_json_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [make_json_serializable(item) for item in obj]
            elif isinstance(obj, bool):
                return obj  # bool is JSON serializable
            elif isinstance(obj, (int, float)):
                return float(obj) if isinstance(obj, float) else int(obj)
            elif isinstance(obj, str):
                return obj
            elif obj is None:
                return None
            elif hasattr(obj, 'item'):  # Handle numpy types
                return obj.item()
            else:
                return str(obj)  # Convert any other type to string
        
        analysis_details = make_json_serializable(analysis_details)
        
        print(f"Analysis details updated with {len(advanced_metrics) if isinstance(advanced_metrics, dict) else 0} advanced metrics")
        
        return {
            'overall_fraud_score': float(overall_fraud_score),
            'isolation_forest_score': float(np.mean(results['isolation_forest_scores'])) if len(results['isolation_forest_scores']) > 0 else 0,
            'xgboost_score': 0,  # Placeholder for future implementation
            'lof_score': 0,  # Placeholder for future implementation
            'random_forest_score': 0,  # Placeholder for future implementation
            'risk_level': risk_level,
            'amount_anomalies_detected': amount_anomalies,
            'timing_anomalies_detected': timing_anomalies,
            'vendor_anomalies_detected': vendor_anomalies,
            'employee_anomalies_detected': employee_anomalies,
            'duplicate_suspicions': duplicate_suspicions,
            'total_flagged_expenses': sum(1 for score in anomaly_scores if score > 25),
            'high_risk_expenses': sum(1 for score in anomaly_scores if score > 50),
            'critical_risk_expenses': sum(1 for score in anomaly_scores if score > 75),
            'analysis_details': analysis_details
        }
    
    def _save_sheet_analysis(self, expense_sheet, metrics, results):
        """Save or update sheet analysis"""
        sheet_analysis, created = SheetAnalysis.objects.get_or_create(
            expense_sheet=expense_sheet,
            defaults=metrics
        )
        
        if not created:
            # Update existing analysis
            for key, value in metrics.items():
                setattr(sheet_analysis, key, value)
            sheet_analysis.save()
        
        return sheet_analysis
    
    def _save_expense_analyses(self, expense_sheet, df, results, sheet_analysis):
        """Save individual expense analyses"""
        expenses = list(expense_sheet.expenses.all())
        
        for i, expense in enumerate(expenses):
            if i < len(df):
                # Calculate individual fraud score and collect reasons
                score = 0
                anomaly_reasons = []
                
                # Amount anomaly analysis
                if results['amount_anomalies'][i]:
                    score += 25
                    amount = df['amount'].iloc[i]
                    amount_mean = df['amount'].mean()
                    amount_std = df['amount'].std()
                    deviation = abs(amount - amount_mean) / amount_std
                    anomaly_reasons.append({
                        'type': 'amount_anomaly',
                        'severity': 'HIGH',
                        'reason': f'Amount ${amount:.2f} is {deviation:.1f} standard deviations from mean (${amount_mean:.2f})',
                        'details': {
                            'amount': float(amount),
                            'mean': float(amount_mean),
                            'std': float(amount_std),
                            'deviation': float(deviation)
                        }
                    })
                
                # Timing anomaly analysis
                if results['timing_anomalies'][i]:
                    score += 20
                    date = df['date'].iloc[i]
                    daily_count = df.groupby('date').size().get(date, 0)
                    anomaly_reasons.append({
                        'type': 'timing_anomaly',
                        'severity': 'MEDIUM',
                        'reason': f'Multiple expenses ({daily_count}) on same day ({date})',
                        'details': {
                            'date': date.isoformat(),
                            'expenses_on_date': int(daily_count),
                            'threshold': 3
                        }
                    })
                
                # Vendor anomaly analysis
                if results['vendor_anomalies'][i]:
                    score += 15
                    vendor = df['vendor_supplier'].iloc[i]
                    vendor_count = df['vendor_supplier'].value_counts().get(vendor, 0)
                    anomaly_reasons.append({
                        'type': 'vendor_anomaly',
                        'severity': 'MEDIUM',
                        'reason': f'Unusual vendor "{vendor}" (only {vendor_count} occurrence(s))',
                        'details': {
                            'vendor': vendor,
                            'occurrences': int(vendor_count),
                            'total_vendors': int(df['vendor_supplier'].nunique())
                        }
                    })
                
                # Employee anomaly analysis
                if results['employee_anomalies'][i]:
                    score += 15
                    employee = df['employee'].iloc[i]
                    employee_count = df['employee'].value_counts().get(employee, 0)
                    anomaly_reasons.append({
                        'type': 'employee_anomaly',
                        'severity': 'MEDIUM',
                        'reason': f'Unusual employee "{employee}" (only {employee_count} expense(s))',
                        'details': {
                            'employee': employee,
                            'expense_count': int(employee_count),
                            'total_employees': int(df['employee'].nunique())
                        }
                    })
                
                # Duplicate suspicion analysis
                if results['duplicate_suspicions'][i]:
                    score += 25
                    duplicate_types = []
                    duplicate_details = []
                    
                    if df['duplicate_description'].iloc[i]:
                        duplicate_types.append('description')
                        duplicate_details.append('Same description as another expense')
                    
                    if df['duplicate_amount'].iloc[i]:
                        duplicate_types.append('amount')
                        amount = df['amount'].iloc[i]
                        vendor = df['vendor_supplier'].iloc[i]
                        employee = df['employee'].iloc[i]
                        date = df['date'].iloc[i]
                        
                        # Check which specific duplicate condition was met
                        if df['duplicate_amount_same_vendor'].iloc[i] > 0:
                            duplicate_details.append(f'Same amount (${amount:.2f}) with same vendor "{vendor}"')
                        elif df['duplicate_amount_same_employee'].iloc[i] > 0:
                            duplicate_details.append(f'Same amount (${amount:.2f}) with same employee "{employee}"')
                        elif df['duplicate_amount_same_date'].iloc[i] > 0:
                            duplicate_details.append(f'Same amount (${amount:.2f}) on same date ({date})')
                    
                    if df['duplicate_vendor'].iloc[i]:
                        duplicate_types.append('vendor')
                        vendor = df['vendor_supplier'].iloc[i]
                        amount = df['amount'].iloc[i]
                        employee = df['employee'].iloc[i]
                        
                        # Check which specific vendor duplicate condition was met
                        if df['duplicate_vendor_same_amount'].iloc[i] > 0:
                            duplicate_details.append(f'Same vendor "{vendor}" with same amount (${amount:.2f})')
                        elif df['duplicate_vendor_same_employee'].iloc[i] > 0:
                            duplicate_details.append(f'Same vendor "{vendor}" with same employee "{employee}"')
                    
                    anomaly_reasons.append({
                        'type': 'duplicate_suspicion',
                        'severity': 'HIGH',
                        'reason': f'Potential duplicate detected: {", ".join(duplicate_types)}',
                        'details': {
                            'duplicate_types': duplicate_types,
                            'duplicate_reasons': duplicate_details,
                            'description': df['description'].iloc[i],
                            'amount': float(df['amount'].iloc[i]),
                            'vendor': df['vendor_supplier'].iloc[i],
                            'employee': df['employee'].iloc[i],
                            'date': df['date'].iloc[i].isoformat()
                        }
                    })
                
                fraud_score = min(score, 100)
                
                # Determine risk level
                if fraud_score >= 75:
                    risk_level = 'CRITICAL'
                elif fraud_score >= 50:
                    risk_level = 'HIGH'
                elif fraud_score >= 25:
                    risk_level = 'MEDIUM'
                else:
                    risk_level = 'LOW'
                
                # Create or update expense analysis
                expense_analysis, created = ExpenseAnalysis.objects.get_or_create(
                    expense=expense,
                    defaults={
                        'sheet_analysis': sheet_analysis,
                        'fraud_score': fraud_score,
                        'risk_level': risk_level,
                        'amount_anomaly': results['amount_anomalies'][i],
                        'timing_anomaly': results['timing_anomalies'][i],
                        'vendor_anomaly': results['vendor_anomalies'][i],
                        'employee_anomaly': results['employee_anomalies'][i],
                        'duplicate_suspicion': results['duplicate_suspicions'][i],
                        'analysis_details': {
                            'amount': float(df['amount'].iloc[i]),
                            'category': df['category'].iloc[i],
                            'employee': df['employee'].iloc[i],
                            'vendor': df['vendor_supplier'].iloc[i],
                            'date': df['date'].iloc[i].isoformat(),
                            'anomaly_reasons': anomaly_reasons,
                            'fraud_score_breakdown': {
                                'amount_anomaly': 25 if results['amount_anomalies'][i] else 0,
                                'timing_anomaly': 20 if results['timing_anomalies'][i] else 0,
                                'vendor_anomaly': 15 if results['vendor_anomalies'][i] else 0,
                                'employee_anomaly': 15 if results['employee_anomalies'][i] else 0,
                                'duplicate_suspicion': 25 if results['duplicate_suspicions'][i] else 0,
                                'total_score': fraud_score
                            }
                        }
                    }
                )
                
                if not created:
                    # Update existing analysis
                    expense_analysis.fraud_score = fraud_score
                    expense_analysis.risk_level = risk_level
                    expense_analysis.amount_anomaly = results['amount_anomalies'][i]
                    expense_analysis.timing_anomaly = results['timing_anomalies'][i]
                    expense_analysis.vendor_anomaly = results['vendor_anomalies'][i]
                    expense_analysis.employee_anomaly = results['employee_anomalies'][i]
                    expense_analysis.duplicate_suspicion = results['duplicate_suspicions'][i]
                    
                    # Update analysis details
                    analysis_details = expense_analysis.analysis_details or {}
                    analysis_details.update({
                        'amount': float(df['amount'].iloc[i]),
                        'category': df['category'].iloc[i],
                        'employee': df['employee'].iloc[i],
                        'vendor': df['vendor_supplier'].iloc[i],
                        'date': df['date'].iloc[i].isoformat(),
                        'anomaly_reasons': anomaly_reasons,
                        'fraud_score_breakdown': {
                            'amount_anomaly': 25 if results['amount_anomalies'][i] else 0,
                            'timing_anomaly': 20 if results['timing_anomalies'][i] else 0,
                            'vendor_anomaly': 15 if results['vendor_anomalies'][i] else 0,
                            'employee_anomaly': 15 if results['employee_anomalies'][i] else 0,
                            'duplicate_suspicion': 25 if results['duplicate_suspicions'][i] else 0,
                            'total_score': fraud_score
                        }
                    })
                    expense_analysis.analysis_details = analysis_details
                    expense_analysis.save()
    
    def train_models(self, sheets=None):
        """Train models on historical data"""
        print("Training fraud detection models...")
        
        if sheets is None:
            sheets = ExpenseSheet.objects.all()
        
        all_data = []
        all_labels = []
        
        for sheet in sheets:
            df = self.prepare_sheet_data(sheet)
            if df is None or len(df) < 5:  # Need minimum data
                continue
            
            # Encode features
            df_encoded = self.encode_categorical_features(df, is_training=True)
            feature_cols = self.get_feature_columns()
            available_features = [col for col in feature_cols if col in df_encoded.columns]
            
            if not available_features:
                continue
            
            X = df_encoded[available_features].fillna(0)
            
            # Create labels (simplified - you can enhance this)
            # For now, we'll use amount anomalies as a proxy for fraud
            amount_mean = df['amount'].mean()
            amount_std = df['amount'].std()
            y = [1 if abs(amount - amount_mean) > 2 * amount_std else 0 for amount in df['amount']]
            
            all_data.append(X)
            all_labels.extend(y)
        
        if not all_data:
            print("No data available for training")
            return False
        
        # Combine all data
        X_combined = pd.concat(all_data, ignore_index=True)
        y_combined = np.array(all_labels)
        
        if len(X_combined) < 10:
            print("Insufficient data for training")
            return False
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_combined, y_combined, test_size=0.2, random_state=42
        )
        
        # Train models
        for name, model in self.models.items():
            try:
                if name == 'isolation_forest':
                    model.fit(X_train)
                else:
                    model.fit(X_train, y_train)
                
                # Save model
                model_file = os.path.join(self.model_path, f'{name}_model.pkl')
                joblib.dump(model, model_file)
                print(f"Trained and saved {name} model")
                
            except Exception as e:
                print(f"Error training {name}: {e}")
        
        # Save scaler and encoders
        joblib.dump(self.scaler, os.path.join(self.model_path, 'scaler.pkl'))
        joblib.dump(self.label_encoders, os.path.join(self.model_path, 'label_encoders.pkl'))
        
        print("Model training completed")
        return True
    
    def load_models(self):
        """Load trained models from disk"""
        try:
            for name in self.models.keys():
                model_file = os.path.join(self.model_path, f'{name}_model.pkl')
                if os.path.exists(model_file):
                    self.models[name] = joblib.load(model_file)
            
            # Load scaler and encoders
            scaler_file = os.path.join(self.model_path, 'scaler.pkl')
            encoders_file = os.path.join(self.model_path, 'label_encoders.pkl')
            
            if os.path.exists(scaler_file):
                self.scaler = joblib.load(scaler_file)
            if os.path.exists(encoders_file):
                self.label_encoders = joblib.load(encoders_file)
            
            print("Models loaded successfully")
            return True
        except Exception as e:
            print(f"Error loading models: {e}")
            return False
    
    def calculate_advanced_metrics(self, df, expense_sheet):
        """Calculate advanced expense analytics metrics"""
        if df is None or len(df) == 0:
            return {}
        
        # Get actual expense objects to match with DataFrame
        expenses = list(expense_sheet.expenses.all())
        
        # Ensure date column is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Basic calculations
        total_expenses = len(df)
        total_amount = df['amount'].sum()
        date_range = (df['date'].max() - df['date'].min()).days + 1 if len(df) > 0 else 1
        
        # 1. Expense Velocity Ratio (EVR)
        evr = total_amount / date_range if date_range > 0 else 0
        
        # 2. Approval Concentration Index (ACI)
        approver_totals = df.groupby('approved_by')['amount'].sum()
        largest_approver_total = approver_totals.max() if len(approver_totals) > 0 else 0
        aci = (largest_approver_total / total_amount * 100) if total_amount > 0 else 0
        
        # 3. Payment Method Risk Score (PMRS)
        personal_card_expenses = df[df['payment_method'].str.contains('personal', case=False, na=False)]['amount'].sum()
        pmrs = (personal_card_expenses / total_amount * 100) if total_amount > 0 else 0
        
        # 4. Category Deviation Index (CDI) - by department
        dept_category_spend = df.groupby(['department', 'category'])['amount'].sum().reset_index()
        dept_avg_spend = df.groupby('department')['amount'].mean()
        
        cdi_results = []
        for _, row in dept_category_spend.iterrows():
            dept_avg = dept_avg_spend.get(row['department'], 0)
            if dept_avg > 0:
                cdi = abs(row['amount'] - dept_avg) / dept_avg
                cdi_results.append({
                    'department': row['department'],
                    'category': row['category'],
                    'spend': float(row['amount']),
                    'department_avg': float(dept_avg),
                    'cdi': float(cdi)
                })
        
        # 5. Vendor Concentration Ratio (VCR)
        vendor_totals = df.groupby('vendor_supplier')['amount'].sum().sort_values(ascending=False)
        top_5_vendors_total = vendor_totals.head(5).sum()
        vcr = (top_5_vendors_total / total_amount * 100) if total_amount > 0 else 0
        
        # 6. High-Value Expense Frequency (HVEF)
        high_value_threshold = df['amount'].quantile(0.75) if len(df) > 0 else 0
        high_value_expenses = df[df['amount'] > high_value_threshold]
        hvef = (len(high_value_expenses) / total_expenses * 100) if total_expenses > 0 else 0
        
        # 7. Department Expense Intensity (DEI)
        dept_expenses = df.groupby('department')['amount'].sum()
        dei_results = {}
        for dept, amount in dept_expenses.items():
            dei_results[dept] = float(amount)
        
        # 8. Recurring Expense Variance (REV)
        rev_results = []
        try:
            if pd.api.types.is_datetime64_any_dtype(df['date']):
                category_monthly = df.groupby(['category', df['date'].dt.to_period('M')])['amount'].sum()
                for category in df['category'].unique():
                    category_data = category_monthly.get(category, pd.Series())
                    if len(category_data) > 1:
                        variance = category_data.std() / category_data.mean() if category_data.mean() > 0 else 0
                        rev_results.append({
                            'category': category,
                            'variance': float(variance),
                            'periods': len(category_data)
                        })
        except Exception as e:
            print(f"Warning: Could not calculate recurring expense variance: {e}")
        
        # 9. Expense Complexity Score (ECS)
        ecs_scores = []
        for idx, row in df.iterrows():
            score = 0
            issues = []
            
            # Get actual expense ID if available
            expense_id = f"expense_{idx + 1}"
            if idx < len(expenses):
                expense_id = str(expenses[idx].id)
            
            # Missing receipts (assuming notes field indicates receipt status)
            if pd.isna(row['notes']) or str(row['notes']).strip() == '':
                score += 3
                issues.append('Missing receipt documentation')
            
            # Multiple approvers (check if approved_by contains multiple names)
            if pd.notna(row['approved_by']) and ',' in str(row['approved_by']):
                score += 2
                issues.append('Multiple approvers')
            
            # Vague descriptions
            if pd.isna(row['description']) or len(str(row['description'])) < 10:
                score += 1
                issues.append('Vague description')
            
            # High value expenses (above 75th percentile)
            amount = float(row['amount'])
            high_value_threshold = df['amount'].quantile(0.75)
            if amount > high_value_threshold:
                score += 2
                issues.append(f'High value expense (${amount:.2f} > ${high_value_threshold:.2f})')
            
            # Personal payment methods
            payment_method = str(row['payment_method']).lower()
            if 'personal' in payment_method:
                score += 2
                issues.append('Personal payment method used')
            
            # Unusual vendors (vendors with only 1 occurrence)
            vendor = row['vendor_supplier']
            vendor_count = df['vendor_supplier'].value_counts().get(vendor, 0)
            if vendor_count == 1:
                score += 1
                issues.append(f'Unusual vendor: {vendor}')
            
            # Weekend or holiday expenses (potential personal use)
            expense_date = pd.to_datetime(row['date'])
            if expense_date.dayofweek >= 5:  # Saturday = 5, Sunday = 6
                score += 1
                issues.append('Weekend expense')
            
            # Multiple expenses on same day by same employee
            employee = row['employee']
            expense_date = pd.to_datetime(row['date'])
            same_day_expenses = df[
                (df['employee'] == employee) & 
                (pd.to_datetime(df['date']) == expense_date)
            ]
            if len(same_day_expenses) > 3:
                score += 1
                issues.append(f'Multiple expenses on same day ({len(same_day_expenses)} total)')
            
            # Round dollar amounts (potential fabricated expenses)
            if amount % 100 == 0 and amount > 100:
                score += 1
                issues.append('Round dollar amount')
            
            # Suspicious amount patterns
            if amount in [99.99, 199.99, 299.99, 399.99, 499.99, 999.99]:
                score += 2
                issues.append('Suspicious amount pattern')
            
            # Missing vendor information
            if pd.isna(vendor) or str(vendor).strip() == '':
                score += 3
                issues.append('Missing vendor information')
            
            # Unusual categories for employee/department
            category = row['category']
            department = row['department']
            dept_categories = df[df['department'] == department]['category'].value_counts()
            if category not in dept_categories.index or dept_categories[category] == 1:
                score += 1
                issues.append(f'Unusual category "{category}" for department "{department}"')
            
            ecs_scores.append({
                'expense_id': expense_id,
                'score': score,
                'issues': issues,
                'description': row['description'],
                'amount': float(amount),
                'employee': employee,
                'vendor': vendor,
                'category': category,
                'date': expense_date.isoformat()
            })
        
        # 10. Cross-Department Expense Ratio (CDER)
        category_dept_spend = df.groupby(['category', 'department'])['amount'].sum().reset_index()
        cder_results = []
        for category in df['category'].unique():
            category_data = category_dept_spend[category_dept_spend['category'] == category]
            if len(category_data) > 1:
                total_category_spend = category_data['amount'].sum()
                cross_dept_spend = total_category_spend
                cder = (cross_dept_spend / total_category_spend * 100) if total_category_spend > 0 else 0
                cder_results.append({
                    'category': category,
                    'departments': len(category_data),
                    'cross_dept_ratio': float(cder),
                    'total_spend': float(total_category_spend)
                })
        
        # 11. Expense Timing Anomaly Score (ETAS)
        etas_results = []
        try:
            if pd.api.types.is_datetime64_any_dtype(df['date']):
                day_of_month_counts = df.groupby(df['date'].dt.day).size()
                expected_pattern = day_of_month_counts.mean()
                pattern_std = day_of_month_counts.std()
                
                for day, count in day_of_month_counts.items():
                    if pattern_std > 0:
                        etas = abs(count - expected_pattern) / pattern_std
                        etas_results.append({
                            'day_of_month': int(day),
                            'expense_count': int(count),
                            'expected': float(expected_pattern),
                            'etas_score': float(etas)
                        })
        except Exception as e:
            print(f"Warning: Could not calculate expense timing anomaly score: {e}")
        
        # 12. Vendor Loyalty Index (VLI)
        employee_vendor_counts = df.groupby('employee')['vendor_supplier'].nunique()
        employee_total_expenses = df.groupby('employee').size()
        
        vli_results = []
        for employee in df['employee'].unique():
            vendor_count = employee_vendor_counts.get(employee, 0)
            expense_count = employee_total_expenses.get(employee, 0)
            vli = vendor_count / expense_count if expense_count > 0 else 0
            vli_results.append({
                'employee': employee,
                'unique_vendors': int(vendor_count),
                'total_expenses': int(expense_count),
                'vli_score': float(vli)
            })
        
        # 13. Expense Categorization Accuracy (ECA)
        potential_misclassifications = []
        category_keywords = {
            'IT': ['computer', 'software', 'printer', 'maintenance', 'tech'],
            'Marketing': ['advertising', 'event', 'promotion', 'brand'],
            'Office Supplies': ['paper', 'pen', 'stationery', 'supplies'],
            'Travel': ['flight', 'hotel', 'transport', 'parking', 'meal']
        }
        
        for _, row in df.iterrows():
            description = str(row['description']).lower()
            category = str(row['category']).lower()
            
            # Check if description contains keywords from other categories
            for expected_category, keywords in category_keywords.items():
                if expected_category.lower() != category:
                    for keyword in keywords:
                        if keyword in description:
                            potential_misclassifications.append({
                                'expense_id': row.get('id', 'unknown'),
                                'current_category': row['category'],
                                'suggested_category': expected_category,
                                'description': row['description'],
                                'keyword_match': keyword
                            })
                            break
        
        # 14. Budget Burn Rate (BBR) - Placeholder (requires budget data)
        bbr = {
            'note': 'Budget data not available in current dataset',
            'calculation_ready': False
        }
        
        # 15. Approval Turnaround Time (ATT) - Placeholder (requires submission timestamps)
        att = {
            'note': 'Submission timestamps not available in current dataset',
            'calculation_ready': False
        }
        
        # Generate chart data
        chart_data = self._generate_chart_data(df)
        
        # Ensure all numeric values are properly converted
        result = {
            'basic_metrics': {
                'total_expenses': int(total_expenses),
                'total_amount': float(total_amount),
                'average_expense': float(df['amount'].mean()) if len(df) > 0 else 0.0,
                'median_expense': float(df['amount'].median()) if len(df) > 0 else 0.0,
                'largest_expense': float(df['amount'].max()) if len(df) > 0 else 0.0,
                'smallest_expense': float(df['amount'].min()) if len(df) > 0 else 0.0,
                'date_range_days': int(date_range)
            },
            'expense_velocity_ratio': float(evr),
            'approval_concentration_index': float(aci),
            'payment_method_risk_score': float(pmrs),
            'category_deviation_index': cdi_results,
            'vendor_concentration_ratio': float(vcr),
            'high_value_expense_frequency': {
                'percentage': float(hvef),
                'threshold': float(high_value_threshold),
                'count': int(len(high_value_expenses)),
                'total_count': int(total_expenses)
            },
            'department_expense_intensity': dei_results,
            'recurring_expense_variance': rev_results,
            'expense_complexity_scores': ecs_scores,
            'cross_department_expense_ratio': cder_results,
            'expense_timing_anomaly_score': etas_results,
            'vendor_loyalty_index': vli_results,
            'expense_categorization_accuracy': {
                'potential_misclassifications': potential_misclassifications,
                'misclassification_count': int(len(potential_misclassifications))
            },
            'budget_burn_rate': bbr,
            'approval_turnaround_time': att,
            'risk_indicators': {
                'high_aci_warning': bool(aci > 50),  # More than 50% concentration
                'high_pmrs_warning': bool(pmrs > 20),  # More than 20% personal cards
                'high_vcr_warning': bool(vcr > 80),  # More than 80% vendor concentration
                'high_hvef_warning': bool(hvef > 25),  # More than 25% high-value expenses
                'complex_expenses': int(len([s for s in ecs_scores if s['score'] > 5]))
            },
            'chart_data': chart_data
        }
        
        return result
    
    def _generate_chart_data(self, df):
        """Generate comprehensive chart data for visualization"""
        if df is None or len(df) == 0:
            return {}
        
        chart_data = {}
        
        # 1. Expense Distribution by Department
        dept_expenses = df.groupby('department')['amount'].sum().sort_values(ascending=False)
        chart_data['department_expenses'] = {
            'labels': dept_expenses.index.tolist(),
            'data': dept_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
        }
        
        # 2. Expense Distribution by Category
        category_expenses = df.groupby('category')['amount'].sum().sort_values(ascending=False)
        chart_data['category_expenses'] = {
            'labels': category_expenses.index.tolist(),
            'data': category_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB']
        }
        
        # 3. Monthly Expense Trend
        if pd.api.types.is_datetime64_any_dtype(df['date']):
            monthly_expenses = df.groupby(df['date'].dt.to_period('M'))['amount'].sum()
            chart_data['monthly_trend'] = {
                'labels': [str(period) for period in monthly_expenses.index],
                'data': monthly_expenses.values.tolist(),
                'color': '#36A2EB'
            }
        
        # 4. Employee Expense Distribution
        employee_expenses = df.groupby('employee')['amount'].sum().sort_values(ascending=False)
        chart_data['employee_expenses'] = {
            'labels': employee_expenses.index.tolist(),
            'data': employee_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
        }
        
        # 5. Vendor Expense Distribution (Top 10)
        vendor_expenses = df.groupby('vendor_supplier')['amount'].sum().sort_values(ascending=False).head(10)
        chart_data['vendor_expenses'] = {
            'labels': vendor_expenses.index.tolist(),
            'data': vendor_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # 6. Payment Method Distribution
        payment_method_expenses = df.groupby('payment_method')['amount'].sum()
        chart_data['payment_methods'] = {
            'labels': payment_method_expenses.index.tolist(),
            'data': payment_method_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        # 7. Expense Amount Distribution (Histogram data)
        amount_ranges = [
            (0, 100, '0-100'),
            (100, 500, '100-500'),
            (500, 1000, '500-1000'),
            (1000, 5000, '1000-5000'),
            (5000, float('inf'), '5000+')
        ]
        
        amount_distribution = []
        for min_val, max_val, label in amount_ranges:
            if max_val == float('inf'):
                count = len(df[df['amount'] >= min_val])
            else:
                count = len(df[(df['amount'] >= min_val) & (df['amount'] < max_val)])
            amount_distribution.append({'range': label, 'count': count})
        
        chart_data['amount_distribution'] = {
            'labels': [item['range'] for item in amount_distribution],
            'data': [item['count'] for item in amount_distribution],
            'color': '#FFCE56'
        }
        
        # 8. Risk Level Distribution
        risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        risk_distribution = []
        for level in risk_levels:
            # This would need to be calculated based on fraud scores
            # For now, we'll use a placeholder
            risk_distribution.append({'level': level, 'count': 0})
        
        chart_data['risk_distribution'] = {
            'labels': [item['level'] for item in risk_distribution],
            'data': [item['count'] for item in risk_distribution],
            'colors': ['#4BC0C0', '#FFCE56', '#FF9F40', '#FF6384']
        }
        
        # 9. Daily Expense Pattern
        if pd.api.types.is_datetime64_any_dtype(df['date']):
            daily_expenses = df.groupby(df['date'].dt.day)['amount'].sum()
            chart_data['daily_pattern'] = {
                'labels': [f"Day {day}" for day in daily_expenses.index],
                'data': daily_expenses.values.tolist(),
                'color': '#36A2EB'
            }
        
        # 10. Approval Concentration
        approver_expenses = df.groupby('approved_by')['amount'].sum().sort_values(ascending=False)
        chart_data['approval_concentration'] = {
            'labels': approver_expenses.index.tolist(),
            'data': approver_expenses.values.tolist(),
            'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0']
        }
        
        return chart_data 