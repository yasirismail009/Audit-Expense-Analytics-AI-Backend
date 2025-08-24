"""
Unified Chart Data Generator for Analytics
Provides consistent chart data structure for all analysis types
"""

import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Q, Count, Sum, Avg, Min, Max
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

class UnifiedChartDataGenerator:
    """Generates unified chart data for all analysis types"""
    
    def __init__(self):
        self.chart_colors = {
            'primary': '#4BC0C0',
            'secondary': '#FFCE56', 
            'danger': '#FF6384',
            'warning': '#FF9F40',
            'success': '#36A2EB',
            'info': '#9966FF',
            'light': '#C9CBCF',
            'dark': '#4BC0C0'
        }
    
    def generate_amount_charts(self, transactions, analysis_type):
        """Generate amount-based charts for any analysis type"""
        if not transactions:
            return self._empty_chart_data()
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'posting_date': t.posting_date,
            'gl_account': t.gl_account,
            'user_name': t.user_name,
            'transaction_type': t.transaction_type,
            'document_type': t.document_type
        } for t in transactions])
        
        # Amount distribution chart
        amount_ranges = [
            {'min': 0, 'max': 1000, 'label': '0-1K'},
            {'min': 1000, 'max': 10000, 'label': '1K-10K'},
            {'min': 10000, 'max': 100000, 'label': '10K-100K'},
            {'min': 100000, 'max': 1000000, 'label': '100K-1M'},
            {'min': 1000000, 'max': float('inf'), 'label': '1M+'}
        ]
        
        amount_distribution = []
        for range_info in amount_ranges:
            count = len(df[(df['amount'] >= range_info['min']) & (df['amount'] < range_info['max'])])
            amount_distribution.append({
                'range': range_info['label'],
                'count': count,
                'percentage': (count / len(df) * 100) if len(df) > 0 else 0
            })
        
        # Top amounts chart
        top_amounts = df.nlargest(10, 'amount')[['amount', 'gl_account', 'user_name', 'posting_date']].to_dict('records')
        
        # Amount trend over time
        df['month'] = pd.to_datetime(df['posting_date']).dt.to_period('M')
        monthly_amounts = df.groupby('month')['amount'].sum().reset_index()
        monthly_amounts['month'] = monthly_amounts['month'].astype(str)
        
        return {
            'amount_distribution': {
                'type': 'bar',
                'data': {
                    'labels': [item['range'] for item in amount_distribution],
                    'datasets': [{
                        'label': f'{analysis_type} Amount Distribution',
                        'data': [item['count'] for item in amount_distribution],
                        'backgroundColor': [self.chart_colors['primary']] * len(amount_distribution),
                        'borderColor': [self.chart_colors['dark']] * len(amount_distribution),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Amount Distribution'
                        }
                    }
                }
            },
            'top_amounts': {
                'type': 'horizontalBar',
                'data': {
                    'labels': [f"{item['gl_account']} - {item['user_name']}" for item in top_amounts],
                    'datasets': [{
                        'label': 'Amount',
                        'data': [item['amount'] for item in top_amounts],
                        'backgroundColor': [self.chart_colors['danger']] * len(top_amounts),
                        'borderColor': [self.chart_colors['dark']] * len(top_amounts),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Top 10 Amounts'
                        }
                    }
                }
            },
            'monthly_trend': {
                'type': 'line',
                'data': {
                    'labels': monthly_amounts['month'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} Monthly Trend',
                        'data': monthly_amounts['amount'].tolist(),
                        'borderColor': self.chart_colors['success'],
                        'backgroundColor': self.chart_colors['light'],
                        'fill': True,
                        'tension': 0.1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Monthly Amount Trend'
                        }
                    }
                }
            }
        }
    
    def generate_account_charts(self, transactions, analysis_type):
        """Generate account-based charts for any analysis type"""
        if not transactions:
            return self._empty_chart_data()
        
        # Convert to DataFrame
        df = pd.DataFrame([{
            'gl_account': t.gl_account,
            'amount': float(t.amount_local_currency),
            'transaction_type': t.transaction_type,
            'posting_date': t.posting_date
        } for t in transactions])
        
        # Account distribution
        account_summary = df.groupby('gl_account').agg({
            'amount': ['sum', 'count'],
            'transaction_type': 'count'
        }).reset_index()
        account_summary.columns = ['gl_account', 'total_amount', 'transaction_count', 'type_count']
        
        # Top accounts by amount
        top_accounts_by_amount = account_summary.nlargest(10, 'total_amount')
        
        # Top accounts by transaction count
        top_accounts_by_count = account_summary.nlargest(10, 'transaction_count')
        
        # Account risk distribution
        account_risk = []
        for _, row in account_summary.iterrows():
            risk_level = 'low'
            if row['total_amount'] > 1000000:
                risk_level = 'critical'
            elif row['total_amount'] > 100000:
                risk_level = 'high'
            elif row['total_amount'] > 10000:
                risk_level = 'medium'
            
            account_risk.append({
                'account': row['gl_account'],
                'amount': row['total_amount'],
                'count': row['transaction_count'],
                'risk_level': risk_level
            })
        
        risk_distribution = {}
        for item in account_risk:
            risk_level = item['risk_level']
            if risk_level not in risk_distribution:
                risk_distribution[risk_level] = {'count': 0, 'amount': 0}
            risk_distribution[risk_level]['count'] += 1
            risk_distribution[risk_level]['amount'] += item['amount']
        
        return {
            'top_accounts_by_amount': {
                'type': 'bar',
                'data': {
                    'labels': top_accounts_by_amount['gl_account'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Top Accounts by Amount',
                        'data': top_accounts_by_amount['total_amount'].tolist(),
                        'backgroundColor': [self.chart_colors['primary']] * len(top_accounts_by_amount),
                        'borderColor': [self.chart_colors['dark']] * len(top_accounts_by_amount),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Top Accounts by Amount'
                        }
                    }
                }
            },
            'top_accounts_by_count': {
                'type': 'bar',
                'data': {
                    'labels': top_accounts_by_count['gl_account'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Top Accounts by Transaction Count',
                        'data': top_accounts_by_count['transaction_count'].tolist(),
                        'backgroundColor': [self.chart_colors['secondary']] * len(top_accounts_by_count),
                        'borderColor': [self.chart_colors['dark']] * len(top_accounts_by_count),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Top Accounts by Transaction Count'
                        }
                    }
                }
            },
            'account_risk_distribution': {
                'type': 'doughnut',
                'data': {
                    'labels': list(risk_distribution.keys()),
                    'datasets': [{
                        'label': f'{analysis_type} - Account Risk Distribution',
                        'data': [risk_distribution[risk]['count'] for risk in risk_distribution.keys()],
                        'backgroundColor': [
                            self.chart_colors['success'],  # low
                            self.chart_colors['warning'],  # medium
                            self.chart_colors['danger'],   # high
                            self.chart_colors['dark']      # critical
                        ][:len(risk_distribution)],
                        'borderWidth': 2
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Account Risk Distribution'
                        }
                    }
                }
            }
        }
    
    def generate_user_charts(self, transactions, analysis_type):
        """Generate user-based charts for any analysis type"""
        if not transactions:
            return self._empty_chart_data()
        
        # Convert to DataFrame
        df = pd.DataFrame([{
            'user_name': t.user_name,
            'amount': float(t.amount_local_currency),
            'transaction_type': t.transaction_type,
            'posting_date': t.posting_date,
            'gl_account': t.gl_account
        } for t in transactions])
        
        # User summary
        user_summary = df.groupby('user_name').agg({
            'amount': ['sum', 'count'],
            'gl_account': 'nunique'
        }).reset_index()
        user_summary.columns = ['user_name', 'total_amount', 'transaction_count', 'unique_accounts']
        
        # Top users by amount
        top_users_by_amount = user_summary.nlargest(10, 'total_amount')
        
        # Top users by transaction count
        top_users_by_count = user_summary.nlargest(10, 'transaction_count')
        
        # User activity over time
        df['month'] = pd.to_datetime(df['posting_date']).dt.to_period('M')
        user_monthly_activity = df.groupby(['user_name', 'month']).size().reset_index(name='count')
        
        # Get top 5 users for activity chart
        top_users = user_summary.nlargest(5, 'transaction_count')['user_name'].tolist()
        top_user_activity = user_monthly_activity[user_monthly_activity['user_name'].isin(top_users)]
        
        return {
            'top_users_by_amount': {
                'type': 'bar',
                'data': {
                    'labels': top_users_by_amount['user_name'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Top Users by Amount',
                        'data': top_users_by_amount['total_amount'].tolist(),
                        'backgroundColor': [self.chart_colors['primary']] * len(top_users_by_amount),
                        'borderColor': [self.chart_colors['dark']] * len(top_users_by_amount),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Top Users by Amount'
                        }
                    }
                }
            },
            'top_users_by_count': {
                'type': 'bar',
                'data': {
                    'labels': top_users_by_count['user_name'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Top Users by Transaction Count',
                        'data': top_users_by_count['transaction_count'].tolist(),
                        'backgroundColor': [self.chart_colors['secondary']] * len(top_users_by_count),
                        'borderColor': [self.chart_colors['dark']] * len(top_users_by_count),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Top Users by Transaction Count'
                        }
                    }
                }
            },
            'user_activity_trend': {
                'type': 'line',
                'data': {
                    'labels': sorted(top_user_activity['month'].unique().astype(str)),
                    'datasets': []
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - User Activity Trend'
                        }
                    }
                }
            }
        }
    
    def generate_date_charts(self, transactions, analysis_type):
        """Generate date-based charts for any analysis type"""
        if not transactions:
            return self._empty_chart_data()
        
        # Convert to DataFrame
        df = pd.DataFrame([{
            'posting_date': t.posting_date,
            'amount': float(t.amount_local_currency),
            'transaction_type': t.transaction_type,
            'user_name': t.user_name
        } for t in transactions if t.posting_date])
        
        if df.empty:
            return self._empty_chart_data()
        
        # Daily activity
        daily_activity = df.groupby('posting_date').agg({
            'amount': ['sum', 'count']
        }).reset_index()
        daily_activity.columns = ['date', 'total_amount', 'transaction_count']
        
        # Monthly activity
        df['month'] = pd.to_datetime(df['posting_date']).dt.to_period('M')
        monthly_activity = df.groupby('month').agg({
            'amount': ['sum', 'count']
        }).reset_index()
        monthly_activity.columns = ['month', 'total_amount', 'transaction_count']
        monthly_activity['month'] = monthly_activity['month'].astype(str)
        
        # Day of week activity
        df['day_of_week'] = pd.to_datetime(df['posting_date']).dt.day_name()
        dow_activity = df.groupby('day_of_week').agg({
            'amount': ['sum', 'count']
        }).reset_index()
        dow_activity.columns = ['day_of_week', 'total_amount', 'transaction_count']
        
        # Order days properly
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_activity['day_order'] = dow_activity['day_of_week'].map({day: i for i, day in enumerate(day_order)})
        dow_activity = dow_activity.sort_values('day_order')
        
        return {
            'daily_activity': {
                'type': 'line',
                'data': {
                    'labels': daily_activity['date'].dt.strftime('%Y-%m-%d').tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Daily Transaction Count',
                        'data': daily_activity['transaction_count'].tolist(),
                        'borderColor': self.chart_colors['primary'],
                        'backgroundColor': self.chart_colors['light'],
                        'fill': False,
                        'tension': 0.1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Daily Activity'
                        }
                    }
                }
            },
            'monthly_activity': {
                'type': 'bar',
                'data': {
                    'labels': monthly_activity['month'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Monthly Transaction Count',
                        'data': monthly_activity['transaction_count'].tolist(),
                        'backgroundColor': [self.chart_colors['secondary']] * len(monthly_activity),
                        'borderColor': [self.chart_colors['dark']] * len(monthly_activity),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Monthly Activity'
                        }
                    }
                }
            },
            'day_of_week_activity': {
                'type': 'bar',
                'data': {
                    'labels': dow_activity['day_of_week'].tolist(),
                    'datasets': [{
                        'label': f'{analysis_type} - Day of Week Activity',
                        'data': dow_activity['transaction_count'].tolist(),
                        'backgroundColor': [self.chart_colors['info']] * len(dow_activity),
                        'borderColor': [self.chart_colors['dark']] * len(dow_activity),
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': f'{analysis_type} - Day of Week Activity'
                        }
                    }
                }
            }
        }
    
    def generate_unified_charts(self, transactions, analysis_type):
        """Generate all chart types for a given analysis"""
        return {
            'amount_charts': self.generate_amount_charts(transactions, analysis_type),
            'account_charts': self.generate_account_charts(transactions, analysis_type),
            'user_charts': self.generate_user_charts(transactions, analysis_type),
            'date_charts': self.generate_date_charts(transactions, analysis_type)
        }
    
    def _empty_chart_data(self):
        """Return empty chart data structure"""
        return {
            'amount_distribution': {
                'type': 'bar',
                'data': {'labels': [], 'datasets': []},
                'options': {'responsive': True}
            },
            'top_amounts': {
                'type': 'horizontalBar',
                'data': {'labels': [], 'datasets': []},
                'options': {'responsive': True}
            },
            'monthly_trend': {
                'type': 'line',
                'data': {'labels': [], 'datasets': []},
                'options': {'responsive': True}
            }
        }
