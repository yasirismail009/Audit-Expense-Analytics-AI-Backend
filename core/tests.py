from django.test import TestCase, Client
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from datetime import date
import uuid

from .models import (
    SAPGLPosting, DataFile, AnalysisSession, TransactionAnalysis, 
    SystemMetrics, GLAccount, FileProcessingJob, MLModelTraining
)

class ModelTestCase(TestCase):
    """Test cases for model functionality"""
    
    def setUp(self):
        """Set up test data"""
        # Create GL Account
        self.gl_account = GLAccount.objects.create(
            account_id='1000',
            account_name='Cash',
            account_type='Asset',
            account_category='Current Assets',
            normal_balance='DEBIT'
        )
        
        # Create SAP GL Posting
        self.posting = SAPGLPosting.objects.create(
            document_number='1000000001',
            posting_date=date(2024, 1, 1),
            gl_account='1000',
            gl_account_ref=self.gl_account,
            amount_local_currency=Decimal('1000.00'),
            transaction_type='DEBIT',
            local_currency='SAR',
            user_name='TEST_USER',
            fiscal_year=2024,
            posting_period=1
        )
        
        # Create Data File
        self.data_file = DataFile.objects.create(
            file_name='test_file.csv',
            file_size=1024,
            engagement_id='TEST_ENGAGEMENT',
            client_name='Test Client',
            company_name='Test Company',
            fiscal_year=2024,
            audit_start_date=date(2024, 1, 1),
            audit_end_date=date(2024, 12, 31),
            status='COMPLETED'
        )

    def test_gl_account_str(self):
        """Test GL Account string representation"""
        self.assertEqual(str(self.gl_account), '1000 - Cash')

    def test_sap_posting_str(self):
        """Test SAP GL Posting string representation"""
        expected = f'Document {self.posting.document_number} - {self.posting.amount_local_currency} {self.posting.local_currency}'
        self.assertEqual(str(self.posting), expected)

    def test_data_file_str(self):
        """Test Data File string representation"""
        self.assertEqual(str(self.data_file), 'test_file.csv')

    def test_gl_account_current_balance(self):
        """Test GL Account current balance calculation"""
        # Add a credit transaction
        SAPGLPosting.objects.create(
            document_number='1000000002',
            posting_date=date(2024, 1, 2),
            gl_account='1000',
            gl_account_ref=self.gl_account,
            amount_local_currency=Decimal('500.00'),
            transaction_type='CREDIT',
            local_currency='SAR',
            user_name='TEST_USER',
            fiscal_year=2024,
            posting_period=1
        )
        
        # Debit balance should be 1000 - 500 = 500
        self.assertEqual(self.gl_account.current_balance, Decimal('500.00'))

class APITestCase(TestCase):
    """Test cases for API endpoints"""
    
    def setUp(self):
        """Set up test client and data"""
        self.client = Client()
        
        # Create test data
        self.gl_account = GLAccount.objects.create(
            account_id='1000',
            account_name='Cash',
            account_type='Asset',
            account_category='Current Assets',
            normal_balance='DEBIT'
        )
        
        self.posting = SAPGLPosting.objects.create(
            document_number='1000000001',
            posting_date=date(2024, 1, 1),
            gl_account='1000',
            gl_account_ref=self.gl_account,
            amount_local_currency=Decimal('1000.00'),
            transaction_type='DEBIT',
            local_currency='SAR',
            user_name='TEST_USER',
            fiscal_year=2024,
            posting_period=1
        )

    def test_sap_postings_list(self):
        """Test SAP GL Postings list endpoint"""
        url = reverse('sap-gl-posting-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.json())

    def test_sap_postings_detail(self):
        """Test SAP GL Postings detail endpoint"""
        url = reverse('sap-gl-posting-detail', args=[self.posting.id])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['document_number'], '1000000001')

    def test_gl_accounts_list(self):
        """Test GL Accounts list endpoint"""
        url = reverse('gl-account-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.json())

    def test_gl_accounts_detail(self):
        """Test GL Accounts detail endpoint"""
        url = reverse('gl-account-detail', args=[self.gl_account.id])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['account_id'], '1000')

class AnalyticsTestCase(TestCase):
    """Test cases for analytics functionality"""
    
    def setUp(self):
        """Set up test data for analytics"""
        from .analytics import SAPGLAnalyzer
        
        self.analyzer = SAPGLAnalyzer()
        
        # Create test transactions
        self.gl_account = GLAccount.objects.create(
            account_id='1000',
            account_name='Cash',
            account_type='Asset',
            account_category='Current Assets',
            normal_balance='DEBIT'
        )
        
        # Create duplicate transactions for testing
        for i in range(3):
            SAPGLPosting.objects.create(
                document_number=f'100000000{i+1}',
                posting_date=date(2024, 1, 1),
                gl_account='1000',
                gl_account_ref=self.gl_account,
                amount_local_currency=Decimal('1000.00'),
                transaction_type='DEBIT',
                local_currency='SAR',
                user_name='TEST_USER',
                fiscal_year=2024,
                posting_period=1
            )

    def test_duplicate_detection(self):
        """Test duplicate detection functionality"""
        transactions = list(SAPGLPosting.objects.all())
        result = self.analyzer.detect_duplicate_entries(transactions)
        
        self.assertIn('duplicates', result)
        self.assertIn('summary', result)
        self.assertIn('drilldown_data', result)

    def test_user_anomaly_detection(self):
        """Test user anomaly detection functionality"""
        transactions = list(SAPGLPosting.objects.all())
        result = self.analyzer.detect_user_anomalies(transactions)
        
        self.assertIsInstance(result, list)

class CeleryTestCase(TestCase):
    """Test cases for Celery functionality"""
    
    def test_celery_task_import(self):
        """Test that Celery tasks can be imported"""
        try:
            from .tasks import process_file_with_anomalies, debug_task
            self.assertTrue(True)  # Import successful
        except ImportError as e:
            self.fail(f"Failed to import Celery tasks: {e}")

    def test_celery_app_configuration(self):
        """Test Celery app configuration"""
        try:
            from analytics.celery import app
            self.assertIsNotNone(app)
            self.assertIsNotNone(app.conf.broker_url)
        except ImportError as e:
            self.fail(f"Failed to import Celery app: {e}")
