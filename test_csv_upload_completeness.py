#!/usr/bin/env python3
"""
Comprehensive Completeness Test for CSV File Upload Flow
Tests the complete end-to-end flow from upload to analysis completion
"""

import requests
import json
import time
import csv
import io
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000/api"
TEST_TIMEOUT = 300

class CSVUploadCompletenessTest:
    def __init__(self):
        self.test_results = {}
        self.file_id = None
        self.processing_job_id = None
        
    def run_all_tests(self):
        """Run all completeness tests"""
        print("🧪 CSV Upload Flow Completeness Test")
        print("=" * 50)
        
        tests = [
            ("API Health", self.test_api_health),
            ("Upload Validation", self.test_upload_validation),
            ("CSV Processing", self.test_csv_processing),
            ("Data Integrity", self.test_data_integrity),
            ("Background Processing", self.test_background_processing),
            ("Analysis Pipeline", self.test_analysis_pipeline),
            ("Error Handling", self.test_error_handling),
            ("Performance", self.test_performance),
            ("Data Retrieval", self.test_data_retrieval),
            ("Status Tracking", self.test_status_tracking)
        ]
        
        total_score = 0
        max_score = len(tests) * 10
        
        for test_name, test_func in tests:
            print(f"\n📋 {test_name}")
            print("-" * 30)
            
            try:
                score = test_func()
                total_score += score
                print(f"✅ {test_name}: {score}/10")
            except Exception as e:
                print(f"❌ {test_name} failed: {e}")
                print(f"⚠️  {test_name}: 0/10")
        
        overall_score = (total_score / max_score) * 100
        print(f"\n🎯 Overall Score: {overall_score:.1f}%")
        
        if overall_score >= 80:
            print("✅ COMPLETENESS TEST PASSED!")
            return True
        else:
            print("❌ COMPLETENESS TEST FAILED!")
            return False
    
    def test_api_health(self):
        """Test API accessibility"""
        try:
            response = requests.get(f"{BASE_URL}/data-files/", timeout=10)
            if response.status_code == 200:
                print("✅ API accessible")
                return 10
            else:
                print(f"❌ API returned {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ API not accessible: {e}")
            return 0
    
    def test_upload_validation(self):
        """Test file upload validation"""
        score = 0
        
        # Test no file
        try:
            response = requests.post(f"{BASE_URL}/data-files/upload/", timeout=10)
            if response.status_code == 400:
                score += 2
                print("✅ No file validation works")
        except Exception as e:
            print(f"❌ No file test error: {e}")
        
        # Test valid CSV upload
        try:
            csv_content = self.create_test_csv(5)
            files = {'file': ('test.csv', io.StringIO(csv_content), 'text/csv')}
            data = {
                'engagement_id': 'TEST_001',
                'client_name': 'Test Client',
                'company_name': 'Test Company',
                'fiscal_year': '2025',
                'audit_start_date': '2025-01-01',
                'audit_end_date': '2025-12-31'
            }
            
            response = requests.post(f"{BASE_URL}/data-files/upload/", files=files, data=data, timeout=30)
            
            if response.status_code == 201:
                result = response.json()
                self.file_id = result.get('id')
                score += 8
                print(f"✅ Valid upload successful: {self.file_id}")
            else:
                print(f"❌ Upload failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Upload test error: {e}")
        
        return score
    
    def test_csv_processing(self):
        """Test CSV processing"""
        if not self.file_id:
            return 0
        
        try:
            response = requests.get(f"{BASE_URL}/data-files/{self.file_id}/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                status = data.get('status')
                
                if status in ['COMPLETED', 'PARTIAL']:
                    print(f"✅ Processing completed: {status}")
                    return 10
                elif status == 'PROCESSING':
                    print("⚠️  Still processing")
                    return 5
                else:
                    print(f"❌ Unexpected status: {status}")
                    return 0
            else:
                print(f"❌ Status check failed: {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ Processing test error: {e}")
            return 0
    
    def test_data_integrity(self):
        """Test data integrity"""
        if not self.file_id:
            return 0
        
        try:
            response = requests.get(f"{BASE_URL}/sapgl-postings/{self.file_id}/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                transactions = data.get('results', [])
                
                if len(transactions) > 0:
                    print(f"✅ {len(transactions)} transactions saved")
                    return 10
                else:
                    print("❌ No transactions found")
                    return 0
            else:
                print(f"❌ Data retrieval failed: {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ Data integrity error: {e}")
            return 0
    
    def test_background_processing(self):
        """Test background processing"""
        if not self.file_id:
            return 0
        
        try:
            response = requests.get(f"{BASE_URL}/processing-jobs/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                jobs = data.get('results', [])
                
                file_job = next((j for j in jobs if j.get('data_file') == self.file_id), None)
                if file_job:
                    self.processing_job_id = file_job.get('id')
                    print(f"✅ Processing job created: {self.processing_job_id}")
                    return 10
                else:
                    print("❌ No processing job found")
                    return 0
            else:
                print(f"❌ Jobs retrieval failed: {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ Background processing error: {e}")
            return 0
    
    def test_analysis_pipeline(self):
        """Test analysis pipeline"""
        if not self.file_id:
            return 0
        
        analysis_types = [
            'duplicate-analysis',
            'backdated-analysis',
            'user-analysis',
            'unusual-days-analysis',
            'closing-entries-analysis',
            'holiday-analysis'
        ]
        
        successful = 0
        for analysis_type in analysis_types:
            try:
                response = requests.get(f"{BASE_URL}/{analysis_type}/{self.file_id}/", timeout=30)
                if response.status_code == 200:
                    successful += 1
            except Exception as e:
                print(f"❌ {analysis_type} failed: {e}")
        
        score = (successful / len(analysis_types)) * 10
        print(f"✅ {successful}/{len(analysis_types)} analyses successful")
        return score
    
    def test_error_handling(self):
        """Test error handling"""
        score = 0
        
        # Test invalid CSV
        try:
            invalid_csv = "invalid,csv,format\nno,proper,headers"
            files = {'file': ('invalid.csv', io.StringIO(invalid_csv), 'text/csv')}
            data = {'engagement_id': 'TEST_ERROR', 'client_name': 'Test', 'company_name': 'Test'}
            
            response = requests.post(f"{BASE_URL}/data-files/upload/", files=files, data=data, timeout=30)
            if response.status_code in [400, 500]:
                score += 5
                print("✅ Invalid CSV handled")
        except Exception as e:
            print(f"❌ Error handling test failed: {e}")
        
        # Test missing fields
        try:
            csv_content = self.create_test_csv(2)
            files = {'file': ('missing_fields.csv', io.StringIO(csv_content), 'text/csv')}
            response = requests.post(f"{BASE_URL}/data-files/upload/", files=files, timeout=30)
            if response.status_code == 201:
                score += 5
                print("✅ Missing fields handled with defaults")
        except Exception as e:
            print(f"❌ Missing fields test failed: {e}")
        
        return score
    
    def test_performance(self):
        """Test performance"""
        score = 0
        
        # Test upload speed
        try:
            start_time = time.time()
            csv_content = self.create_test_csv(50)
            files = {'file': ('performance.csv', io.StringIO(csv_content), 'text/csv')}
            data = {'engagement_id': 'TEST_PERF', 'client_name': 'Test', 'company_name': 'Test'}
            
            response = requests.post(f"{BASE_URL}/data-files/upload/", files=files, data=data, timeout=30)
            upload_time = time.time() - start_time
            
            if response.status_code == 201 and upload_time < 10:
                score += 5
                print(f"✅ Upload performance good: {upload_time:.2f}s")
            elif response.status_code == 201:
                score += 2
                print(f"⚠️  Upload performance slow: {upload_time:.2f}s")
        except Exception as e:
            print(f"❌ Performance test failed: {e}")
        
        # Test response speed
        if self.file_id:
            try:
                start_time = time.time()
                response = requests.get(f"{BASE_URL}/data-files/{self.file_id}/", timeout=10)
                response_time = time.time() - start_time
                
                if response.status_code == 200 and response_time < 2:
                    score += 5
                    print(f"✅ Response performance good: {response_time:.2f}s")
                elif response.status_code == 200:
                    score += 2
                    print(f"⚠️  Response performance slow: {response_time:.2f}s")
            except Exception as e:
                print(f"❌ Response performance test failed: {e}")
        
        return score
    
    def test_data_retrieval(self):
        """Test data retrieval"""
        if not self.file_id:
            return 0
        
        score = 0
        
        # Test basic retrieval
        try:
            response = requests.get(f"{BASE_URL}/sapgl-postings/{self.file_id}/", timeout=10)
            if response.status_code == 200:
                score += 5
                print("✅ Data retrieval works")
        except Exception as e:
            print(f"❌ Data retrieval failed: {e}")
        
        # Test filtering
        try:
            response = requests.get(f"{BASE_URL}/sapgl-postings/{self.file_id}/?date_from=2025-01-01", timeout=10)
            if response.status_code == 200:
                score += 3
                print("✅ Filtering works")
        except Exception as e:
            print(f"❌ Filtering failed: {e}")
        
        # Test export
        try:
            response = requests.get(f"{BASE_URL}/excel-export/{self.file_id}/", timeout=30)
            if response.status_code == 200:
                score += 2
                print("✅ Export works")
        except Exception as e:
            print(f"❌ Export failed: {e}")
        
        return score
    
    def test_status_tracking(self):
        """Test status tracking"""
        if not self.file_id:
            return 0
        
        score = 0
        
        # Test file status
        try:
            response = requests.get(f"{BASE_URL}/data-files/{self.file_id}/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') and data.get('uploaded_at'):
                    score += 5
                    print("✅ File status tracking works")
        except Exception as e:
            print(f"❌ File status tracking failed: {e}")
        
        # Test job status
        if self.processing_job_id:
            try:
                response = requests.get(f"{BASE_URL}/processing-jobs/{self.processing_job_id}/status/", timeout=10)
                if response.status_code == 200:
                    score += 3
                    print("✅ Job status tracking works")
            except Exception as e:
                print(f"❌ Job status tracking failed: {e}")
        
        # Test file listing
        try:
            response = requests.get(f"{BASE_URL}/files-listing/", timeout=10)
            if response.status_code == 200:
                score += 2
                print("✅ File listing works")
        except Exception as e:
            print(f"❌ File listing failed: {e}")
        
        return score
    
    def create_test_csv(self, num_records=5):
        """Create test CSV content"""
        headers = [
            'Document', 'Document type', 'Posting Date', 'Document Date', 'Entry Date',
            'Amount in Local Currency', 'Local Currency', 'G/L Account', 'Profit Center',
            'User Name', 'Fiscal Year', 'Posting period', 'Text', 'Segment',
            'Clearing Document', 'Offsetting', 'Invoice Reference', 'Sales Document',
            'Assignment', 'Year/Month'
        ]
        
        csv_content = io.StringIO()
        writer = csv.writer(csv_content)
        writer.writerow(headers)
        
        for i in range(num_records):
            row = [
                str(1000000001 + i),  # Document
                'DZ' if i % 2 == 0 else 'SA',  # Document type
                f'01/{15 + i:02d}/2025',  # Posting Date
                f'01/{15 + i:02d}/2025',  # Document Date
                f'01/{15 + i:02d}/2025',  # Entry Date
                f'{50000 + (i * 25000):.2f}',  # Amount
                'SAR',  # Currency
                str(5000 + i),  # GL Account
                f'PC{i+1:03d}',  # Profit Center
                f'USER{i+1:03d}',  # User Name
                '2025',  # Fiscal Year
                '1',  # Posting period
                f'Test Transaction {i+1}',  # Text
                f'SEG{i+1:03d}',  # Segment
                '',  # Clearing Document
                '',  # Offsetting
                f'INV{i+1:03d}',  # Invoice Reference
                '',  # Sales Document
                f'ASS{i+1:03d}',  # Assignment
                '2025/01'  # Year/Month
            ]
            writer.writerow(row)
        
        return csv_content.getvalue()

def main():
    """Main test execution"""
    print("🚀 Starting CSV Upload Flow Completeness Test")
    print("=" * 50)
    
    # Check API accessibility
    try:
        response = requests.get(f"{BASE_URL}/data-files/", timeout=5)
        if response.status_code != 200:
            print(f"❌ API not accessible. Status: {response.status_code}")
            print("Make sure the Django server is running on localhost:8000")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        print("Make sure the Django server is running on localhost:8000")
        return False
    
    # Run tests
    tester = CSVUploadCompletenessTest()
    success = tester.run_all_tests()
    
    return success

if __name__ == "__main__":
    main()
