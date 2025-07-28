from django.core.management.base import BaseCommand
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory
from core.views import DataFileViewSet
import csv
import io

class Command(BaseCommand):
    help = 'Test CSV upload functionality'

    def handle(self, *args, **options):
        self.stdout.write("Testing CSV Upload Functionality...")
        self.stdout.write("=" * 50)
        
        # Create test CSV content
        csv_content = """Document,Document type,Posting Date,Document Date,Entry Date,Amount in Local Currency,Local Currency,G/L Account,Profit Center,User Name,Fiscal Year,Posting period,Text,Segment,Clearing Document,Offsetting,Invoice Reference,Sales Document,Assignment,Year/Month
1000000001,DZ,01/15/2025,01/15/2025,01/15/2025,50000.00,SAR,5000,PC001,USER001,2025,1,Office Supplies,SEG001,,,INV001,,ASS001,2025/01
1000000002,DZ,01/16/2025,01/16/2025,01/16/2025,75000.00,SAR,5100,PC002,USER002,2025,1,Travel Expenses,SEG002,,,INV002,,ASS002,2025/01
1000000003,SA,01/17/2025,01/17/2025,01/17/2025,120000.00,SAR,6000,PC003,USER003,2025,1,Advertising,SEG003,,,INV003,,ASS003,2025/01
1000000004,DZ,01/18/2025,01/18/2025,01/18/2025,25000.00,SAR,7000,PC004,USER004,2025,1,Utilities,SEG004,,,INV004,,ASS004,2025/01
1000000005,SA,01/19/2025,01/19/2025,01/19/2025,180000.00,SAR,5000,PC005,USER005,2025,1,Professional Services,SEG005,,,INV005,,ASS005,2025/01"""
        
        # Create file object
        file_obj = SimpleUploadedFile(
            "test_upload.csv",
            csv_content.encode('utf-8'),
            content_type='text/csv'
        )
        
        # Create request
        factory = RequestFactory()
        request = factory.post('/api/data-files/upload/')
        request.FILES['file'] = file_obj
        request.data = {
            'engagement_id': 'TEST_ENG_001',
            'client_name': 'Test Client',
            'company_name': 'Test Company',
            'fiscal_year': 2025,
            'audit_start_date': '2025-01-01',
            'audit_end_date': '2025-12-31'
        }
        
        try:
            # Test upload
            viewset = DataFileViewSet()
            response = viewset.upload(request)
            
            if response.status_code == 201:
                self.stdout.write(
                    self.style.SUCCESS("✅ Upload successful!")
                )
                result = response.data
                self.stdout.write(f"File ID: {result.get('id')}")
                self.stdout.write(f"File Name: {result.get('file_name')}")
                self.stdout.write(f"Status: {result.get('status')}")
                self.stdout.write(f"Processed Records: {result.get('processed_records')}")
                self.stdout.write(f"Total Records: {result.get('total_records')}")
                
                # Check if data was saved to database
                if result.get('processed_records', 0) > 0:
                    self.stdout.write(
                        self.style.SUCCESS("✅ Data was successfully saved to database!")
                    )
                    
                    # Verify data in database
                    from core.models import DataFile, SAPGLPosting
                    data_file = DataFile.objects.get(id=result.get('id'))
                    postings = SAPGLPosting.objects.filter(data_file=data_file)
                    
                    self.stdout.write(f"✅ Found {postings.count()} postings in database")
                    
                    # Show some sample data
                    for posting in postings[:3]:
                        self.stdout.write(f"  - {posting.document_number}: {posting.amount_local_currency} {posting.local_currency}")
                        
                else:
                    self.stdout.write(
                        self.style.ERROR("❌ No records were processed")
                    )
            else:
                self.stdout.write(
                    self.style.ERROR(f"❌ Upload failed with status {response.status_code}")
                )
                self.stdout.write(f"Error: {response.data}")
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Error during upload: {e}")
            )
            import traceback
            self.stdout.write(traceback.format_exc()) 