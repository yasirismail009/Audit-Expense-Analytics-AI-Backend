#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_direct_completeness():
    print('🚀 Testing Direct Completeness Analysis (No AI)')
    print('=' * 60)
    
    try:
        from core.tasks import run_gl_completeness_analysis
        from core.models import DataFile, CompletenessTestResult
        
        file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
        
        # Check if data file exists
        try:
            data_file = DataFile.objects.get(id=file_id)
            print(f"📁 Using data file: {data_file.file_name}")
            print(f"   Status: {data_file.status}")
            print(f"   Records: {data_file.total_records}")
        except DataFile.DoesNotExist:
            print(f"❌ DataFile with ID {file_id} not found.")
            return
        
        # Check existing tests
        existing_tests = CompletenessTestResult.objects.filter(data_file=data_file).count()
        print(f"🧪 Existing completeness tests: {existing_tests}")
        
        # Run the completeness test directly
        print("🔄 Running completeness test...")
        result = run_gl_completeness_analysis(data_file_id=file_id)
        
        print('✅ COMPLETENESS TEST COMPLETED!')
        print('=' * 60)
        
        if result and result.get('success', True):
            print(f"📊 Overall Status: {result.get('completeness_results', {}).get('overall_status', 'Unknown')}")
            print(f"📈 Completeness Score: {result.get('completeness_results', {}).get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Duration: {result.get('completeness_results', {}).get('processing_duration', 0):.2f} seconds")
            
            # Check if saved to DB
            latest_test = CompletenessTestResult.objects.filter(data_file=data_file).order_by('-test_timestamp').first()
            if latest_test:
                print(f"\n💾 Latest result saved to DB (ID: {latest_test.id}):")
                print(f"  Status: {latest_test.overall_status}")
                print(f"  Score: {latest_test.completeness_score:.1f}%")
                print(f"  Critical Issues: {latest_test.critical_issues_count}")
                
                # Check GL listing statistics
                stats = latest_test.comprehensive_statistics
                if stats and 'gl_listing_statistics' in stats:
                    gl_stats = stats['gl_listing_statistics']
                    print(f"\n📊 GL Listing Statistics:")
                    print(f"  Total GL Records: {gl_stats.get('total_gl_records', 0):,}")
                    print(f"  Unique Documents: {gl_stats.get('unique_documents', 0):,}")
                    print(f"  Unique Accounts: {gl_stats.get('unique_accounts', 0):,}")
                    print(f"  Unique Users: {gl_stats.get('unique_users', 0):,}")
                else:
                    print("  ❌ No GL listing statistics found")
            else:
                print("❌ No completeness test result found in DB after running.")
        else:
            error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
            print(f"❌ Test Failed: {error_msg}")
        
    except Exception as e:
        print(f"💥 Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_direct_completeness()
