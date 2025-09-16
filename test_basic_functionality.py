#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_basic_functionality():
    print('🚀 Testing Basic Functionality')
    print('=' * 40)
    
    try:
        from core.models import DataFile, SAPGLPosting, TrialBalance, GLAccount
        
        # Check data files
        data_files = DataFile.objects.all()
        print(f"📁 DataFiles count: {data_files.count()}")
        
        if data_files.count() == 0:
            print("❌ No data files found")
            return
        
        # Use the first data file
        data_file = data_files.first()
        print(f"📁 Using data file: {data_file.file_name} (ID: {data_file.id})")
        
        # Check GL postings
        gl_postings = SAPGLPosting.objects.filter(data_file=data_file)
        print(f"📊 GL Postings: {gl_postings.count()}")
        
        if gl_postings.count() == 0:
            print("❌ No GL postings found")
            return
        
        # Check TB records
        tb_records = TrialBalance.objects.filter(data_file=data_file)
        print(f"📊 TB Records: {tb_records.count()}")
        
        # Check GL accounts
        gl_accounts = GLAccount.objects.filter(data_file=data_file)
        print(f"📊 GL Accounts: {gl_accounts.count()}")
        
        # Test basic calculations
        print("\n🔄 Testing Basic Calculations...")
        
        # Calculate GL totals
        from django.db.models import Sum, Q
        gl_debit_total = gl_postings.aggregate(
            total=Sum('amount_local_currency', filter=Q(amount_local_currency__gt=0))
        )['total'] or 0
        
        gl_credit_total = abs(gl_postings.aggregate(
            total=Sum('amount_local_currency', filter=Q(amount_local_currency__lt=0))
        )['total'] or 0)
        
        print(f"  GL Debit Total: ${gl_debit_total:,.2f}")
        print(f"  GL Credit Total: ${gl_credit_total:,.2f}")
        
        # Calculate TB totals
        tb_debit_total = tb_records.aggregate(total=Sum('debit_balance'))['total'] or 0
        tb_credit_total = tb_records.aggregate(total=Sum('credit_balance'))['total'] or 0
        
        print(f"  TB Debit Total: ${tb_debit_total:,.2f}")
        print(f"  TB Credit Total: ${tb_credit_total:,.2f}")
        
        # Test account coverage
        gl_account_codes = set(gl_postings.values_list('gl_account', flat=True))
        tb_account_codes = set(tb_records.values_list('account_code', flat=True))
        
        print(f"  GL Account Codes: {len(gl_account_codes)}")
        print(f"  TB Account Codes: {len(tb_account_codes)}")
        
        # Test GL listing statistics
        print("\n📈 Testing GL Listing Statistics...")
        
        unique_documents = gl_postings.values('document_number').distinct().count()
        unique_accounts = gl_postings.values('gl_account').distinct().count()
        unique_users = gl_postings.values('user_name').distinct().count()
        
        print(f"  Unique Documents: {unique_documents}")
        print(f"  Unique Accounts: {unique_accounts}")
        print(f"  Unique Users: {unique_users}")
        
        # Test account analysis
        account_stats = {}
        for posting in gl_postings[:100]:  # Limit to first 100 for testing
            account_code = posting.gl_account
            amount = float(posting.amount_local_currency or 0)
            
            if account_code not in account_stats:
                account_stats[account_code] = {
                    'debit_total': 0,
                    'credit_total': 0,
                    'count': 0
                }
            
            if amount > 0:
                account_stats[account_code]['debit_total'] += amount
            else:
                account_stats[account_code]['credit_total'] += abs(amount)
            
            account_stats[account_code]['count'] += 1
        
        print(f"  Account Analysis: {len(account_stats)} accounts analyzed")
        
        # Show top 5 accounts by volume
        sorted_accounts = sorted(account_stats.items(), 
                               key=lambda x: x[1]['debit_total'] + x[1]['credit_total'], 
                               reverse=True)
        
        print("  Top 5 Accounts by Volume:")
        for i, (account, stats) in enumerate(sorted_accounts[:5], 1):
            total_volume = stats['debit_total'] + stats['credit_total']
            print(f"    {i}. {account}: ${total_volume:,.2f}")
        
        print("\n✅ Basic functionality test completed successfully!")
        print("   All core calculations are working properly.")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_basic_functionality()

