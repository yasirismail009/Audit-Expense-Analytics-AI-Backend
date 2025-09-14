#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def run_completeness_direct():
    print('🚀 Running Completeness Analysis Directly')
    print('=' * 60)
    
    try:
        from core.models import DataFile, CompletenessTestResult, SAPGLPosting, TrialBalance, GLAccount
        from django.utils import timezone
        from django.db.models import Sum, Count, Q
        import logging
        
        logger = logging.getLogger(__name__)
        
        file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
        
        # Get data file
        try:
            data_file = DataFile.objects.get(id=file_id)
            print(f"📁 Using data file: {data_file.file_name}")
        except DataFile.DoesNotExist:
            print(f"❌ DataFile with ID {file_id} not found.")
            return
        
        # Get GL postings
        gl_postings = SAPGLPosting.objects.filter(data_file=data_file)
        print(f"📊 GL Postings: {gl_postings.count()}")
        
        if gl_postings.count() == 0:
            print("❌ No GL postings found for this data file.")
            return
        
        # Get TB records
        tb_records = TrialBalance.objects.filter(data_file=data_file)
        print(f"📊 TB Records: {tb_records.count()}")
        
        # Get GL accounts
        gl_accounts = GLAccount.objects.filter(data_file=data_file)
        print(f"📊 GL Accounts: {gl_accounts.count()}")
        
        # Step 1: GL-TB Reconciliation
        print("\n🔄 Step 1: GL-TB Reconciliation")
        gl_debit_total = gl_postings.aggregate(total=Sum('amount_local_currency', filter=Q(amount_local_currency__gt=0)))['total'] or 0
        gl_credit_total = abs(gl_postings.aggregate(total=Sum('amount_local_currency', filter=Q(amount_local_currency__lt=0)))['total'] or 0)
        
        tb_debit_total = tb_records.aggregate(total=Sum('debit_balance'))['total'] or 0
        tb_credit_total = tb_records.aggregate(total=Sum('credit_balance'))['total'] or 0
        
        print(f"  GL Debits: ${gl_debit_total:,.2f}")
        print(f"  GL Credits: ${gl_credit_total:,.2f}")
        print(f"  TB Debits: ${tb_debit_total:,.2f}")
        print(f"  TB Credits: ${tb_credit_total:,.2f}")
        
        debit_variance = abs(gl_debit_total - tb_debit_total)
        credit_variance = abs(gl_credit_total - tb_credit_total)
        step1_passed = debit_variance < 0.01 and credit_variance < 0.01
        
        print(f"  Debit Variance: ${debit_variance:.2f}")
        print(f"  Credit Variance: ${credit_variance:.2f}")
        print(f"  Step 1 Result: {'✅ PASS' if step1_passed else '❌ FAIL'}")
        
        # Step 2: Debit-Credit Balance
        print("\n🔄 Step 2: Debit-Credit Balance")
        gl_balance = gl_debit_total - gl_credit_total
        tb_balance = tb_debit_total - tb_credit_total
        
        print(f"  GL Balance: ${gl_balance:,.2f}")
        print(f"  TB Balance: ${tb_balance:,.2f}")
        
        step2_passed = abs(gl_balance) < 0.01 and abs(tb_balance) < 0.01
        print(f"  Step 2 Result: {'✅ PASS' if step2_passed else '❌ FAIL'}")
        
        # Step 3: Account Coverage
        print("\n🔄 Step 3: Account Coverage")
        gl_account_codes = set(gl_postings.values_list('gl_account', flat=True))
        tb_account_codes = set(tb_records.values_list('account_code', flat=True))
        
        missing_in_gl = tb_account_codes - gl_account_codes
        missing_in_tb = gl_account_codes - tb_account_codes
        
        print(f"  GL Accounts: {len(gl_account_codes)}")
        print(f"  TB Accounts: {len(tb_account_codes)}")
        print(f"  Missing in GL: {len(missing_in_gl)}")
        print(f"  Missing in TB: {len(missing_in_tb)}")
        
        step3_passed = len(missing_in_gl) == 0
        print(f"  Step 3 Result: {'✅ PASS' if step3_passed else '❌ FAIL'}")
        
        # Calculate overall results
        tests_passed = sum([step1_passed, step2_passed, step3_passed])
        total_tests = 3
        completeness_score = (tests_passed / total_tests) * 100
        overall_status = 'PASS' if tests_passed >= 2 else 'FAIL'
        
        print(f"\n📊 Overall Results:")
        print(f"  Tests Passed: {tests_passed}/{total_tests}")
        print(f"  Completeness Score: {completeness_score:.1f}%")
        print(f"  Overall Status: {overall_status}")
        
        # Create GL listing statistics
        print("\n📈 Generating GL Listing Statistics...")
        
        # Basic GL listing statistics
        total_gl_records = gl_postings.count()
        unique_documents = gl_postings.values('document_number').distinct().count()
        unique_accounts = gl_postings.values('gl_account').distinct().count()
        unique_users = gl_postings.values('user_name').distinct().count()
        
        # GL Account completeness analysis
        gl_account_stats = {}
        for posting in gl_postings:
            account_code = posting.gl_account
            amount = float(posting.amount_local_currency or 0)
            
            if account_code not in gl_account_stats:
                gl_account_stats[account_code] = {
                    'debit_total': 0, 
                    'credit_total': 0, 
                    'debit_count': 0, 
                    'credit_count': 0
                }
            
            if amount > 0:
                gl_account_stats[account_code]['debit_total'] += amount
                gl_account_stats[account_code]['debit_count'] += 1
            else:
                gl_account_stats[account_code]['credit_total'] += abs(amount)
                gl_account_stats[account_code]['credit_count'] += 1
        
        # Convert to sorted list format (by total volume)
        gl_account_completeness = []
        for account_code, stats in sorted(gl_account_stats.items()):
            total_volume = stats['debit_total'] + stats['credit_total']
            gl_account_completeness.append({
                'account_code': account_code,
                'debit_total': round(stats['debit_total'], 2),
                'credit_total': round(stats['credit_total'], 2),
                'debit_count': stats['debit_count'],
                'credit_count': stats['credit_count'],
                'net_amount': round(stats['debit_total'] - stats['credit_total'], 2),
                'total_transactions': stats['debit_count'] + stats['credit_count'],
                'total_volume': round(total_volume, 2)
            })
        
        # Sort by total volume (descending)
        gl_account_completeness.sort(key=lambda x: x['total_volume'], reverse=True)
        
        print(f"  Total GL Records: {total_gl_records:,}")
        print(f"  Unique Documents: {unique_documents:,}")
        print(f"  Unique Accounts: {unique_accounts:,}")
        print(f"  Unique Users: {unique_users:,}")
        print(f"  GL Account Analysis: {len(gl_account_completeness)} accounts")
        
        # Save to database
        print("\n💾 Saving to Database...")
        
        comprehensive_statistics = {
            'gl_listing_statistics': {
                'total_gl_records': total_gl_records,
                'unique_documents': unique_documents,
                'unique_accounts': unique_accounts,
                'unique_users': unique_users,
                'duplicate_document_ratio': round((total_gl_records - unique_documents) / total_gl_records * 100, 2) if total_gl_records > 0 else 0
            },
            'gl_account_completeness': gl_account_completeness,
            'summary_statistics': {
                'total_gl_accounts': len(gl_account_completeness),
                'most_active_account': gl_account_completeness[0]['account_code'] if gl_account_completeness else 'N/A',
                'total_debit_amount': round(sum(acc['debit_total'] for acc in gl_account_completeness), 2),
                'total_credit_amount': round(sum(acc['credit_total'] for acc in gl_account_completeness), 2)
            }
        }
        
        # Create completeness test result
        completeness_test = CompletenessTestResult.objects.create(
            data_file=data_file,
            overall_status=overall_status,
            completeness_score=completeness_score,
            tests_passed=tests_passed,
            total_tests=total_tests,
            critical_issues_count=len(missing_in_gl) + (1 if not step1_passed else 0) + (1 if not step2_passed else 0),
            comprehensive_statistics=comprehensive_statistics,
            step1_gl_tb_reconciliation={
                'passed': step1_passed,
                'debit_variance': debit_variance,
                'credit_variance': credit_variance,
                'gl_debit_total': gl_debit_total,
                'gl_credit_total': gl_credit_total,
                'tb_debit_total': tb_debit_total,
                'tb_credit_total': tb_credit_total
            },
            step2_debit_credit_balance={
                'passed': step2_passed,
                'gl_balance': gl_balance,
                'tb_balance': tb_balance
            },
            step3_account_coverage={
                'passed': step3_passed,
                'missing_in_gl': list(missing_in_gl),
                'missing_in_tb': list(missing_in_tb),
                'gl_account_count': len(gl_account_codes),
                'tb_account_count': len(tb_account_codes)
            },
            processing_duration=0.0,
            test_timestamp=timezone.now()
        )
        
        print(f"✅ Completeness test saved to database (ID: {completeness_test.id})")
        print(f"   Status: {completeness_test.overall_status}")
        print(f"   Score: {completeness_test.completeness_score:.1f}%")
        print(f"   Critical Issues: {completeness_test.critical_issues_count}")
        
        # Verify save
        saved_test = CompletenessTestResult.objects.get(id=completeness_test.id)
        print(f"✅ Verification: Test {saved_test.id} found in database")
        
    except Exception as e:
        print(f"💥 Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_completeness_direct()
