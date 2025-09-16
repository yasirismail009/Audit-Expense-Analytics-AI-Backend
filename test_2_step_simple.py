#!/usr/bin/env python
"""
Simple 2-Step GL Completeness Test for ENG-008
"""
import os
import django
import sys
from uuid import UUID

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, CompletenessTestResult, SAPGLPosting, TrialBalance, GLAccount
from django.utils import timezone

def run_2_step_test():
    engagement_id_str = 'ENG-008'
    gl_file_id_str = '51671285-0c2e-4c26-ae59-0292a21fd113'

    print("🎯 2-Step GL Completeness Test for ENG-008")
    print("=" * 60)

    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        print(f"✅ Engagement: {engagement.engagement_id}")

        gl_file = DataFile.objects.get(id=UUID(gl_file_id_str))
        print(f"📊 GL File: {gl_file.file_name}")

        # Get data
        gl_postings = SAPGLPosting.objects.filter(data_file=gl_file)
        trial_balance_records = TrialBalance.objects.filter(data_file__engagement=engagement)
        
        print(f"\n📈 Data Summary:")
        print(f"  GL Records: {gl_postings.count():,}")
        print(f"  TB Records: {trial_balance_records.count():,}")

        # =======================================================================
        # STEP 1: GL COMPLETENESS CHECK
        # =======================================================================
        
        print("\n🔍 STEP 1: GL Completeness Check...")
        
        # Calculate GL totals (debits are positive, credits are negative)
        gl_total_debit = sum(float(p.amount_local_currency or 0) for p in gl_postings if float(p.amount_local_currency or 0) > 0)
        gl_total_credit = sum(abs(float(p.amount_local_currency or 0)) for p in gl_postings if float(p.amount_local_currency or 0) < 0)
        
        # Check if GL is balanced (Credit - Debit = 0)
        gl_net_balance = gl_total_credit - gl_total_debit
        gl_is_balanced = abs(gl_net_balance) < 1.00  # Allow 1 unit variance
        
        # Additional completeness checks
        transaction_count = gl_postings.count()
        account_count = gl_postings.values('gl_account').distinct().count()
        has_tb = trial_balance_records.exists()
        
        # Volume and data availability check
        volume_check = transaction_count >= 100 and account_count >= 10
        data_availability = has_tb and gl_postings.exists()
        
        step1_passed = gl_is_balanced and volume_check and data_availability
        
        print(f"   💰 GL Debit Total: {gl_total_debit:,.2f}")
        print(f"   💰 GL Credit Total: {gl_total_credit:,.2f}")
        print(f"   ⚖️  GL Net Balance (Credit - Debit): {gl_net_balance:,.2f}")
        print(f"   ✅ GL Balanced: {'YES' if gl_is_balanced else 'NO'}")
        print(f"   📊 Transaction Count: {transaction_count:,}")
        print(f"   📋 Account Count: {account_count:,}")
        print(f"   📈 TB Available: {'YES' if has_tb else 'NO'}")
        print(f"   🎯 Volume Check: {'PASS' if volume_check else 'FAIL'}")
        print(f"   ✅ STEP 1 RESULT: {'✅ PASS' if step1_passed else '❌ FAIL'}")

        # =======================================================================
        # STEP 2: ACCOUNT-WISE BALANCE VERIFICATION
        # =======================================================================
        
        print("\n🔍 STEP 2: Account-wise Balance Verification...")
        
        # Build GL account-wise totals
        gl_account_totals = {}
        for posting in gl_postings:
            account_code = str(posting.gl_account).replace('.0', '')
            amount = float(posting.amount_local_currency or 0)
            
            if account_code not in gl_account_totals:
                gl_account_totals[account_code] = {
                    'debit_total': 0,
                    'credit_total': 0,
                    'net_movement': 0
                }
            
            if amount > 0:
                gl_account_totals[account_code]['debit_total'] += amount
            else:
                gl_account_totals[account_code]['credit_total'] += abs(amount)
            
            gl_account_totals[account_code]['net_movement'] = (
                gl_account_totals[account_code]['debit_total'] - 
                gl_account_totals[account_code]['credit_total']
            )
        
        print(f"   📋 GL Account Summaries Built: {len(gl_account_totals)} accounts")
        
        # Verify account-wise balance equation
        balance_equation_passed_count = 0
        balance_equation_failed_count = 0
        total_variance = 0
        failed_accounts = []
        
        for tb_record in trial_balance_records:
            account_code = str(tb_record.gl_account).replace('.0', '')
            
            # Get TB data
            tb_debit = float(tb_record.debit or 0)
            tb_credit = float(tb_record.credit or 0)
            opening_balance = float(getattr(tb_record, 'opening_balance', 0) or 0)
            closing_balance = float(getattr(tb_record, 'closing_balance', 0) or 0)
            
            # Get GL data for this account
            gl_data = gl_account_totals.get(account_code, {
                'debit_total': 0,
                'credit_total': 0,
                'net_movement': 0
            })
            
            # Calculate expected closing balance: Opening + Debits - Credits = Closing
            calculated_closing = opening_balance + tb_debit - tb_credit
            balance_variance = abs(calculated_closing - closing_balance)
            
            # Check if GL movements match TB movements
            gl_vs_tb_debit_variance = abs(gl_data['debit_total'] - tb_debit)
            gl_vs_tb_credit_variance = abs(gl_data['credit_total'] - tb_credit)
            
            # Verification result
            balance_equation_correct = balance_variance < 1.00  # Allow 1 unit variance
            gl_tb_movements_match = (gl_vs_tb_debit_variance + gl_vs_tb_credit_variance) < 10.00
            
            account_passed = balance_equation_correct and gl_tb_movements_match
            total_variance += balance_variance
            
            if account_passed:
                balance_equation_passed_count += 1
            else:
                balance_equation_failed_count += 1
                if len(failed_accounts) < 5:  # Only store first 5 for display
                    failed_accounts.append({
                        'account': account_code,
                        'variance': balance_variance,
                        'gl_debit': gl_data['debit_total'],
                        'tb_debit': tb_debit,
                        'gl_credit': gl_data['credit_total'],
                        'tb_credit': tb_credit
                    })
        
        # Overall Step 2 result
        total_accounts_verified = balance_equation_passed_count + balance_equation_failed_count
        pass_rate = (balance_equation_passed_count / total_accounts_verified) if total_accounts_verified > 0 else 0
        step2_passed = pass_rate >= 0.90  # 90% pass rate required
        
        print(f"   📊 Account Verifications:")
        print(f"      Total Accounts: {total_accounts_verified}")
        print(f"      Passed: {balance_equation_passed_count}")
        print(f"      Failed: {balance_equation_failed_count}")
        print(f"      Pass Rate: {pass_rate:.1%}")
        print(f"      Total Variance: {total_variance:,.2f}")
        print(f"   ✅ STEP 2 RESULT: {'✅ PASS' if step2_passed else '❌ FAIL'}")
        
        if failed_accounts:
            print(f"   ⚠️  Sample Failed Accounts:")
            for acc in failed_accounts:
                print(f"      {acc['account']}: Variance {acc['variance']:.2f} (GL: D{acc['gl_debit']:.0f}/C{acc['gl_credit']:.0f}, TB: D{acc['tb_debit']:.0f}/C{acc['tb_credit']:.0f})")

        # =======================================================================
        # OVERALL RESULT
        # =======================================================================
        
        print(f"\n📊 OVERALL COMPLETENESS ASSESSMENT:")
        print("=" * 60)
        
        # 2-Step Completeness Assessment
        all_steps_passed = step1_passed and step2_passed
        
        # Calculate weighted completeness score (0-100%) for 2 steps
        weights = {
            'step1': 40,  # GL completeness (fundamental)
            'step2': 60   # Account-wise verification (critical)
        }
        
        scores = {
            'step1': 100 if step1_passed else (70 if gl_is_balanced else 30),
            'step2': round(pass_rate * 100, 1) if pass_rate > 0 else 0
        }
        
        completeness_score = sum(scores[step] * weights[step] / 100 for step in weights.keys())
        
        # Determine overall status
        if all_steps_passed and completeness_score >= 95:
            overall_status = 'COMPLETE'
            overall_explanation = 'GL completeness verified: Credit-Debit balanced and all account equations verified.'
        elif completeness_score >= 80:
            overall_status = 'COMPLETE'
            overall_explanation = f'GL mostly complete with minor issues. Score: {completeness_score:.1f}%.'
        else:
            overall_status = 'INCOMPLETE'
            issues = []
            if not step1_passed:
                if not gl_is_balanced:
                    issues.append(f'GL imbalanced by {gl_net_balance:.2f}')
                if not volume_check:
                    issues.append('Insufficient data volume')
                if not data_availability:
                    issues.append('Missing TB data')
            if not step2_passed:
                issues.append(f'{balance_equation_failed_count} accounts failed balance verification')
            overall_explanation = f"GL incomplete (Score: {completeness_score:.1f}%): {', '.join(issues)}"
        
        print(f"🎯 Overall Status: {overall_status}")
        print(f"📈 Completeness Score: {completeness_score:.1f}%")
        print(f"📋 Step 1 (GL Completeness): {'✅ PASS' if step1_passed else '❌ FAIL'}")
        print(f"📋 Step 2 (Account Verification): {'✅ PASS' if step2_passed else '❌ FAIL'}")
        print(f"📝 Explanation: {overall_explanation}")
        
        print(f"\n💡 Score Breakdown:")
        print(f"   Step 1 Weight: {weights['step1']}%, Score: {scores['step1']}% = {scores['step1'] * weights['step1'] / 100:.1f}")
        print(f"   Step 2 Weight: {weights['step2']}%, Score: {scores['step2']}% = {scores['step2'] * weights['step2'] / 100:.1f}")
        print(f"   Final Score: {completeness_score:.1f}%")

    except Engagement.DoesNotExist:
        print(f"❌ Engagement {engagement_id_str} not found in database.")
    except DataFile.DoesNotExist:
        print(f"❌ GL DataFile {gl_file_id_str} not found in database.")
    except Exception as e:
        print(f"💥 An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\n🎯 2-Step completeness test completed.")

if __name__ == '__main__':
    run_2_step_test()
