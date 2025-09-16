#!/usr/bin/env python3
"""
Test script for AmountParser functionality
Demonstrates parsing of various amount formats commonly found in audit data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the AmountParser from the models
from core.models import AmountParser

def test_amount_parsing():
    """Test various amount formats"""
    
    test_cases = [
        # Basic formats
        ("1234.56", "1234.56"),
        ("1,234.56", "1234.56"),
        ("1.234,56", "1234.56"),  # European format
        ("1 234.56", "1234.56"),  # Space separator
        
        # Negative formats - YOUR EXAMPLE
        ("-3264327.90", "-3264327.90"),  # Your specific example
        ("(1,234.56)", "-1234.56"),     # Parentheses
        ("1,234.56-", "-1234.56"),      # Trailing minus
        
        # Currency formats
        ("SAR 1,234.56", "1234.56"),
        ("1,234.56 SAR", "1234.56"),
        ("$1,234.56", "1234.56"),
        ("€1.234,56", "1234.56"),
        ("ريال 1,234.56", "1234.56"),
        
        # Large amounts
        ("1,234,567.89", "1234567.89"),
        ("1.234.567,89", "1234567.89"),
        ("-10,000,000.00", "-10000000.00"),
        
        # Edge cases
        ("", None),
        ("NULL", None),
        ("N/A", None),
        ("-", None),
        ("0", "0"),
        ("0.00", "0.00"),
        
        # Complex cases
        ("(SAR 5,432.10)", "-5432.10"),
        ("USD -1,000.50", "-1000.50"),
        
        # Real-world examples
        ("123,456,789.12", "123456789.12"),
        ("-987,654.32", "-987654.32"),
        ("(100.00)", "-100.00"),
        ("50,000-", "-50000.00"),
    ]
    
    print("=== Amount Parsing Test Results ===\n")
    print(f"{'Input':<25} {'Expected':<15} {'Parsed':<15} {'Status'}")
    print("-" * 70)
    
    passed = 0
    failed = 0
    
    for input_amount, expected in test_cases:
        try:
            parsed = AmountParser.parse_amount(input_amount)
            parsed_str = str(parsed) if parsed is not None else "None"
            expected_str = expected if expected is not None else "None"
            
            if parsed_str == expected_str:
                status = "✅ PASS"
                passed += 1
            else:
                status = "❌ FAIL"
                failed += 1
            
            print(f"{input_amount:<25} {expected_str:<15} {parsed_str:<15} {status}")
            
        except Exception as e:
            print(f"{input_amount:<25} {expected_str:<15} {'ERROR':<15} ❌ FAIL ({e})")
            failed += 1
    
    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    
    # Test debit/credit parsing
    print("\n=== Debit/Credit Parsing ===\n")
    debit_credit_tests = [
        ("1000.00", (1000.00, None)),
        ("-500.00", (None, 500.00)),
        ("(250.75)", (None, 250.75)),
        ("-3264327.90", (None, 3264327.90)),  # Your example
    ]
    
    for amount_str, expected in debit_credit_tests:
        debit, credit = AmountParser.parse_debit_credit_amounts(amount_str)
        print(f"Amount: {amount_str}")
        print(f"  Debit: {debit}")
        print(f"  Credit: {credit}")
        print(f"  Expected: Debit={expected[0]}, Credit={expected[1]}")
        print()

def test_validation():
    """Test amount validation"""
    print("=== Amount Validation Test ===\n")
    
    validation_tests = [
        "1,234.56",
        "-3264327.90",  # Your example
        "(1,000.00)",
        "invalid_amount",
        "SAR 500.00",
        "",
    ]
    
    for amount in validation_tests:
        is_valid, error = AmountParser.validate_amount_format(amount)
        status = "✅ Valid" if is_valid else f"❌ Invalid: {error}"
        print(f"{amount:<20} {status}")

def test_formatting():
    """Test amount formatting"""
    print("\n=== Amount Formatting Test ===\n")
    
    from decimal import Decimal
    
    amounts = [
        Decimal("1234.56"),
        Decimal("-3264327.90"),  # Your example
        Decimal("0.00"),
        None,
    ]
    
    for amount in amounts:
        formatted = AmountParser.format_amount(amount, currency='SAR')
        formatted_no_currency = AmountParser.format_amount(amount, include_currency=False)
        print(f"Amount: {amount}")
        print(f"  With currency: {formatted}")
        print(f"  Without currency: {formatted_no_currency}")
        print()

if __name__ == "__main__":
    print("Testing AmountParser with your example: -3264327.90\n")
    
    # Test your specific example first
    your_example = "-3264327.90"
    parsed = AmountParser.parse_amount(your_example)
    debit, credit = AmountParser.parse_debit_credit_amounts(your_example)
    
    print(f"Your example: {your_example}")
    print(f"Parsed amount: {parsed}")
    print(f"Transaction type: {'DEBIT' if parsed and parsed > 0 else 'CREDIT' if parsed and parsed < 0 else 'UNKNOWN'}")
    print(f"Debit amount: {debit}")
    print(f"Credit amount: {credit}")
    print()
    
    # Run all tests
    test_amount_parsing()
    test_validation()
    test_formatting()
