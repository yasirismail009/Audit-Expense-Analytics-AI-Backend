#!/usr/bin/env python

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

print("🔍 Debugging CompletenessTestResult import...")

try:
    print("1. Importing models module...")
    import core.models
    print("✅ Models module imported successfully")
    
    print("2. Checking available classes...")
    model_classes = [name for name in dir(core.models) if not name.startswith('_')]
    print(f"📋 Found {len(model_classes)} classes in models module")
    
    if 'CompletenessTestResult' in model_classes:
        print("✅ CompletenessTestResult found in models module")
    else:
        print("❌ CompletenessTestResult NOT found in models module")
        print("Available classes:", model_classes)
    
    print("3. Trying to import CompletenessTestResult directly...")
    from core.models import CompletenessTestResult
    print("✅ CompletenessTestResult imported successfully")
    
    print("4. Checking if it's a proper Django model...")
    print(f"Model name: {CompletenessTestResult.__name__}")
    print(f"Model meta: {CompletenessTestResult._meta}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
