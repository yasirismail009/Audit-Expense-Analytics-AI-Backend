#!/usr/bin/env python
"""
Script to clean up the tasks.py file by removing everything after the END marker
"""

# Read the file
with open('core/tasks.py', 'r') as f:
    content = f.read()

# Find the END marker
end_marker = "# ============================================================================\n# END OF TASKS\n# ============================================================================"
end_index = content.find(end_marker)

if end_index != -1:
    # Keep everything up to and including the END marker
    clean_content = content[:end_index + len(end_marker)]
    
    # Write the cleaned content back
    with open('core/tasks.py', 'w') as f:
        f.write(clean_content)
    
    print("✅ Successfully cleaned tasks.py file")
    print(f"📊 Original size: {len(content)} characters")
    print(f"📊 Cleaned size: {len(clean_content)} characters")
    print(f"🗑️  Removed: {len(content) - len(clean_content)} characters")
else:
    print("❌ END marker not found")
