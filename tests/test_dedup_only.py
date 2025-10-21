"""
Simple unit test untuk DedupStore.mark_processed_if_new()
Jalankan: python -m pytest tests/test_dedup_only.py -v -s
Atau: python tests/test_dedup_only.py
"""
import os
import sys
import tempfile
import shutil

# Only needed if running directly (not via pytest)
if __name__ == '__main__':
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dedup_store import DedupStore
from src.util import iso_now

def test_mark_processed_if_new():
    # Create temp dir
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, 'test.db')
    
    print(f"📁 Test DB: {db_path}")
    
    try:
        # Create store
        store = DedupStore(db_path)
        
        # Test 1: Insert event pertama (harus return True)
        result1 = store.mark_processed_if_new('test-topic', 'evt-1', iso_now())
        print(f"✅ First insert result: {result1}")
        assert result1 == True, f"Expected True, got {result1}"
        
        # Test 2: Insert duplikat (harus return False)
        result2 = store.mark_processed_if_new('test-topic', 'evt-1', iso_now())
        print(f"✅ Duplicate insert result: {result2}")
        assert result2 == False, f"Expected False, got {result2}"
        
        # Test 3: Insert event baru lagi (harus return True)
        result3 = store.mark_processed_if_new('test-topic', 'evt-2', iso_now())
        print(f"✅ Second new insert result: {result3}")
        assert result3 == True, f"Expected True, got {result3}"
        
        # Verify database
        rows = store.list_processed('test-topic')
        print(f"💾 DB contains {len(rows)} processed events")
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        
        store.close()
        print("✅ All tests passed!")
        
    finally:
        # Cleanup
        shutil.rmtree(tmpdir)

if __name__ == '__main__':
    test_mark_processed_if_new()