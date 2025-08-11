import gzip
import json
import os
from pathlib import Path

def read_compressed_json(filename):
    """Read a compressed JSON file"""
    try:
        with gzip.open(filename, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        return None
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None

def decompress_batch_files():
    """Decompress all batch files in the enhanced_migration/batches directory"""
    batches_dir = Path("enhanced_migration/batches")
    
    if not batches_dir.exists():
        print(f"❌ Batches directory not found: {batches_dir}")
        return
    
    # Find all .gz files
    gz_files = list(batches_dir.glob("*.gz"))
    
    if not gz_files:
        print(f"❌ No .gz files found in {batches_dir}")
        return
    
    print(f"📁 Found {len(gz_files)} compressed batch files:")
    
    for gz_file in gz_files:
        print(f"\n--- Processing {gz_file.name} ---")
        
        # Read the compressed data
        data = read_compressed_json(gz_file)
        
        if data:
            print(f"✅ Successfully loaded {len(data)} tickets from {gz_file.name}")
            
            # Show first ticket info
            if isinstance(data, list) and len(data) > 0:
                first_ticket = data[0]
                print(f"📋 First ticket ID: {first_ticket.get('id', 'N/A')}")
                print(f"📝 Subject: {first_ticket.get('subject', 'N/A')}")
                print(f"📊 Status: {first_ticket.get('status', 'N/A')}")
                
                # Check for conversations
                conversations = first_ticket.get('conversations', [])
                print(f"💬 Conversations: {len(conversations)}")
                
                # Check for attachments
                attachments = first_ticket.get('attachments', [])
                print(f"📎 Attachments: {len(attachments)}")
            
            # Decompress to readable file
            output_file = gz_file.with_suffix('.json')
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"💾 Decompressed to: {output_file}")
            except Exception as e:
                print(f"❌ Error saving decompressed file: {e}")

def read_specific_batch(batch_number):
    """Read a specific batch file by number"""
    filename = f"enhanced_migration/batches/batch_{batch_number:03d}.json.gz"
    
    print(f"📖 Reading {filename}...")
    data = read_compressed_json(filename)
    
    if data:
        print(f"✅ Loaded {len(data)} tickets from batch {batch_number}")
        return data
    else:
        print(f"❌ Failed to load batch {batch_number}")
        return None

if __name__ == "__main__":
    print("🔍 Batch File Decompression Tool")
    print("=" * 40)
    
    # Option 1: Decompress all batch files
    print("\n1. Decompressing all batch files...")
    decompress_batch_files()
    
    # Option 2: Read specific batch (batch_001.json.gz)
    print("\n2. Reading specific batch (batch_001.json.gz)...")
    batch_001_data = read_specific_batch(1)
    
    if batch_001_data:
        print(f"📊 Batch 001 contains {len(batch_001_data)} tickets")
        
        # Show sample data structure
        if len(batch_001_data) > 0:
            sample_ticket = batch_001_data[0]
            print(f"\n📋 Sample ticket structure:")
            print(f"   ID: {sample_ticket.get('id')}")
            print(f"   Subject: {sample_ticket.get('subject')}")
            print(f"   Status: {sample_ticket.get('status')}")
            print(f"   Priority: {sample_ticket.get('priority')}")
            print(f"   Created: {sample_ticket.get('created_at')}")
            
            # Show available fields
            print(f"\n🔍 Available fields: {list(sample_ticket.keys())}")
    
    print("\n✅ Decompression complete!") 