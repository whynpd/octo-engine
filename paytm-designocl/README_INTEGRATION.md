# 🚀 Freshdesk to JIRA Migration Tool - Integrated System

This directory contains an integrated system for migrating Freshdesk tickets to JIRA with enhanced functionality and organized data extraction.

## 📁 **File Structure**

```
paytm-designocl/
├── modular_extraction.py          # Main coordinator (4-JSON structure)
├── extract_ticket_details.py      # Step 2: Extract ticket details & conversations
├── download_attachments.py        # Step 3: Download all attachments
├── Sample Data from Design OCL - Sheet1.csv  # Input CSV with ticket IDs
├── test_integration.py            # Test script to verify integration
└── README_INTEGRATION.md          # This file
```

## 🔧 **How It Works**

### **1. Data Flow**
```
CSV File → modular_extraction.py → 4 JSON Files per Ticket + Attachments
```

### **2. Processing Steps**
- **Step 1**: Read ticket IDs from CSV file
- **Step 2**: Extract detailed ticket information and create 4 JSON files per ticket
- **Step 3**: Download all attachments

### **3. Output Structure**
For each ticket, 4 separate JSON files are created (only if data exists):

```
ticket_details/           # Core ticket information
├── ticket_26542_details.json
├── ticket_26543_details.json
└── ...

ticket_attachments/       # Ticket-level attachments
├── ticket_26542_attachments.json
├── ticket_26543_attachments.json
└── ...

conversations/            # Ticket conversations
├── ticket_26542_conversations.json
├── ticket_26543_conversations.json
└── ...

conversation_attachments/ # Conversation-level attachments
├── ticket_26542_conversation_attachments.json
├── ticket_26543_conversation_attachments.json
└── ...

attachments/              # Downloaded files
├── 26542/
│   ├── file1.pdf
│   └── file2.jpg
├── 26543/
│   └── file3.docx
└── ...
```

## 🚀 **Quick Start**

### **1. Set Environment Variables**
```bash
export FRESHDESK_DOMAIN="your-domain.freshdesk.com"
export FRESHDESK_API_KEY="your-api-key"
```

### **2. Test Integration**
```bash
cd paytm-designocl
python test_integration.py
```

### **3. Run Migration**
```bash
python modular_extraction.py
```

## 📊 **CSV Input Format**

The system expects a CSV file with this structure:
```csv
Ticket ID,Agent name,Status,Priority,Source
26542,Divya Sharma,1. Not Picked,urgent,Portal
26543,Subhash Pathak,Closed,urgent,Portal
26544,Govind Dimri,7. Business Approval Pending,urgent,Portal
...
```

## 🔑 **Key Features**

### **✅ Enhanced Data Extraction**
- **User details**: `requester_id`, `responder_id`, `user_id`
- **Conversation metadata**: `created_by`, `incoming`, `outgoing`, `source`, `thread_id`
- **Attachment information**: `content_type`, `size`, `created_at`

### **✅ Conditional File Creation**
- JSON files are only created when data exists
- No empty files for tickets without attachments/conversations

### **✅ Rate Limiting & Error Handling**
- Built-in rate limiting between API requests
- Retry logic for failed requests
- Comprehensive error logging

### **✅ Organized Output**
- Separate directories for different data types
- Consistent file naming convention
- Easy to process for JIRA migration

## 🧪 **Testing**

Run the test script to verify everything works:
```bash
python test_integration.py
```

This will test:
- Module imports
- CSV file reading
- MigrationCoordinator creation

## 📝 **Configuration**

Key configuration options in `modular_extraction.py`:
```python
CONFIG = {
    'batch_size': 100,                    # Process 100 tickets per batch
    'delay_between_requests': 0.01,       # 0.01 seconds between API calls
    'delay_between_batches': 10,          # 10 seconds between batches
    'max_retries': 3,                     # Retry failed requests 3 times
    'save_interval': 10,                  # Save progress every 10 batches
    'resume_enabled': True,               # Enable resuming from failures
    'rate_limit_requests_per_hour': 10000 # API rate limit
}
```

## 🚨 **Troubleshooting**

### **Common Issues**

1. **Import Errors**: Ensure all files are in the same directory
2. **CSV Not Found**: Check the CSV filename matches exactly
3. **Authentication Errors**: Verify FRESHDESK_DOMAIN and FRESHDESK_API_KEY
4. **Rate Limiting**: Increase delays in CONFIG if hitting API limits

### **Logs**
- Check `modular_migration.log` for detailed execution logs
- Check `migration_summary.json` for final results summary

## 🎯 **Next Steps**

After successful extraction:
1. **Review JSON files** to ensure data quality
2. **Process attachments** for JIRA upload
3. **Map Freshdesk fields** to JIRA fields
4. **Upload to JIRA** using JIRA REST API

## 📞 **Support**

If you encounter issues:
1. Check the logs for error details
2. Verify environment variables are set correctly
3. Ensure CSV file format matches expected structure
4. Test with a small subset of tickets first

---

**🎉 Your integrated migration system is ready to use!** 