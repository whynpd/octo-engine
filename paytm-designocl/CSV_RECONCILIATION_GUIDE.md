# 📊 CSV Reconciliation System for Freshdesk Migration

This document explains the 4 CSV files created during migration that help you track and reconcile extracted data.

## 🎯 **Purpose**

The CSV reconciliation files provide a **tabular view** of all extracted data, making it easy to:
- **Track progress** during migration
- **Verify data completeness** 
- **Reconcile** JSON files with actual data
- **Audit** the migration process
- **Generate reports** for stakeholders

## 📁 **CSV Files Location**

All CSV files are stored in the `migration/` folder:
```
migration/
├── ticket_attachments_reconciliation.csv
├── conversation_attachments_reconciliation.csv
├── conversations_reconciliation.csv
└── ticket_details_reconciliation.csv
```

## 📊 **1. Ticket Attachments Reconciliation**

**File**: `ticket_attachments_reconciliation.csv`

**Purpose**: Track all attachments directly attached to tickets

**Columns**:
| Column | Description | Example |
|--------|-------------|---------|
| `Ticket_ID` | Freshdesk ticket ID | `26542` |
| `Timestamp_of_ticket` | When ticket was created | `2024-01-15T10:30:00Z` |
| `Attachment_ID` | Freshdesk attachment ID | `12345` |
| `Attachment_Type` | MIME type of attachment | `application/pdf` |
| `Attachment_size` | Size in bytes | `1048576` |
| `Uploaded_by` | User ID who uploaded | `45678` |
| `Attachment_URL` | **Local file path where downloaded** | `attachments/26542/document.pdf` |

**Use Case**: Verify that all ticket attachments were extracted and downloaded

---

## 📊 **2. Conversation Attachments Reconciliation**

**File**: `conversation_attachments_reconciliation.csv`

**Purpose**: Track all attachments in ticket conversations

**Columns**:
| Column | Description | Example |
|--------|-------------|---------|
| `Ticket_ID` | Freshdesk ticket ID | `26542` |
| `Conversation_ID` | Freshdesk conversation ID | `67890` |
| `Timestamp_of_conversation` | When conversation was created | `2024-01-15T11:00:00Z` |
| `Attachment_ID` | Freshdesk attachment ID | `12346` |
| `Attachment_Type` | MIME type of attachment | `image/jpeg` |
| `Attachment_size` | Size in bytes | `512000` |
| `Uploaded_by` | User ID who uploaded | `45679` |
| `Attachment_URL` | **Local file path where downloaded** | `attachments/26542/conv_image.jpg` |

**Use Case**: Verify that all conversation attachments were extracted and downloaded

---

## 📊 **3. Conversations Reconciliation**

**File**: `conversations_reconciliation.csv`

**Purpose**: Track all conversations for each ticket

**Columns**:
| Column | Description | Example |
|--------|-------------|---------|
| `Ticket_ID` | Freshdesk ticket ID | `26542` |
| `Timestamp_of_ticket` | When ticket was created | `2024-01-15T10:30:00Z` |
| `Conversation_ID` | Freshdesk conversation ID | `67890` |
| `Created_by` | User ID who created conversation | `45679` |
| `Comment` | First 500 chars of conversation text | `Customer reported issue...` |
| `Attachment_IDs` | **Semicolon-separated attachment IDs** | `12346;12347` |

**Use Case**: Verify that all conversations were extracted with correct metadata and **see which attachments belong to each conversation**

---

## 📊 **4. Ticket Details Reconciliation**

**File**: `ticket_details_reconciliation.csv`

**Purpose**: Track core ticket information

**Columns**:
| Column | Description | Example |
|--------|-------------|---------|
| `Ticket_ID` | Freshdesk ticket ID | `26542` |
| `Timestamp_of_ticket` | When ticket was created | `2024-01-15T10:30:00Z` |
| `Created_by` | User ID who created ticket | `45678` |
| `Comment` | First 500 chars of ticket description | `Customer needs help with...` |
| `Attachment_IDs` | Semicolon-separated attachment IDs | `12345;12346` |

**Use Case**: Verify that all ticket details were extracted correctly

---

## 🔍 **How to Use for Reconciliation**

### **Step 1: Check Data Completeness**
```bash
# Count records in each CSV
wc -l migration/*.csv

# Expected: Each CSV should have header + data rows
# If ticket_attachments has 81 rows (1 header + 80 tickets), 
# but only 60 have attachments, you know 20 tickets had no attachments
```

### **Step 2: Cross-Reference with JSON Files**
```bash
# Count JSON files created
ls ticket_details/ | wc -l
ls ticket_attachments/ | wc -l
ls conversations/ | wc -l
ls conversation_attachments/ | wc -l

# Compare with CSV record counts
```

### **Step 3: Verify Specific Tickets**
```bash
# Check specific ticket (e.g., 26542)
grep "26542" migration/*.csv

# This shows all records for that ticket across all CSV files
```

### **Step 4: Generate Summary Reports**
```bash
# Count attachments by type
cut -d',' -f4 migration/ticket_attachments_reconciliation.csv | sort | uniq -c

# Count conversations by user
cut -d',' -f4 migration/conversations_reconciliation.csv | sort | uniq -c
```

---

## 📈 **Example Reconciliation Process**

### **Scenario**: Verify ticket 26542 was processed completely

1. **Check Ticket Details**:
   ```bash
   grep "26542" migration/ticket_details_reconciliation.csv
   # Should show: 26542,2024-01-15T10:30:00Z,45678,"Customer issue...",12345;12346
   ```

2. **Check Ticket Attachments**:
   ```bash
   grep "26542" migration/ticket_attachments_reconciliation.csv
   # Should show 2 rows if ticket has 2 attachments
   ```

3. **Check Conversations**:
   ```bash
   grep "26542" migration/conversations_reconciliation.csv
   # Should show rows for each conversation
   ```

4. **Check Conversation Attachments**:
   ```bash
   grep "26542" migration/conversation_attachments_reconciliation.csv
   # Should show rows for conversation attachments
   ```

5. **Verify JSON Files Exist**:
   ```bash
   ls -la ticket_details/ticket_26542_details.json
   ls -la ticket_attachments/ticket_26542_attachments.json
   ls -la conversations/ticket_26542_conversations.json
   ls -la conversation_attachments/ticket_26542_conversation_attachments.json
   ```

6. **Verify Downloaded Files**:
   ```bash
   ls -la attachments/26542/
   # Should show all downloaded attachment files
   ```

---

## 🚨 **Common Reconciliation Issues**

### **Issue 1: Missing Attachments**
- **Symptom**: CSV shows attachment but file not downloaded
- **Cause**: Download failed, URL expired, or file too large
- **Solution**: Check logs for download errors

### **Issue 2: Missing Conversations**
- **Symptom**: Ticket exists but no conversation records
- **Cause**: API error or ticket has no conversations
- **Solution**: Verify API response for that ticket

### **Issue 3: Data Mismatch**
- **Symptom**: CSV count ≠ JSON file count
- **Cause**: Processing error or partial failure
- **Solution**: Re-run migration for failed tickets

---

## 📊 **Sample CSV Output**

### **Default Configuration (Local Paths + Actual File URLs)**
```csv
Ticket_ID,Timestamp_of_ticket,Attachment_ID,Attachment_Type,Attachment_size,Uploaded_by,Attachment_URL
26542,2024-01-15T10:30:00Z,12345,application/pdf,1048576,45678,file:///Users/.../attachments/26542/document.pdf
26542,2024-01-15T10:30:00Z,12346,image/jpeg,512000,45678,file:///Users/.../attachments/26542/screenshot.jpg
26543,2024-01-15T11:00:00Z,12347,application/docx,256000,45679,file:///Users/.../attachments/26543/report.docx
```

### **S3-Style Paths + Actual File URLs**
```csv
Ticket_ID,Timestamp_of_ticket,Attachment_ID,Attachment_Type,Attachment_size,Uploaded_by,Attachment_URL
26542,2024-01-15T10:30:00Z,12345,application/pdf,1048576,45678,s3://freshdesk-attachments/26542/document.pdf
26542,2024-01-15T10:30:00Z,12346,image/jpeg,512000,45678,s3://freshdesk-attachments/26542/screenshot.jpg
26543,2024-01-15T11:00:00Z,12347,application/docx,256000,45679,s3://freshdesk-attachments/26543/report.docx
```

### **conversation_attachments_reconciliation.csv**
```csv
Ticket_ID,Conversation_ID,Timestamp_of_conversation,Attachment_ID,Attachment_Type,Attachment_size,Uploaded_by,Attachment_URL
26542,67890,2024-01-15T14:00:00Z,12348,image/png,128000,45679,file:///Users/.../attachments/26542/conv_error_screenshot.png
26542,67891,2024-01-15T15:00:00Z,12349,application/pdf,512000,45680,file:///Users/.../attachments/26542/conv_solution.pdf
```

### **conversations_reconciliation.csv**
```csv
Ticket_ID,Timestamp_of_ticket,Conversation_ID,Created_by,Comment,Attachment_IDs
26542,2024-01-15T10:30:00Z,67890,45678,Customer reported issue with login,12345;12346
26542,2024-01-15T14:00:00Z,67891,45679,Investigating the issue...,12348
26542,2024-01-15T15:00:00Z,67892,45680,Issue resolved, here's the solution,12349
26543,2024-01-15T11:00:00Z,67893,45680,New feature request,12347
```

---

## 🎯 **Benefits of CSV Reconciliation**

✅ **Immediate Visibility**: See exactly what was extracted  
✅ **Easy Auditing**: Track progress and identify issues  
✅ **Data Validation**: Ensure completeness and accuracy  
✅ **Stakeholder Reports**: Generate summaries for management  
✅ **Troubleshooting**: Quickly identify and fix problems  
✅ **Compliance**: Maintain audit trail of migration process  

---

## 🚀 **Next Steps**

1. **Run Migration**: Execute `python modular_extraction.py`
2. **Monitor Progress**: Watch CSV files populate in real-time
3. **Verify Data**: Use CSV files to cross-reference with JSON files
4. **Generate Reports**: Create summary reports from CSV data
5. **Archive**: Keep CSV files for future reference and auditing

---

**🎉 Your CSV reconciliation system is ready to provide complete visibility into the migration process!** 

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
    'rate_limit_requests_per_hour': 10000, # API rate limit
    'csv_show_local_paths': True,         # True: local paths, False: S3-style paths
    'csv_include_original_urls': False    # False: actual file URLs, True: Freshdesk S3 URLs
}
```

### **File Path Configuration Options:**

#### **`csv_show_local_paths`**
- **`True`**: Shows local file system paths (e.g., `attachments/26542/document.pdf`)
- **`False`**: Shows S3-style paths (e.g., `s3://freshdesk-attachments/26542/document.pdf`)

#### **`csv_include_original_urls`**
- **`False`**: Shows actual downloaded file URLs (e.g., `file:///path/to/attachments/26542/document.pdf`)
- **`True`**: Shows original Freshdesk S3 URLs (e.g., `https://s3.amazonaws.com/...`)

### **Example Configurations:**

#### **Configuration 1: Local Paths + Actual File URLs (Default)**
```python
'csv_show_local_paths': True,
'csv_include_original_urls': False
```
**Result**: `file:///Users/.../attachments/26542/document.pdf`

#### **Configuration 2: S3-Style Paths + Actual File URLs**
```python
'csv_show_local_paths': False,
'csv_include_original_urls': False
```
**Result**: `s3://freshdesk-attachments/26542/document.pdf`

#### **Configuration 3: Local Paths + Freshdesk S3 URLs**
```python
'csv_show_local_paths': True,
'csv_include_original_urls': True
```
**Result**: `https://s3.amazonaws.com/freshdesk/...`

#### **Configuration 4: S3-Style Paths + Freshdesk S3 URLs**
```python
'csv_show_local_paths': False,
'csv_include_original_urls': True
```
**Result**: `https://s3.amazonaws.com/freshdesk/...` 