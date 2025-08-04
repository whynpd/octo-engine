# Freshdesk to Jira ITSM Migration Tool

A comprehensive, configurable migration tool to transfer tickets, comments, attachments, and user details from Freshdesk ITSM to Jira ITSM. Built to handle large-scale migrations with enterprise-grade features.

## 🎯 **Overview**

This tool is designed to migrate data from multiple Freshdesk instances to Jira ITSM, supporting:
- **500,000+ tickets** with efficient batch processing
- **Multiple Freshdesk instances** with different configurations
- **Complete data transfer** including tickets, comments, attachments, users, and custom fields
- **Enterprise-grade features** with robust error handling and progress tracking

## 📁 **Project Structure**

```
octo-engine/
├── config/
│   └── migration_config.yaml      # Main configuration file
├── docs/
│   └── MIGRATION_GUIDE.md         # Comprehensive migration guide
├── scripts/
│   ├── setup_migration.py         # Interactive setup script
│   └── quick_start.py             # Quick start and testing script
├── src/
│   ├── main.py                    # Main CLI entry point
│   ├── adapters/                  # API adapters
│   │   ├── freshdesk_adapter.py   # Freshdesk API integration
│   │   └── jira_adapter.py        # Jira API integration
│   ├── core/                      # Core migration logic
│   │   ├── config_manager.py      # Configuration management
│   │   ├── migration_manager.py   # Main migration orchestration
│   │   ├── status_manager.py      # Progress tracking
│   │   └── analyzer.py            # Data analysis
│   ├── mappers/                   # Data transformation
│   │   └── data_mapper.py         # Freshdesk to Jira mapping
│   └── utils/                     # Utilities
│       ├── logger.py              # Logging configuration
│       └── validator.py           # Configuration validation
├── logs/                          # Migration logs (created during runtime)
├── data/                          # Temporary data storage (created during runtime)
├── README.md                      # Project overview
└── requirements.txt               # Python dependencies
```

## 🚀 **Quick Start**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Configure Your Migration**
You have two options:

**Option A: Interactive Setup (Recommended)**
```bash
python scripts/setup_migration.py
```

**Option B: Manual Configuration**
Edit `config/migration_config.yaml` with your details:
- Freshdesk URLs and API keys
- Jira URLs and API tokens
- Field mappings
- Migration settings

### **3. Test Your Setup**
```bash
python scripts/quick_start.py
```

### **4. Run the Migration Process**

**Step 1: Validate Configuration**
```bash
python src/main.py validate
```

**Step 2: Analyze Your Data**
```bash
python src/main.py analyze --limit 1000
```

**Step 3: Dry Run (Test without creating data)**
```bash
python src/main.py migrate --dry-run
```

**Step 4: Full Migration**
```bash
python src/main.py migrate
```

## 🔧 **Key Features Built**

### **✅ Configurable for Multiple Instances**
- Support for multiple Freshdesk instances
- Different configurations per instance
- Batch processing for large datasets
- Instance-specific field mappings

### **✅ Complete Data Migration**
- **Tickets**: All ticket data with custom fields
- **Comments**: Full conversation history with HTML to Jira format conversion
- **Attachments**: File downloads and uploads with size/type filtering
- **Users**: User creation and mapping with role preservation
- **Custom Fields**: Flexible field mapping with validation

### **✅ Enterprise-Grade Features**
- **Rate Limiting**: Respects API limits for both Freshdesk and Jira
- **Error Handling**: Robust retry logic with exponential backoff
- **Progress Tracking**: Real-time status updates and ETA calculations
- **Checkpointing**: Resume interrupted migrations from last checkpoint
- **Logging**: Comprehensive audit trail with rotation and compression

### **✅ Performance Optimized**
- **Batch Processing**: Configurable batch sizes (default: 100 tickets)
- **Memory Management**: Efficient data handling for large datasets
- **Concurrent Operations**: Parallel processing where possible
- **Large Dataset Support**: Handles 500k+ tickets with optimized memory usage

### **✅ User-Friendly**
- **CLI Interface**: Easy-to-use commands with help and progress bars
- **Interactive Setup**: Guided configuration with validation
- **Status Monitoring**: Real-time progress with detailed statistics
- **Comprehensive Logging**: Detailed error reporting and debugging

### **✅ Data Transformation**
- **HTML to Jira Format**: Automatic conversion of HTML content
- **Field Mapping**: Flexible priority, status, and custom field mappings
- **User Mapping**: Intelligent user creation and assignment
- **Attachment Processing**: Size validation and type filtering

## 📋 **Prerequisites**

### **System Requirements**
- Python 3.8 or higher
- Minimum 4GB RAM (8GB recommended for large migrations)
- Sufficient disk space for attachments and logs
- Network access to both Freshdesk and Jira instances

### **API Access Requirements**
- **Freshdesk Enterprise**: API access with appropriate permissions
- **Jira Premium/Enterprise**: API access with admin permissions for user creation
- Valid API keys/tokens for both systems

### **Required Permissions**
- **Freshdesk**: Read access to tickets, users, comments, and attachments
- **Jira**: Create issues, users, comments, and attachments

## ⚙️ **Configuration**

### **Freshdesk Configuration**
```yaml
freshdesk:
  instances:
    - name: "production"
      url: "https://your-company.freshdesk.com"
      api_key: "your_api_key"
      rate_limit: 100
      timeout: 30
      batch_size: 100
```

### **Jira Configuration**
```yaml
jira:
  url: "https://your-company.atlassian.net"
  username: "your_email@company.com"
  api_token: "your_api_token"
  project_key: "ITSM"
  issue_type: "Incident"
  rate_limit: 100
  timeout: 30
  batch_size: 50
```

### **Field Mapping**
```yaml
field_mapping:
  priority:
    "low": "Low"
    "medium": "Medium"
    "high": "High"
    "urgent": "Highest"
  
  status:
    "open": "To Do"
    "pending": "In Progress"
    "resolved": "Done"
    "closed": "Done"
  
  custom_fields:
    "category": "components"
    "department": "customfield_10001"
```

## 🛠️ **Available Commands**

```bash
# Main commands
python src/main.py migrate          # Execute migration
python src/main.py validate         # Validate configuration
python src/main.py analyze          # Analyze data structure
python src/main.py status           # Show migration status
python src/main.py resume           # Resume interrupted migration

# Options
python src/main.py migrate --dry-run                    # Test without creating data
python src/main.py migrate --instance instance1         # Migrate specific instance
python src/main.py analyze --limit 1000                 # Analyze with limit
python src/main.py --help                              # Show all options
```

## 📊 **Migration Components**

### **What Gets Migrated**

#### **1. Tickets/Issues**
- **Subject** → Summary
- **Description** → Description (HTML converted to Jira format)
- **Priority** → Priority (mapped)
- **Status** → Status (mapped)
- **Assignee** → Assignee (user mapping)
- **Reporter** → Reporter (user mapping)
- **Created Date** → Created
- **Updated Date** → Updated
- **Custom Fields** → Custom Fields (mapped)

#### **2. Comments**
- **Comment Body** → Comment (HTML converted)
- **Author** → Author (user mapping)
- **Created Date** → Created
- **Private/Public** → Internal/Public

#### **3. Attachments**
- **File Name** → File Name
- **File Content** → File Content
- **File Size** → File Size (with configurable limits)
- **Content Type** → Content Type

#### **4. Users**
- **Name** → Display Name
- **Email** → Email Address
- **Active Status** → Active Status
- **Role** → Role (mapped)

## 🔍 **Data Analysis Features**

The tool includes comprehensive data analysis capabilities:

- **Ticket Distribution**: Status, priority, and type analysis
- **User Analysis**: Active/inactive users, role distribution
- **Custom Field Mapping**: Field usage and mapping recommendations
- **Attachment Analysis**: File types, sizes, and storage requirements
- **Migration Recommendations**: Automated suggestions for optimal configuration

## 📈 **Performance & Monitoring**

### **Batch Processing**
- Configurable batch sizes (default: 100 tickets)
- Memory-efficient processing for large datasets
- Progress tracking with ETA calculations

### **Rate Limiting**
- Respects API limits for both systems
- Configurable rate limits per instance
- Automatic retry with exponential backoff

### **Checkpointing**
- Automatic progress saving
- Resume capability from any interruption
- Configurable checkpoint intervals

### **Monitoring**
- Real-time progress updates
- Success/failure statistics
- Performance metrics
- Error tracking and reporting

## 🚨 **Error Handling**

### **Retry Logic**
- Automatic retry for failed requests
- Exponential backoff strategy
- Configurable retry count and delays

### **Error Logging**
- Detailed error logging with context
- Error categorization and reporting
- Failed item tracking for manual review

### **Continue on Error**
- Option to continue despite individual failures
- Success rate calculation and reporting
- Comprehensive error summary

## 📚 **Documentation**

- **[Migration Guide](docs/MIGRATION_GUIDE.md)**: Comprehensive step-by-step guide
- **Configuration Examples**: Sample configurations for different scenarios
- **Troubleshooting Guide**: Common issues and solutions
- **Best Practices**: Migration planning and execution tips

## 🆘 **Support & Troubleshooting**

### **Common Issues**
1. **API Authentication Errors**: Verify API keys and permissions
2. **Rate Limiting Issues**: Adjust rate limits in configuration
3. **Large Attachment Issues**: Configure size limits and file type filters
4. **Memory Issues**: Reduce batch sizes for large datasets

### **Getting Help**
1. Check the logs at `logs/migration.log`
2. Review the migration report at `data/migration_report.json`
3. Consult the troubleshooting section in the migration guide
4. Use the validation command to check configuration

## 🤝 **Contributing**

This tool is designed to be extensible and configurable. Key areas for customization:

- **Custom Field Mappings**: Add new field mappings in configuration
- **Data Transformations**: Modify the data mapper for custom logic
- **API Adapters**: Extend adapters for additional API features
- **Validation Rules**: Add custom validation logic

## 📄 **License**

This project is designed for enterprise use. Please ensure compliance with your organization's policies and the terms of service for both Freshdesk and Jira.

---

## 🎉 **Ready to Start?**

Your Freshdesk to Jira ITSM migration tool is now ready! Start with:

```bash
python scripts/setup_migration.py
```

This will guide you through the entire configuration process and get you ready for a successful migration.

For detailed instructions, see the [Migration Guide](docs/MIGRATION_GUIDE.md). 