# Freshdesk to Jira ITSM Migration Guide

This guide provides step-by-step instructions for migrating from Freshdesk ITSM to Jira ITSM using the migration tool.

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Minimum 4GB RAM (8GB recommended for large migrations)
- Sufficient disk space for attachments and logs
- Network access to both Freshdesk and Jira instances

### API Access Requirements
- **Freshdesk Enterprise**: API access with appropriate permissions
- **Jira Premium/Enterprise**: API access with admin permissions for user creation
- Valid API keys/tokens for both systems

### Required Permissions
- **Freshdesk**: Read access to tickets, users, comments, and attachments
- **Jira**: Create issues, users, comments, and attachments

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd octo-engine
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation**:
   ```bash
   python src/main.py --help
   ```

## Configuration

### Step 1: Run Setup Script
Use the interactive setup script to configure your migration:

```bash
python scripts/setup_migration.py
```

The script will guide you through:
- Freshdesk instance configuration
- Jira instance configuration
- Migration settings
- Field mappings

### Step 2: Manual Configuration (Optional)
If you prefer to configure manually, edit `config/migration_config.yaml`:

```yaml
freshdesk:
  instances:
    - name: "production"
      url: "https://your-company.freshdesk.com"
      api_key: "your_api_key"
      rate_limit: 100
      timeout: 30
      batch_size: 100

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

### Step 3: Field Mapping Configuration
Configure how Freshdesk fields map to Jira fields:

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

## Pre-Migration Steps

### Step 1: Validate Configuration
Test your configuration and connectivity:

```bash
python src/main.py validate
```

This will:
- Validate configuration syntax
- Test connectivity to Freshdesk and Jira
- Check API permissions
- Verify field mappings

### Step 2: Analyze Data
Analyze your Freshdesk data structure:

```bash
python src/main.py analyze --limit 1000
```

This will:
- Analyze ticket distribution
- Identify custom fields
- Check user data
- Analyze attachments
- Generate recommendations

### Step 3: Review Analysis Results
Check the generated analysis report at `data/analysis_results.json` and review:
- Total number of tickets to migrate
- Custom field mappings needed
- Attachment storage requirements
- User migration strategy

## Migration Process

### Step 1: Dry Run
Test the migration without creating actual data:

```bash
python src/main.py migrate --dry-run
```

This will:
- Process all tickets without creating Jira issues
- Validate data mapping
- Check for potential issues
- Generate a report of what would be migrated

### Step 2: Full Migration
Execute the complete migration:

```bash
python src/main.py migrate
```

For specific instances:
```bash
python src/main.py migrate --instance instance1
```

### Step 3: Monitor Progress
Check migration status:

```bash
python src/main.py status
```

### Step 4: Resume if Needed
If migration is interrupted, resume from checkpoint:

```bash
python src/main.py resume
```

## Migration Components

### What Gets Migrated

#### 1. Tickets/Issues
- **Subject** → Summary
- **Description** → Description (HTML converted to Jira format)
- **Priority** → Priority (mapped)
- **Status** → Status (mapped)
- **Assignee** → Assignee (user mapping)
- **Reporter** → Reporter (user mapping)
- **Created Date** → Created
- **Updated Date** → Updated
- **Custom Fields** → Custom Fields (mapped)

#### 2. Comments
- **Comment Body** → Comment (HTML converted)
- **Author** → Author (user mapping)
- **Created Date** → Created
- **Private/Public** → Internal/Public

#### 3. Attachments
- **File Name** → File Name
- **File Content** → File Content
- **File Size** → File Size (with limits)
- **Content Type** → Content Type

#### 4. Users
- **Name** → Display Name
- **Email** → Email Address
- **Active Status** → Active Status
- **Role** → Role (mapped)

### Data Transformation

#### HTML to Jira Format
The tool automatically converts HTML content to Jira format:
- `<strong>` → `*text*`
- `<em>` → `_text_`
- `<h1>` → `h1. text`
- `<ul><li>` → `* text`
- `<code>` → `{{text}}`

#### Field Mapping
- Priority levels are mapped according to configuration
- Status values are mapped to Jira workflow states
- Custom fields are mapped using field IDs

## Performance Optimization

### Batch Processing
- Tickets are processed in configurable batches
- Default batch size: 100 tickets
- Adjust based on system performance

### Rate Limiting
- Configurable rate limits for both Freshdesk and Jira
- Default: 100 requests per minute
- Adjust based on API limits

### Memory Management
- Large datasets are processed in chunks
- Attachments are downloaded temporarily
- Memory usage is monitored and controlled

### Checkpointing
- Progress is saved at configurable intervals
- Migration can be resumed from last checkpoint
- Default checkpoint interval: 1000 tickets

## Error Handling

### Retry Logic
- Failed requests are retried automatically
- Exponential backoff strategy
- Configurable retry count and delay

### Error Logging
- All errors are logged with details
- Error summary in migration report
- Failed items are tracked separately

### Continue on Error
- Option to continue migration despite errors
- Failed tickets are logged for manual review
- Success rate is calculated and reported

## Post-Migration Steps

### Step 1: Verify Migration
Check the migration report at `data/migration_report.json`:
- Total tickets migrated
- Success rate
- Error summary
- Duration and performance metrics

### Step 2: Validate Data
Manually verify a sample of migrated tickets:
- Check ticket content and formatting
- Verify attachments
- Confirm user assignments
- Validate custom field values

### Step 3: Update Workflows
Configure Jira workflows to match your business process:
- Set up status transitions
- Configure automation rules
- Update notification settings

### Step 4: User Training
Train users on the new Jira interface:
- Show migrated data structure
- Explain field mappings
- Demonstrate new features

## Troubleshooting

### Common Issues

#### 1. API Authentication Errors
**Symptoms**: 401/403 errors in logs
**Solutions**:
- Verify API keys/tokens
- Check user permissions
- Ensure correct URLs

#### 2. Rate Limiting Issues
**Symptoms**: 429 errors, slow migration
**Solutions**:
- Reduce rate limits in configuration
- Increase delays between requests
- Contact API provider for limits

#### 3. Large Attachment Issues
**Symptoms**: Timeout errors, disk space issues
**Solutions**:
- Reduce attachment size limits
- Filter by file type
- Increase timeout values

#### 4. Memory Issues
**Symptoms**: Out of memory errors
**Solutions**:
- Reduce batch sizes
- Increase memory limits
- Process in smaller chunks

### Debug Mode
Enable debug logging for detailed troubleshooting:

```yaml
logging:
  level: "DEBUG"
```

### Log Analysis
Check logs at `logs/migration.log` for:
- Error details
- Performance metrics
- API response codes
- Data transformation issues

## Best Practices

### Before Migration
1. **Backup Data**: Ensure Freshdesk data is backed up
2. **Test Environment**: Use test instances first
3. **Plan Downtime**: Schedule migration during low usage
4. **Notify Users**: Inform stakeholders of migration

### During Migration
1. **Monitor Progress**: Check status regularly
2. **Watch Logs**: Monitor for errors and warnings
3. **Validate Sample**: Check migrated data periodically
4. **Have Rollback Plan**: Be prepared to revert if needed

### After Migration
1. **Verify Data**: Thoroughly check migrated content
2. **Update Documentation**: Update process documentation
3. **Train Users**: Provide training on new system
4. **Monitor Performance**: Watch for issues in new system

## Support

### Getting Help
1. Check the logs for error details
2. Review the migration report
3. Consult the troubleshooting section
4. Contact support with specific error messages

### Reporting Issues
When reporting issues, include:
- Configuration file (with sensitive data removed)
- Error logs
- Migration report
- System information
- Steps to reproduce

## Advanced Configuration

### Custom Field Mapping
For complex custom fields, use field IDs:

```yaml
custom_fields:
  "category": "customfield_10001"
  "priority": "customfield_10002"
  "department": "components"
```

### Filtering Options
Filter tickets by various criteria:

```yaml
ticket_filters:
  created_after: "2023-01-01"
  created_before: "2024-12-31"
  statuses: ["open", "pending"]
  priorities: ["high", "urgent"]
  exclude_ids: [12345, 67890]
```

### Performance Tuning
Optimize for your environment:

```yaml
performance:
  max_concurrent_requests: 20
  max_memory_usage_mb: 4096
  connection_pool_size: 50
```

This completes the migration guide. Follow these steps carefully to ensure a successful migration from Freshdesk to Jira ITSM. 