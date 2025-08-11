# modular_extraction.py - Modular sequential extraction system
import time
import logging
import json
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import gzip

# Import functions from individual step files in the same directory
from extract_ticket_details import process_ticket_batch, read_ticket_ids_from_csv, get_ticket_details, fetch_conversations
from download_attachments import download_attachments_batch

# Import additional functions from complete_extraction.py
import requests
import os
from pathlib import Path

# Load environment variables
load_dotenv()

# Configuration for sequential migration
CONFIG = {
    'batch_size': 100,
    'delay_between_requests': 0.01,
    'delay_between_batches': 10,
    'max_retries': 3,
    'save_interval': 10,
    'resume_enabled': True,
    'rate_limit_requests_per_hour': 10000
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('modular_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MigrationCoordinator:
    """Coordinates sequential migration process with 4 separate JSON files per ticket"""
    
    def __init__(self):
        self.ticket_ids = []
        self.all_tickets = []
        self.attachments = []
        self.errors = []
        
        # Create output directories
        self.output_dirs = {
            'ticket_details': Path('ticket_details'),
            'ticket_attachments': Path('ticket_attachments'),
            'conversations': Path('conversations'),
            'conversation_attachments': Path('conversation_attachments')
        }
        
        for dir_path in self.output_dirs.values():
            dir_path.mkdir(exist_ok=True)
        
        # Get credentials from environment variables
        self.FRESHDESK_DOMAIN = os.getenv("FRESHDESK_DOMAIN")
        self.API_KEY = os.getenv("FRESHDESK_API_KEY")
        
        # Validate that required environment variables are set
        if not self.FRESHDESK_DOMAIN:
            raise ValueError("FRESHDESK_DOMAIN environment variable is not set")
        if not self.API_KEY:
            raise ValueError("FRESHDESK_API_KEY environment variable is not set")
    
    def download_file(self, url, filepath):
        """Download a file from URL to the specified filepath with enhanced error handling"""
        try:
            # For S3 URLs, don't use Freshdesk authentication
            if 's3.amazonaws.com' in url or 'cdn.freshdesk.com' in url:
                response = requests.get(url, stream=True)
            else:
                # For Freshdesk API URLs, use authentication
                response = requests.get(url, auth=(self.API_KEY, 'X'), stream=True)
            
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return True
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return False
    
    def sanitize_filename(self, filename):
        """Sanitize filename to be safe for filesystem"""
        # Remove or replace problematic characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Limit length
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        
        return filename
    
    def download_attachments_from_tickets(self, tickets, base_dir="attachments"):
        """Download attachments from ticket data with enhanced functionality"""
        if not tickets:
            logger.info("No tickets provided for attachment download")
            return
        
        # Create base directory
        Path(base_dir).mkdir(exist_ok=True)
        
        downloaded_count = 0
        failed_count = 0
        total_attachments = 0
        
        logger.info(f"📁 Downloading attachments from {len(tickets)} tickets...")
        
        for ticket in tickets:
            ticket_id = ticket.get('id')
            if not ticket_id:
                continue
            
            # Process ticket attachments
            if 'attachments' in ticket:
                for attachment in ticket['attachments']:
                    total_attachments += 1
                    attachment_url = attachment.get('attachment_url') or attachment.get('url')
                    filename = attachment.get('name')
                    
                    if not attachment_url or not filename:
                        logger.warning(f"Skipping attachment with missing URL or filename for ticket {ticket_id}")
                        continue
                    
                    # Create ticket directory
                    ticket_dir = Path(base_dir) / str(ticket_id)
                    ticket_dir.mkdir(exist_ok=True)
                    
                    # Sanitize filename
                    safe_filename = self.sanitize_filename(filename)
                    filepath = ticket_dir / safe_filename
                    
                    # Check if file already exists
                    if filepath.exists():
                        logger.info(f"File already exists: {filepath}")
                        continue
                    
                    logger.info(f"Downloading: {filename} for ticket {ticket_id}")
                    
                    if self.download_file(attachment_url, filepath):
                        downloaded_count += 1
                        logger.info(f"✅ Downloaded: {filepath}")
                    else:
                        failed_count += 1
                        logger.error(f"❌ Failed to download: {filename}")
                    
                    # Rate limiting for downloads
                    time.sleep(CONFIG['delay_between_requests'])
            
            # Process conversation attachments
            if 'conversations' in ticket:
                for conv in ticket['conversations']:
                    if 'attachments' in conv:
                        for attachment in conv['attachments']:
                            total_attachments += 1
                            attachment_url = attachment.get('attachment_url') or attachment.get('url')
                            filename = attachment.get('name')
                            
                            if not attachment_url or not filename:
                                continue
                            
                            # Create ticket directory
                            ticket_dir = Path(base_dir) / str(ticket_id)
                            ticket_dir.mkdir(exist_ok=True)
                            
                            # Sanitize filename and add conversation prefix
                            safe_filename = self.sanitize_filename(filename)
                            safe_filename = f"conv_{safe_filename}"
                            filepath = ticket_dir / safe_filename
                            
                            # Check if file already exists
                            if filepath.exists():
                                logger.info(f"File already exists: {filepath}")
                                continue
                            
                            logger.info(f"Downloading conversation attachment: {filename} for ticket {ticket_id}")
                            
                            if self.download_file(attachment_url, filepath):
                                downloaded_count += 1
                                logger.info(f"✅ Downloaded: {filepath}")
                            else:
                                failed_count += 1
                                logger.error(f"❌ Failed to download: {filename}")
                            
                            # Rate limiting for downloads
                            time.sleep(CONFIG['delay_between_requests'])
        
        logger.info(f"\n📊 Attachment Download Summary:")
        logger.info(f"   - Total attachments found: {total_attachments}")
        logger.info(f"   - Successfully downloaded: {downloaded_count}")
        logger.info(f"   - Failed downloads: {failed_count}")
        logger.info(f"   - Files saved to: {base_dir}/")
    
    def get_enhanced_ticket_details(self, ticket_id):
        """Get detailed information for a specific ticket with enhanced fields"""
        url = f"https://{self.FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}"
        
        try:
            response = requests.get(url, auth=(self.API_KEY, 'X'), headers={"Content-Type": "application/json"})
            
            if response.status_code == 200:
                ticket_data = response.json()
                
                # Enhance with additional fields
                ticket_data['requester_id'] = ticket_data.get('requester_id')
                ticket_data['responder_id'] = ticket_data.get('responder_id')
                
                # Enhance attachments with more details if they exist
                if ticket_data.get("attachments"):
                    enhanced_attachments = []
                    for att in ticket_data["attachments"]:
                        enhanced_attachments.append({
                            "id": att.get("id"),
                            "name": att.get("name"),
                            "url": att.get("attachment_url") or att.get("url"),
                            "content_type": att.get("content_type"),
                            "size": att.get("size"),
                            "created_at": att.get("created_at")
                        })
                    ticket_data["attachments"] = enhanced_attachments
                
                return ticket_data
            else:
                logger.error(f"Error fetching ticket details for {ticket_id}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching ticket {ticket_id}: {e}")
            return None
    
    def get_enhanced_conversations(self, ticket_id):
        """Fetch conversations for a ticket with enhanced fields"""
        url = f"https://{self.FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}/conversations"
        
        try:
            response = requests.get(url, auth=(self.API_KEY, 'X'), headers={"Content-Type": "application/json"})

            if response.status_code != 200:
                logger.error(f"Error fetching conversations for ticket {ticket_id}")
                return []

            conversations = []
            for convo in response.json():
                attachments = [
                    {
                        "id": att.get("id"),
                        "name": att.get("name"),
                        "url": att.get("attachment_url") or att.get("url"),
                        "content_type": att.get("content_type"),
                        "size": att.get("size"),
                        "created_at": att.get("created_at")
                    }
                    for att in convo.get("attachments", [])
                ]
                conversations.append({
                    "id": convo.get("id"),
                    "body_text": convo.get("body_text"),
                    "body": convo.get("body"),
                    "private": convo.get("private"),
                    "created_at": convo.get("created_at"),
                    "user_id": convo.get("user_id"),
                    "created_by": convo.get("created_by"),
                    "incoming": convo.get("incoming"),
                    "outgoing": convo.get("outgoing"),
                    "source": convo.get("source"),
                    "thread_id": convo.get("thread_id"),
                    "attachments": attachments
                })

            return conversations
            
        except Exception as e:
            logger.error(f"Error fetching conversations for ticket {ticket_id}: {e}")
            return []
    
    def save_ticket_data(self, ticket_id, ticket_data, conversations):
        """Save ticket data to 4 separate JSON files (only if data exists)"""
        try:
            files_created = 0
            
            # 1. Ticket Details (always created - contains core ticket info)
            ticket_details = {k: v for k, v in ticket_data.items() 
                           if k not in ['attachments', 'conversations']}
            
            # Always create ticket details file as it contains core ticket information
            ticket_details['ticket_id'] = ticket_id  # Include ticket ID
            details_file = self.output_dirs['ticket_details'] / f"ticket_{ticket_id}_details.json"
            with open(details_file, 'w', encoding='utf-8') as f:
                json.dump(ticket_details, f, indent=2, ensure_ascii=False)
            files_created += 1
            
            # 2. Ticket Attachments (only if attachments exist)
            ticket_attachments = ticket_data.get('attachments', [])
            if ticket_attachments:
                # Add ticket ID to each attachment
                for attachment in ticket_attachments:
                    attachment['ticket_id'] = ticket_id
                
                attachments_file = self.output_dirs['ticket_attachments'] / f"ticket_{ticket_id}_attachments.json"
                with open(attachments_file, 'w', encoding='utf-8') as f:
                    json.dump(ticket_attachments, f, indent=2, ensure_ascii=False)
                files_created += 1
                logger.info(f"📎 Created ticket attachments file for ticket {ticket_id} ({len(ticket_attachments)} attachments)")
            else:
                logger.info(f"📎 No ticket attachments for ticket {ticket_id} - skipping file creation")
            
            # 3. Conversations (only if conversations exist)
            if conversations:
                conversations_data = []
                for conv in conversations:
                    conv_data = {k: v for k, v in conv.items() if k != 'attachments'}
                    conv_data['ticket_id'] = ticket_id  # Include ticket ID
                    conversations_data.append(conv_data)
                
                conversations_file = self.output_dirs['conversations'] / f"ticket_{ticket_id}_conversations.json"
                with open(conversations_file, 'w', encoding='utf-8') as f:
                    json.dump(conversations_data, f, indent=2, ensure_ascii=False)
                files_created += 1
                logger.info(f"💬 Created conversations file for ticket {ticket_id} ({len(conversations)} conversations)")
            else:
                logger.info(f"💬 No conversations for ticket {ticket_id} - skipping file creation")
            
            # 4. Conversation Attachments (only if conversation attachments exist)
            conversation_attachments = []
            for conv in conversations:
                if 'attachments' in conv and conv['attachments']:
                    for attachment in conv['attachments']:
                        attachment['conversation_id'] = conv.get('id')
                        attachment['ticket_id'] = ticket_id  # Include ticket ID
                        attachment['user_id'] = conv.get('user_id')  # Include user ID from conversation
                        conversation_attachments.append(attachment)
            
            if conversation_attachments:
                conv_attachments_file = self.output_dirs['conversation_attachments'] / f"ticket_{ticket_id}_conversation_attachments.json"
                with open(conv_attachments_file, 'w', encoding='utf-8') as f:
                    json.dump(conversation_attachments, f, indent=2, ensure_ascii=False)
                files_created += 1
                logger.info(f"📎 Created conversation attachments file for ticket {ticket_id} ({len(conversation_attachments)} attachments)")
            else:
                logger.info(f"📎 No conversation attachments for ticket {ticket_id} - skipping file creation")
            
            logger.info(f"✅ Created {files_created} JSON files for ticket {ticket_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving JSON files for ticket {ticket_id}: {e}")
            return False
    
    def extract_attachments_from_ticket(self, ticket):
        """Extract attachment information from a ticket object"""
        attachments = []
        ticket_id = ticket.get('id')
        
        if not ticket_id:
            return attachments
        
        # Main ticket attachments
        if 'attachments' in ticket:
            for attachment in ticket['attachments']:
                attachments.append({
                    'ticket_id': ticket_id,
                    'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                    'name': attachment.get('name'),
                    'content_type': attachment.get('content_type'),
                    'size': attachment.get('size'),
                    'source': 'ticket'
                })
        
        # Conversation attachments
        if 'conversations' in ticket:
            for conv in ticket['conversations']:
                if 'attachments' in conv:
                    for attachment in conv['attachments']:
                        attachments.append({
                            'ticket_id': ticket_id,
                            'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                            'name': attachment.get('name'),
                            'content_type': attachment.get('content_type'),
                            'size': attachment.get('size'),
                            'source': 'conversation'
                        })
        
        return attachments
    
    def log_error(self, step, error):
        """Log errors during migration"""
        error_info = {
            'step': step,
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
        self.errors.append(error_info)
        logger.error(f"❌ Error in {step}: {error}")
    
    def step1_get_ticket_ids(self):
        """Step 1: Get all ticket IDs from the CSV file"""
        try:
            logger.info("🚀 Starting Step 1: Reading ticket IDs from CSV...")
            
            # Read ticket IDs from the existing CSV file
            ticket_ids = read_ticket_ids_from_csv('Sample Data from Design OCL - Sheet1.csv')
            
            if not ticket_ids:
                logger.error("❌ No ticket IDs found in CSV file")
                return False
            
            # Store ticket IDs for later use
            self.ticket_ids = ticket_ids
            
            logger.info(f"✅ Step 1 completed: {len(self.ticket_ids)} ticket IDs found in CSV")
            return True
            
        except Exception as e:
            self.log_error("Step 1", e)
            return False
    
    def step2_extract_ticket_details(self):
        """Step 2: Extract detailed ticket information and save 4 JSON files per ticket, then download attachments immediately"""
        try:
            logger.info("🚀 Starting Step 2: Extracting detailed ticket information and downloading attachments...")
            
            if not self.ticket_ids:
                logger.error("❌ No ticket IDs available. Please ensure Step 1 completed successfully.")
                return False
            
            logger.info(f"📁 Processing {len(self.ticket_ids)} ticket IDs from CSV")
            
            # Process tickets individually to create 4 JSON files each and download attachments immediately
            processed_count = 0
            total_tickets = len(self.ticket_ids)
            
            for i, ticket_id in enumerate(self.ticket_ids, 1):
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing ticket {i}/{total_tickets} (ID: {ticket_id})")
                logger.info(f"{'='*50}")
                
                try:
                    # Get ticket details using enhanced function
                    logger.info(f"📋 Extracting ticket details for {ticket_id}...")
                    ticket_details = self.get_enhanced_ticket_details(ticket_id)
                    
                    if ticket_details:
                        # Get conversations using enhanced function
                        logger.info(f"💬 Extracting conversations for {ticket_id}...")
                        conversations = self.get_enhanced_conversations(ticket_id)
                        
                        # Save 4 separate JSON files
                        logger.info(f"💾 Saving JSON files for {ticket_id}...")
                        success = self.save_ticket_data(ticket_id, ticket_details, conversations)
                        
                        if success:
                            # Add to all tickets for tracking
                            ticket_details['conversations'] = conversations
                            self.all_tickets.append(ticket_details)
                            
                            # Download ticket attachments immediately
                            logger.info(f"📎 Downloading ticket attachments for {ticket_id}...")
                            self.download_ticket_attachments(ticket_id, ticket_details)
                            
                            # Download conversation attachments immediately
                            if conversations:
                                logger.info(f"📎 Downloading conversation attachments for {ticket_id}...")
                                self.download_conversation_attachments(ticket_id, conversations)
                            
                            processed_count += 1
                            logger.info(f"✅ Completed processing ticket {ticket_id} ({processed_count}/{total_tickets})")
                        else:
                            logger.error(f"❌ Failed to save JSON files for ticket {ticket_id}")
                    else:
                        logger.error(f"❌ Failed to extract details for ticket {ticket_id}")
                    
                    # Rate limiting between tickets
                    if i < total_tickets:
                        logger.info(f"⏳ Waiting {CONFIG['delay_between_requests']} seconds before next ticket...")
                        time.sleep(CONFIG['delay_between_requests'])
                        
                except Exception as e:
                    logger.error(f"❌ Error processing ticket {ticket_id}: {e}")
                    continue
            
            logger.info(f"✅ Step 2 completed: {processed_count}/{total_tickets} tickets processed with attachments downloaded")
            return True
            
        except Exception as e:
            self.log_error("Step 2", e)
            return False
    
    def download_ticket_attachments(self, ticket_id, ticket_data):
        """Download attachments for a specific ticket immediately"""
        if not ticket_data.get('attachments'):
            logger.info(f"📎 No ticket attachments found for {ticket_id}")
            return
        
        # Create ticket directory
        base_dir = "attachments"
        Path(base_dir).mkdir(exist_ok=True)
        ticket_dir = Path(base_dir) / str(ticket_id)
        ticket_dir.mkdir(exist_ok=True)
        
        downloaded_count = 0
        failed_count = 0
        
        for attachment in ticket_data['attachments']:
            attachment_url = attachment.get('attachment_url') or attachment.get('url')
            filename = attachment.get('name')
            
            if not attachment_url or not filename:
                logger.warning(f"Skipping attachment with missing URL or filename for ticket {ticket_id}")
                continue
            
            # Sanitize filename
            safe_filename = self.sanitize_filename(filename)
            filepath = ticket_dir / safe_filename
            
            # Check if file already exists
            if filepath.exists():
                logger.info(f"📎 File already exists: {filepath}")
                downloaded_count += 1
                continue
            
            logger.info(f"📎 Downloading: {filename} for ticket {ticket_id}")
            
            if self.download_file(attachment_url, filepath):
                downloaded_count += 1
                logger.info(f"✅ Downloaded: {filepath}")
            else:
                failed_count += 1
                logger.error(f"❌ Failed to download: {filename}")
            
            # Rate limiting for downloads
            time.sleep(CONFIG['delay_between_requests'])
        
        logger.info(f"📎 Ticket {ticket_id}: {downloaded_count} downloaded, {failed_count} failed")
    
    def download_conversation_attachments(self, ticket_id, conversations):
        """Download conversation attachments for a specific ticket immediately"""
        if not conversations:
            logger.info(f"📎 No conversations found for ticket {ticket_id}")
            return
        
        # Create ticket directory
        base_dir = "attachments"
        Path(base_dir).mkdir(exist_ok=True)
        ticket_dir = Path(base_dir) / str(ticket_id)
        ticket_dir.mkdir(exist_ok=True)
        
        downloaded_count = 0
        failed_count = 0
        
        for conv in conversations:
            if not conv.get('attachments'):
                continue
                
            for attachment in conv['attachments']:
                attachment_url = attachment.get('attachment_url') or attachment.get('url')
                filename = attachment.get('name')
                
                if not attachment_url or not filename:
                    continue
                
                # Sanitize filename and add conversation prefix
                safe_filename = self.sanitize_filename(filename)
                safe_filename = f"conv_{safe_filename}"
                filepath = ticket_dir / safe_filename
                
                # Check if file already exists
                if filepath.exists():
                    logger.info(f"📎 File already exists: {filepath}")
                    downloaded_count += 1
                    continue
                
                logger.info(f"📎 Downloading conversation attachment: {filename} for ticket {ticket_id}")
                
                if self.download_file(attachment_url, filepath):
                    downloaded_count += 1
                    logger.info(f"✅ Downloaded: {filepath}")
                else:
                    failed_count += 1
                    logger.error(f"❌ Failed to download: {filename}")
                
                # Rate limiting for downloads
                time.sleep(CONFIG['delay_between_requests'])
        
        if downloaded_count > 0 or failed_count > 0:
            logger.info(f"📎 Conversation attachments for ticket {ticket_id}: {downloaded_count} downloaded, {failed_count} failed")
        else:
            logger.info(f"📎 No conversation attachments found for ticket {ticket_id}")
    
    def step3_download_attachments(self):
        """Step 3: This is now handled in Step 2 for immediate processing"""
        logger.info("ℹ️ Step 3: Attachments are now downloaded immediately after each ticket is processed")
        logger.info("   This step is no longer needed as attachments are downloaded in real-time")
        return True
    
    def run_migration(self):
        """Run the complete sequential migration"""
        logger.info("🚀 Starting Modular Sequential Migration with 4 JSON files per ticket")
        logger.info(f"Configuration: {CONFIG}")
        logger.info("Output directories:")
        for name, path in self.output_dirs.items():
            logger.info(f"  - {name}: {path}")
        
        start_time = datetime.now()
        
        # Execute steps sequentially
        logger.info("🔄 Starting sequential migration...")
        
        # Step 1: Get ticket IDs from CSV
        if not self.step1_get_ticket_ids():
            logger.error("❌ Step 1 failed. Migration cannot continue.")
            return
        
        # Step 2: Extract ticket details and create JSON files
        if not self.step2_extract_ticket_details():
            logger.error("❌ Step 2 failed. Migration cannot continue.")
            return
        
        # Step 3: Download attachments
        if not self.step3_download_attachments():
            logger.warning("⚠️ Step 3 failed, but migration completed.")
        
        # Check completion status
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("MIGRATION COMPLETION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Tickets processed: {len(self.all_tickets)}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        # Count files created
        file_counts = {}
        total_files_created = 0
        for dir_name, dir_path in self.output_dirs.items():
            if dir_path.exists():
                file_count = len(list(dir_path.glob("*.json")))
                file_counts[dir_name] = file_count
                total_files_created += file_count
                logger.info(f"Files in {dir_name}: {file_count}")
        
        logger.info(f"📁 Total JSON files created: {total_files_created}")
        
        # Calculate attachment statistics
        total_conversations = sum(len(ticket.get('conversations', [])) for ticket in self.all_tickets)
        total_attachments = sum(len(ticket.get('attachments', [])) for ticket in self.all_tickets)
        total_conv_attachments = sum(
            len(conv.get('attachments', [])) 
            for ticket in self.all_tickets 
            for conv in ticket.get('conversations', [])
        )
        
        # Calculate user statistics
        unique_requesters = set()
        unique_conversation_users = set()
        
        for ticket in self.all_tickets:
            if ticket.get('requester_id'):
                unique_requesters.add(ticket['requester_id'])
            
            for conv in ticket.get('conversations', []):
                if conv.get('user_id'):
                    unique_conversation_users.add(conv['user_id'])
        
        logger.info(f"\n📊 Data Summary:")
        logger.info(f"   - Tickets: {len(self.all_tickets)}")
        logger.info(f"   - Conversations: {total_conversations}")
        logger.info(f"   - Ticket Attachments: {total_attachments}")
        logger.info(f"   - Conversation Attachments: {total_conv_attachments}")
        logger.info(f"   - Unique Requesters: {len(unique_requesters)}")
        logger.info(f"   - Unique Conversation Users: {len(unique_conversation_users)}")
        
        if self.errors:
            logger.warning("⚠️ Errors encountered during migration:")
            for error in self.errors:
                logger.warning(f"  - {error['step']}: {error['error']}")
        
        # Save final results summary
        summary = {
            'migration_completed': datetime.now().isoformat(),
            'duration_seconds': duration.total_seconds(),
            'tickets_processed': len(self.all_tickets),
            'files_created': file_counts,
            'total_files_created': total_files_created,
            'data_summary': {
                'tickets': len(self.all_tickets),
                'conversations': total_conversations,
                'ticket_attachments': total_attachments,
                'conversation_attachments': total_conv_attachments,
                'unique_requesters': len(unique_requesters),
                'unique_conversation_users': len(unique_conversation_users)
            },
            'errors': self.errors
        }
        
        with open('migration_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info("📁 Migration summary saved to: migration_summary.json")
        logger.info("🎉 Migration process completed!")

def main():
    """Main function to run modular migration"""
    coordinator = MigrationCoordinator()
    coordinator.run_migration()

if __name__ == "__main__":
    main() 