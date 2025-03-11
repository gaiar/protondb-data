#!/usr/bin/env python3
import os
import json
import sqlite3
import tarfile
import tempfile
import logging
import time
import datetime
import shutil
import argparse
import re
from pathlib import Path
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("protondb_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProtonDBExtractor:
    def __init__(self, archive_dir="reports", db_path="protondb_games.db", remove_json=False):
        """Initialize the extractor with paths to archives and database."""
        self.archive_dir = Path(archive_dir)
        self.db_path = Path(db_path)
        self.conn = None
        self.cursor = None
        self.processed_games = set()  # Track processed games to avoid duplicates
        self.remove_json = remove_json  # Whether to remove JSON files after processing
        self.start_time = time.time()
        
        # Statistics
        self.stats = {
            "total_archives": 0,
            "total_json_files": 0,
            "total_entries_processed": 0,
            "total_games_added": 0,
            "total_games_updated": 0,
            "db_size_before": 0,
            "db_size_after": 0,
            "processing_time": 0
        }

    def get_db_size(self):
        """Get the size of the database file in MB."""
        if self.db_path.exists():
            return round(self.db_path.stat().st_size / (1024 * 1024), 2)
        return 0

    def print_stats(self, title="Current Statistics"):
        """Print current statistics."""
        elapsed_time = time.time() - self.start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Get current database size
        current_db_size = self.get_db_size()
        
        # Get current count of games in database
        self.cursor.execute("SELECT COUNT(*) FROM games")
        current_game_count = self.cursor.fetchone()[0]
        
        logger.info(f"\n{'=' * 50}")
        logger.info(f"{title}")
        logger.info(f"{'=' * 50}")
        logger.info(f"Time elapsed: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        logger.info(f"Archives processed: {self.stats['processed_archives']} / {self.stats['total_archives']}")
        logger.info(f"JSON files processed: {self.stats['processed_json_files']} / {self.stats['total_json_files']}")
        logger.info(f"Entries processed: {self.stats['total_entries_processed']}")
        logger.info(f"Games in database: {current_game_count}")
        logger.info(f"New games added: {self.stats['total_games_added']}")
        logger.info(f"Existing games updated: {self.stats['total_games_updated']}")
        logger.info(f"Database size: {current_db_size} MB")
        if self.stats['db_size_before'] > 0:
            size_change = current_db_size - self.stats['db_size_before']
            logger.info(f"Database growth: {size_change:.2f} MB")
        logger.info(f"{'=' * 50}\n")

    def setup_database(self):
        """Create the SQLite database and required tables if they don't exist."""
        try:
            # Record initial database size
            self.stats['db_size_before'] = self.get_db_size()
            
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create games table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                app_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                first_seen INTEGER,
                last_seen INTEGER,
                report_count INTEGER DEFAULT 1
            )
            ''')
            
            # Create an index on title for faster searches
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_title ON games(title)')
            
            # Get initial count of games in database
            self.cursor.execute("SELECT COUNT(*) FROM games")
            initial_game_count = self.cursor.fetchone()[0]
            
            self.conn.commit()
            logger.info(f"Database setup complete at {self.db_path}")
            logger.info(f"Initial database size: {self.stats['db_size_before']} MB")
            logger.info(f"Initial game count: {initial_game_count}")
            
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise

    def get_archive_files(self):
        """Get a list of all tar.gz files in the archive directory."""
        archives = sorted([f for f in self.archive_dir.glob("*.tar.gz")])
        self.stats['total_archives'] = len(archives)
        self.stats['processed_archives'] = 0
        return archives

    def get_json_files(self):
        """Get a list of all JSON files in the archive directory."""
        json_files = sorted([f for f in self.archive_dir.glob("*.json")])
        self.stats['total_json_files'] = len(json_files)
        self.stats['processed_json_files'] = 0
        return json_files

    def process_json_entry(self, entry_data, timestamp=None):
        """Process a single JSON entry and add it to the database."""
        try:
            # Extract app info
            app_info = entry_data.get("app", {})
            steam_info = app_info.get("steam", {})
            
            app_id = steam_info.get("appId")
            title = app_info.get("title")
            
            # Skip if missing essential data
            if not app_id or not title:
                return False, False
            
            # Use the entry's timestamp if available, otherwise use the provided one
            entry_timestamp = entry_data.get("timestamp", timestamp)
            
            # Check if this game is already in our database
            self.cursor.execute("SELECT app_id, first_seen, last_seen, report_count FROM games WHERE app_id = ?", (app_id,))
            result = self.cursor.fetchone()
            
            is_new = False
            is_updated = False
            
            if result:
                # Game exists, update the record
                _, first_seen, last_seen, report_count = result
                
                # Update first_seen if this entry is older
                if entry_timestamp and entry_timestamp < first_seen:
                    first_seen = entry_timestamp
                    is_updated = True
                
                # Update last_seen if this entry is newer
                if entry_timestamp and entry_timestamp > last_seen:
                    last_seen = entry_timestamp
                    is_updated = True
                
                self.cursor.execute(
                    "UPDATE games SET first_seen = ?, last_seen = ?, report_count = report_count + 1 WHERE app_id = ?",
                    (first_seen, last_seen, app_id)
                )
                is_updated = True
            else:
                # New game, insert it
                self.cursor.execute(
                    "INSERT INTO games (app_id, title, first_seen, last_seen, report_count) VALUES (?, ?, ?, ?, ?)",
                    (app_id, title, entry_timestamp, entry_timestamp, 1)
                )
                is_new = True
                
            # Add to processed set to track unique games
            self.processed_games.add(app_id)
            return is_new, is_updated
            
        except Exception as e:
            logger.error(f"Error processing entry: {e}")
            return False, False

    def process_archive(self, archive_path):
        """Extract and process all JSON files from a tar.gz archive."""
        archive_start_time = time.time()
        logger.info(f"Processing archive: {archive_path}")
        
        entries_processed = 0
        games_added = 0
        games_updated = 0
        
        try:
            # Extract timestamp from filename (if possible)
            # Format: reports_monthX_YYYY.tar.gz (where X can be a number and YYYY is the year)
            filename = archive_path.name
            archive_timestamp = None
            
            # Try to extract date from filename using regex
            try:
                # Pattern to match month names and extract year
                pattern = r'reports_([a-z]+)(\d*)_(\d{4})\.tar\.gz'
                match = re.match(pattern, filename)
                
                if match:
                    month_name = match.group(1)  # e.g., 'jan', 'feb', etc.
                    # year = match.group(3)  # e.g., '2019', '2020', etc.
                    
                    # Map abbreviated month names to their numerical values
                    month_map = {
                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                    }
                    
                    if month_name in month_map:
                        month_num = month_map[month_name]
                        year = int(match.group(3))
                        # Use the 15th day of the month as a default
                        dt = datetime.datetime(year, month_num, 15)
                        archive_timestamp = int(dt.timestamp())
                        logger.info(f"Extracted timestamp from filename: {dt.strftime('%Y-%m-%d')} ({archive_timestamp})")
                    else:
                        logger.warning(f"Unknown month name in filename: {month_name}")
                else:
                    # Handle special files like reports_piiremoved.tar.gz
                    # Use current time as a fallback
                    if "piiremoved" in filename:
                        archive_timestamp = int(time.time())
                        logger.info(f"Using current timestamp for special file: {filename}")
                    else:
                        logger.warning(f"Filename does not match expected pattern: {filename}")
            except Exception as e:
                logger.warning(f"Could not extract timestamp from filename {filename}: {e}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Extract archive to temporary directory
                with tarfile.open(archive_path, "r:gz") as tar:
                    members = tar.getmembers()
                    logger.info(f"Found {len(members)} files in archive")
                    tar.extractall(path=temp_dir)
                
                # Process all JSON files in the temporary directory
                json_files = list(Path(temp_dir).glob("**/*.json"))
                logger.info(f"Found {len(json_files)} JSON files to process")
                
                for json_file in tqdm(json_files, desc=f"Processing {archive_path.name}", unit="files"):
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            
                            # Handle both single entries and lists of entries
                            if isinstance(data, list):
                                for entry in data:
                                    entries_processed += 1
                                    is_new, is_updated = self.process_json_entry(entry, archive_timestamp)
                                    if is_new:
                                        games_added += 1
                                    elif is_updated:
                                        games_updated += 1
                            else:
                                entries_processed += 1
                                is_new, is_updated = self.process_json_entry(data, archive_timestamp)
                                if is_new:
                                    games_added += 1
                                elif is_updated:
                                    games_updated += 1
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in file: {json_file}")
                    except Exception as e:
                        logger.error(f"Error processing file {json_file}: {e}")
            
            self.conn.commit()
            
            # Update statistics
            self.stats['total_entries_processed'] += entries_processed
            self.stats['total_games_added'] += games_added
            self.stats['total_games_updated'] += games_updated
            self.stats['processed_archives'] += 1
            
            # Calculate processing time
            archive_time = time.time() - archive_start_time
            
            # Get current database stats
            self.cursor.execute("SELECT COUNT(*) FROM games")
            current_game_count = self.cursor.fetchone()[0]
            current_db_size = self.get_db_size()
            
            logger.info(f"Archive {archive_path.name} processed in {archive_time:.2f} seconds")
            logger.info(f"  - Entries processed: {entries_processed}")
            logger.info(f"  - Games added: {games_added}")
            logger.info(f"  - Games updated: {games_updated}")
            logger.info(f"  - Current games in database: {current_game_count}")
            logger.info(f"  - Current database size: {current_db_size} MB")
            
            return entries_processed, games_added, games_updated
            
        except Exception as e:
            logger.error(f"Error processing archive {archive_path}: {e}")
            return 0, 0, 0

    def process_json_file(self, json_path):
        """Process a standalone JSON file (not in an archive)."""
        file_start_time = time.time()
        logger.info(f"Processing JSON file: {json_path}")
        
        entries_processed = 0
        games_added = 0
        games_updated = 0
        
        try:
            # Extract timestamp from filename (if possible)
            # Format: reports_monthX_YYYY.json (where X can be a number and YYYY is the year)
            filename = json_path.name
            file_timestamp = None
            
            # Try to extract date from filename using regex
            try:
                # Pattern to match month names and extract year
                pattern = r'reports_([a-z]+)(\d*)_(\d{4})\.json'
                match = re.match(pattern, filename)
                
                if match:
                    month_name = match.group(1)  # e.g., 'jan', 'feb', etc.
                    
                    # Map abbreviated month names to their numerical values
                    month_map = {
                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                    }
                    
                    if month_name in month_map:
                        month_num = month_map[month_name]
                        year = int(match.group(3))
                        # Use the 15th day of the month as a default
                        dt = datetime.datetime(year, month_num, 15)
                        file_timestamp = int(dt.timestamp())
                        logger.info(f"Extracted timestamp from filename: {dt.strftime('%Y-%m-%d')} ({file_timestamp})")
                    else:
                        logger.warning(f"Unknown month name in filename: {month_name}")
                else:
                    # Handle special files like reports_piiremoved.json
                    # Use current time as a fallback
                    if "piiremoved" in filename:
                        file_timestamp = int(time.time())
                        logger.info(f"Using current timestamp for special file: {filename}")
                    else:
                        logger.warning(f"Filename does not match expected pattern: {filename}")
            except Exception as e:
                logger.warning(f"Could not extract timestamp from filename {filename}: {e}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both single entries and lists of entries
                if isinstance(data, list):
                    total_entries = len(data)
                    logger.info(f"Found {total_entries} entries in {json_path.name}")
                    
                    for entry in tqdm(data, desc=f"Processing {json_path.name}", unit="entries"):
                        entries_processed += 1
                        is_new, is_updated = self.process_json_entry(entry, file_timestamp)
                        if is_new:
                            games_added += 1
                        elif is_updated:
                            games_updated += 1
                        
                        # Commit every 10,000 entries to avoid large transactions
                        if entries_processed % 10000 == 0:
                            self.conn.commit()
                            logger.info(f"Processed {entries_processed}/{total_entries} entries from {json_path.name}")
                else:
                    entries_processed += 1
                    is_new, is_updated = self.process_json_entry(data, file_timestamp)
                    if is_new:
                        games_added += 1
                    elif is_updated:
                        games_updated += 1
            
            self.conn.commit()
            
            # Update statistics
            self.stats['total_entries_processed'] += entries_processed
            self.stats['total_games_added'] += games_added
            self.stats['total_games_updated'] += games_updated
            self.stats['processed_json_files'] += 1
            
            # Calculate processing time
            file_time = time.time() - file_start_time
            
            # Get current database stats
            self.cursor.execute("SELECT COUNT(*) FROM games")
            current_game_count = self.cursor.fetchone()[0]
            current_db_size = self.get_db_size()
            
            logger.info(f"JSON file {json_path.name} processed in {file_time:.2f} seconds")
            logger.info(f"  - Entries processed: {entries_processed}")
            logger.info(f"  - Games added: {games_added}")
            logger.info(f"  - Games updated: {games_updated}")
            logger.info(f"  - Current games in database: {current_game_count}")
            logger.info(f"  - Current database size: {current_db_size} MB")
            
            # Remove the JSON file if requested
            if self.remove_json:
                try:
                    logger.info(f"Removing processed JSON file: {json_path}")
                    os.remove(json_path)
                except Exception as e:
                    logger.error(f"Error removing JSON file {json_path}: {e}")
            
            return entries_processed, games_added, games_updated
            
        except Exception as e:
            logger.error(f"Error processing JSON file {json_path}: {e}")
            return 0, 0, 0

    def run(self):
        """Run the extraction process for all archives."""
        try:
            logger.info("Starting ProtonDB data extraction")
            self.start_time = time.time()
            
            self.setup_database()
            
            # Process all tar.gz archives
            archives = self.get_archive_files()
            logger.info(f"Found {len(archives)} archives to process")
            
            # Process all standalone JSON files
            json_files = self.get_json_files()
            logger.info(f"Found {len(json_files)} standalone JSON files to process")
            
            # Print initial stats
            self.print_stats("Initial Statistics")
            
            # Process archives
            for i, archive in enumerate(archives, 1):
                logger.info(f"Processing archive {i}/{len(archives)}: {archive.name}")
                entries, added, updated = self.process_archive(archive)
                
                # Print stats every 5 archives or at the end
                if i % 5 == 0 or i == len(archives):
                    self.print_stats(f"Statistics after processing {i}/{len(archives)} archives")
            
            # Process standalone JSON files
            for i, json_file in enumerate(json_files, 1):
                logger.info(f"Processing JSON file {i}/{len(json_files)}: {json_file.name}")
                entries, added, updated = self.process_json_file(json_file)
                
                # Print stats after each JSON file
                self.print_stats(f"Statistics after processing {i}/{len(json_files)} JSON files")
            
            # Calculate total processing time
            total_time = time.time() - self.start_time
            hours, remainder = divmod(total_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Update final database size
            self.stats['db_size_after'] = self.get_db_size()
            
            # Print final stats
            logger.info(f"\n{'=' * 50}")
            logger.info(f"EXTRACTION COMPLETE")
            logger.info(f"{'=' * 50}")
            logger.info(f"Total processing time: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
            logger.info(f"Archives processed: {self.stats['processed_archives']}/{self.stats['total_archives']}")
            logger.info(f"JSON files processed: {self.stats['processed_json_files']}/{self.stats['total_json_files']}")
            logger.info(f"Total entries processed: {self.stats['total_entries_processed']}")
            
            # Get final database stats
            self.cursor.execute("SELECT COUNT(*) FROM games")
            final_game_count = self.cursor.fetchone()[0]
            logger.info(f"Total games in database: {final_game_count}")
            logger.info(f"New games added: {self.stats['total_games_added']}")
            logger.info(f"Existing games updated: {self.stats['total_games_updated']}")
            
            # Database size stats
            db_growth = self.stats['db_size_after'] - self.stats['db_size_before']
            logger.info(f"Initial database size: {self.stats['db_size_before']} MB")
            logger.info(f"Final database size: {self.stats['db_size_after']} MB")
            logger.info(f"Database growth: {db_growth:.2f} MB")
            logger.info(f"{'=' * 50}\n")
            
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
        finally:
            if self.conn:
                self.conn.close()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Extract ProtonDB data from archives and store in SQLite database")
    parser.add_argument("--archive-dir", default="reports", help="Directory containing the archives (default: reports)")
    parser.add_argument("--db-path", default="protondb_games.db", help="Path to the SQLite database (default: protondb_games.db)")
    parser.add_argument("--remove-json", action="store_true", help="Remove JSON files after processing")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    extractor = ProtonDBExtractor(
        archive_dir=args.archive_dir,
        db_path=args.db_path,
        remove_json=args.remove_json
    )
    extractor.run() 