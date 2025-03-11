# protondb-data
Data exports from ProtonDB.com released under ODbL

# Open Database License
ProtonDB reports data is made available under Open Database License whose full text can be found at http://opendatacommons.org/licenses/odbl/. Any rights in individual contents of the database are licensed under the Database Contents License whose text can be found http://opendatacommons.org/licenses/dbcl/

# ProtonDB Data Extractor

This script extracts game data from ProtonDB tar.gz archives and stores it in a SQLite database.

## Features

- Extracts data from all tar.gz archives in the `reports` directory
- Processes standalone JSON files in the `reports` directory
- Creates a SQLite database with game titles and app IDs
- Tracks the first and last time a game was seen in reports
- Counts the number of reports for each game
- Handles duplicates by updating existing records
- Provides detailed progress tracking with progress bars
- Shows timing information for each processed file
- Displays database statistics during and after processing
- Option to remove JSON files after processing

## Requirements

- Python 3.6+
- Dependencies listed in `requirements.txt`

## Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the script:

```bash
python extract_protondb_data.py [options]
```

### Command-line Options

- `--archive-dir DIRECTORY`: Directory containing the archives (default: reports)
- `--db-path FILE`: Path to the SQLite database (default: protondb_games.db)
- `--remove-json`: Remove JSON files after processing

### Examples

```bash
# Basic usage
python extract_protondb_data.py

# Specify a different archive directory
python extract_protondb_data.py --archive-dir /path/to/archives

# Specify a different database path
python extract_protondb_data.py --db-path /path/to/database.db

# Remove JSON files after processing
python extract_protondb_data.py --remove-json
```

The script will:
1. Create a SQLite database named `protondb_games.db` in the current directory (or the specified path)
2. Process all tar.gz archives in the `reports` directory (or the specified directory)
3. Process any standalone JSON files in the `reports` directory
4. Log progress to both the console and a file named `protondb_extraction.log`
5. Display detailed statistics during and after processing
6. Optionally remove JSON files after processing

## Progress Tracking

The script provides detailed progress information:
- Progress bars for processing files and entries
- Time elapsed for the entire process
- Processing time for each archive and JSON file
- Number of files in the queue
- Number of entries processed
- Number of games added and updated
- Database size before, during, and after processing

## Database Schema

The database contains a single table named `games` with the following columns:

- `app_id`: Steam app ID (primary key)
- `title`: Game title
- `first_seen`: Timestamp of the first time the game was seen in reports
- `last_seen`: Timestamp of the last time the game was seen in reports
- `report_count`: Number of reports for this game

## Example Queries

```sql
-- Get all games
SELECT * FROM games;

-- Get games with the most reports
SELECT app_id, title, report_count FROM games ORDER BY report_count DESC LIMIT 10;

-- Search for games by title
SELECT app_id, title FROM games WHERE title LIKE '%witcher%';
```

## Database Query Script

The repository includes a Python script `querries-db.py` that provides a convenient command-line interface for querying the ProtonDB games database.

### Features

- Query the database without writing SQL
- Search for games by name
- Look up games by Steam app ID
- Get statistics about the database
- View games with the most reports
- See recently added or updated games
- Format output in a readable way

### Usage

```bash
python querries-db.py [--db DB_PATH] COMMAND [ARGS]
```

### Commands

- `count`: Get the total number of games in the database
  ```bash
  python querries-db.py count
  ```

- `stats`: Display various statistics about the database
  ```bash
  python querries-db.py stats
  ```

- `search`: Search for games by name pattern
  ```bash
  python querries-db.py search "Dark Souls"
  ```

- `app`: Get detailed information about a specific game by its app ID
  ```bash
  python querries-db.py app 1091500
  ```

- `most-reported`: Show games with the most reports (default: top 10)
  ```bash
  python querries-db.py most-reported --limit 5
  ```

- `recent`: Show the most recently added games (default: top 10)
  ```bash
  python querries-db.py recent --limit 20
  ```

- `updated`: Show the most recently updated games (default: top 10)
  ```bash
  python querries-db.py updated
  ```

### Options

- `--db DB_PATH`: Path to the database file (default: protondb_games.db)
- `--help`: Show help message and exit

For more information on a specific command, use:
```bash
python querries-db.py COMMAND --help
```

# Steam Database Creator

This script fetches data from the Steam API and stores it in a SQLite database.

## Requirements

- Python 3.6+
- Required packages listed in `requirements.txt`

## Installation

1. Clone this repository or download the files
2. Install the required packages:

```bash
pip install -r requirements.txt
```

## Usage

Simply run the script:

```bash
python create_steam_db.py
```

This will:
1. Create a SQLite database file named `steam_db.db` if it doesn't exist
2. Fetch the list of all Steam apps from the Steam API
3. Store the app IDs and names in the database

## Database Structure

The database contains a single table named `apps` with the following columns:

- `appid` (INTEGER): The unique identifier for the Steam app (primary key)
- `name` (TEXT): The name of the Steam app
- `last_updated` (TIMESTAMP): When the record was last updated

## Accessing the Data

You can access the data using any SQLite client or with Python:

```python
import sqlite3

# Connect to the database
conn = sqlite3.connect('steam_db.db')
cursor = conn.cursor()

# Example: Query all apps
cursor.execute("SELECT * FROM apps LIMIT 10")
apps = cursor.fetchall()
for app in apps:
    print(app)

# Close the connection
conn.close()
```