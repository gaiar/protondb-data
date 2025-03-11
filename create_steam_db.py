#!/usr/bin/env python3
import requests
import sqlite3
import time
import os
import json
from collections import defaultdict, Counter

def create_database():
    """Create the SQLite database and table if they don't exist."""
    conn = sqlite3.connect('steam_db.db')
    cursor = conn.cursor()
    
    # Create the apps table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS apps (
        appid INTEGER PRIMARY KEY,
        name TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    return conn, cursor

def fetch_steam_apps():
    """Fetch the list of all Steam apps from the Steam API."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        data = response.json()
        return data['applist']['apps']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Steam API: {e}")
        return []

def analyze_duplicates(apps):
    """Analyze different types of duplicates in the app data."""
    # Count app IDs
    app_id_counts = Counter(app['appid'] for app in apps)
    
    # Find app IDs that appear multiple times
    duplicate_ids = {app_id: count for app_id, count in app_id_counts.items() if count > 1}
    
    # Group by app ID and name
    app_id_name_pairs = [(app['appid'], app['name']) for app in apps]
    app_id_name_counts = Counter(app_id_name_pairs)
    
    # Find exact duplicates (same ID and name)
    exact_duplicates = {pair: count for pair, count in app_id_name_counts.items() if count > 1}
    
    # Group app names by app ID
    app_id_to_names = defaultdict(set)
    for app in apps:
        app_id_to_names[app['appid']].add(app['name'])
    
    # Find app IDs with different names
    different_names = {app_id: names for app_id, names in app_id_to_names.items() if len(names) > 1}
    
    return {
        'duplicate_ids': duplicate_ids,
        'exact_duplicates': exact_duplicates,
        'different_names': different_names
    }

def check_database_state(conn, cursor):
    """Check the current state of the database."""
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM apps")
    total_count = cursor.fetchone()[0]
    
    # Get sample records
    cursor.execute("SELECT appid, name FROM apps LIMIT 5")
    sample_records = cursor.fetchall()
    
    return {
        'total_count': total_count,
        'sample_records': sample_records
    }

def populate_database(conn, cursor, apps):
    """Populate the database with the fetched apps."""
    # Prepare for batch insert
    app_data = [(app['appid'], app['name']) for app in apps]
    
    # Use a transaction for better performance
    try:
        cursor.executemany(
            "INSERT OR REPLACE INTO apps (appid, name) VALUES (?, ?)",
            app_data
        )
        conn.commit()
        return len(app_data)
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
        return 0

def reset_database(conn, cursor):
    """Reset the database by dropping and recreating the table."""
    try:
        cursor.execute("DROP TABLE IF EXISTS apps")
        cursor.execute('''
        CREATE TABLE apps (
            appid INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        conn.commit()
        print("Database reset successfully.")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Error resetting database: {e}")

def main():
    print("Starting Steam app database creation...")
    
    # Create or connect to the database
    conn, cursor = create_database()
    
    # Check database state before
    print("\nChecking database state before fetching data...")
    before_state = check_database_state(conn, cursor)
    print(f"Records in database before: {before_state['total_count']}")
    if before_state['total_count'] > 0:
        print("Sample records before:")
        for record in before_state['sample_records']:
            print(f"  App ID: {record[0]}, Name: '{record[1]}'")
    
    # Ask if user wants to reset the database
    if before_state['total_count'] > 0:
        reset_choice = input("\nDatabase already contains records. Reset database? (y/n): ")
        if reset_choice.lower() == 'y':
            reset_database(conn, cursor)
    
    # Fetch apps from Steam API
    print("\nFetching apps from Steam API...")
    apps = fetch_steam_apps()
    
    if not apps:
        print("No apps fetched. Exiting.")
        conn.close()
        return
    
    # After fetching apps
    unique_app_ids = set(app['appid'] for app in apps)
    print(f"Total apps fetched: {len(apps)}")
    print(f"Unique app IDs: {len(unique_app_ids)}")
    
    # Analyze duplicates
    print("\nAnalyzing duplicates...")
    duplicate_analysis = analyze_duplicates(apps)
    
    # Report on duplicate IDs
    duplicate_ids = duplicate_analysis['duplicate_ids']
    print(f"App IDs that appear multiple times: {len(duplicate_ids)}")
    if duplicate_ids:
        print("\n5 Examples of app IDs that appear multiple times:")
        for i, (app_id, count) in enumerate(list(duplicate_ids.items())[:5]):
            print(f"App ID {app_id} appears {count} times")
    
    # Report on exact duplicates
    exact_duplicates = duplicate_analysis['exact_duplicates']
    print(f"\nExact duplicates (same ID and name): {len(exact_duplicates)}")
    if exact_duplicates:
        print("\n5 Examples of exact duplicates:")
        for i, ((app_id, name), count) in enumerate(list(exact_duplicates.items())[:5]):
            print(f"App ID {app_id}, Name '{name}' appears {count} times")
    
    # Report on different names
    different_names = duplicate_analysis['different_names']
    print(f"\nApp IDs with different names: {len(different_names)}")
    if different_names:
        print("\n5 Examples of app IDs with different names:")
        for i, (app_id, names) in enumerate(list(different_names.items())[:5]):
            print(f"App ID {app_id}:")
            for name in names:
                print(f"  - '{name}'")
            print()
    
    # Count apps with empty names
    empty_names = sum(1 for app in apps if app['name'] == '')
    print(f"Apps with empty names: {empty_names}")
    
    # Populate the database
    print(f"\nPopulating database with {len(apps)} apps...")
    inserted_count = populate_database(conn, cursor, apps)
    
    # Check database state after
    print("\nChecking database state after insertion...")
    after_state = check_database_state(conn, cursor)
    print(f"Records in database after: {after_state['total_count']}")
    print("Sample records after:")
    for record in after_state['sample_records']:
        print(f"  App ID: {record[0]}, Name: '{record[1]}'")
    
    # Calculate the difference
    records_difference = after_state['total_count'] - before_state['total_count']
    print(f"\nRecords added to database: {records_difference}")
    print(f"Expected records to add: {len(unique_app_ids)}")
    
    print(f"\nDatabase populated successfully!")
    print(f"Inserted/updated {inserted_count} apps.")
    print(f"Total apps in database: {after_state['total_count']}")
    
    # Close the database connection
    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()
