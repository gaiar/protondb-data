#!/usr/bin/env python3
import sqlite3
import argparse
import sys
from pathlib import Path


class ProtonDBQuerier:
    """Class to query the ProtonDB games database."""
    
    def __init__(self, db_path="protondb_games.db"):
        """Initialize the querier with path to database."""
        self.db_path = Path(db_path)
        self.conn = None
        self.cursor = None
        
        # Connect to the database
        self._connect_to_db()
    
    def _connect_to_db(self):
        """Connect to the SQLite database."""
        try:
            if not self.db_path.exists():
                print(f"Error: Database file {self.db_path} does not exist.")
                sys.exit(1)
                
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # This enables column access by name
            self.cursor = self.conn.cursor()
            print(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            sys.exit(1)
    
    def get_total_games_count(self):
        """Get the total count of games in the database."""
        query = "SELECT COUNT(*) FROM games"
        self.cursor.execute(query)
        total_games = self.cursor.fetchone()[0]
        return total_games
    
    def search_games_by_name(self, name_pattern):
        """Search for games by name pattern."""
        query = "SELECT * FROM games WHERE title LIKE ?"
        self.cursor.execute(query, (f'%{name_pattern}%',))
        games = self.cursor.fetchall()
        return games
    
    def get_game_by_app_id(self, app_id):
        """Get a game by its app_id."""
        query = "SELECT * FROM games WHERE app_id = ?"
        self.cursor.execute(query, (app_id,))
        game = self.cursor.fetchone()
        return game
    
    def get_most_reported_games(self, limit=10):
        """Get games with the most reports."""
        query = "SELECT * FROM games ORDER BY report_count DESC LIMIT ?"
        self.cursor.execute(query, (limit,))
        games = self.cursor.fetchall()
        return games
    
    def get_recently_added_games(self, limit=10):
        """Get the most recently added games."""
        query = "SELECT * FROM games ORDER BY first_seen DESC LIMIT ?"
        self.cursor.execute(query, (limit,))
        games = self.cursor.fetchall()
        return games
    
    def get_recently_updated_games(self, limit=10):
        """Get the most recently updated games."""
        query = "SELECT * FROM games ORDER BY last_seen DESC LIMIT ?"
        self.cursor.execute(query, (limit,))
        games = self.cursor.fetchall()
        return games
    
    def get_database_stats(self):
        """Get various statistics about the database."""
        stats = {}
        
        # Total games
        self.cursor.execute("SELECT COUNT(*) FROM games")
        stats['total_games'] = self.cursor.fetchone()[0]
        
        # Games with most reports
        self.cursor.execute("SELECT MAX(report_count) FROM games")
        stats['max_reports'] = self.cursor.fetchone()[0]
        
        # Average reports per game
        self.cursor.execute("SELECT AVG(report_count) FROM games")
        stats['avg_reports'] = round(self.cursor.fetchone()[0], 2)
        
        # Oldest and newest game (by first_seen)
        self.cursor.execute("SELECT MIN(first_seen), MAX(first_seen) FROM games")
        min_max = self.cursor.fetchone()
        stats['oldest_game_timestamp'] = min_max[0]
        stats['newest_game_timestamp'] = min_max[1]
        
        return stats
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")


def display_games(games, show_all_fields=False):
    """Display games in a formatted way."""
    if not games:
        print("No games found.")
        return
    
    if isinstance(games, sqlite3.Row):
        games = [games]  # Convert single game to list
    
    if show_all_fields:
        # Print all fields
        for game in games:
            print("\n" + "=" * 50)
            for key in game.keys():
                print(f"{key}: {game[key]}")
    else:
        # Print simplified view
        print(f"\nFound {len(games)} games:")
        print("-" * 80)
        print(f"{'App ID':<10} | {'Title':<50} | {'Reports':<10}")
        print("-" * 80)
        for game in games:
            print(f"{game['app_id']:<10} | {game['title'][:48]:<50} | {game['report_count']:<10}")


def main():
    parser = argparse.ArgumentParser(description="Query the ProtonDB games database.")
    parser.add_argument("--db", default="protondb_games.db", help="Path to the database file")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Count command
    count_parser = subparsers.add_parser("count", help="Get total number of games")
    
    # Search command
    search_parser = subparsers.add_parser("search", help="Search for games by name")
    search_parser.add_argument("pattern", help="Name pattern to search for")
    
    # Get by app_id command
    app_parser = subparsers.add_parser("app", help="Get game by app_id")
    app_parser.add_argument("app_id", help="Steam app ID")
    
    # Most reported games command
    most_reported_parser = subparsers.add_parser("most-reported", help="Get games with most reports")
    most_reported_parser.add_argument("--limit", type=int, default=10, help="Number of games to show")
    
    # Recently added games command
    recent_parser = subparsers.add_parser("recent", help="Get recently added games")
    recent_parser.add_argument("--limit", type=int, default=10, help="Number of games to show")
    
    # Recently updated games command
    updated_parser = subparsers.add_parser("updated", help="Get recently updated games")
    updated_parser.add_argument("--limit", type=int, default=10, help="Number of games to show")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Get database statistics")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create querier
    querier = ProtonDBQuerier(args.db)
    
    try:
        if args.command == "count":
            count = querier.get_total_games_count()
            print(f"Total games in database: {count}")
        
        elif args.command == "search":
            games = querier.search_games_by_name(args.pattern)
            display_games(games)
        
        elif args.command == "app":
            game = querier.get_game_by_app_id(args.app_id)
            display_games(game, show_all_fields=True)
        
        elif args.command == "most-reported":
            games = querier.get_most_reported_games(args.limit)
            display_games(games)
        
        elif args.command == "recent":
            games = querier.get_recently_added_games(args.limit)
            display_games(games)
        
        elif args.command == "updated":
            games = querier.get_recently_updated_games(args.limit)
            display_games(games)
        
        elif args.command == "stats":
            stats = querier.get_database_stats()
            print("\nDatabase Statistics:")
            print("=" * 50)
            print(f"Total games: {stats['total_games']}")
            print(f"Maximum reports for a game: {stats['max_reports']}")
            print(f"Average reports per game: {stats['avg_reports']}")
            print(f"Oldest game timestamp: {stats['oldest_game_timestamp']}")
            print(f"Newest game timestamp: {stats['newest_game_timestamp']}")
        
        else:
            parser.print_help()
    
    finally:
        querier.close()


if __name__ == "__main__":
    main()
