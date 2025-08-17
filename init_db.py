#!/usr/bin/env python3
"""
Database initialization script for Tombola application.
Run this script to create the database and tables.
"""

import os
from dotenv import load_dotenv
from sql_client import PostgresClient


def init_database():
    """Initialize the database and create required tables."""
    try:
        print("Connecting to PostgreSQL database...")

        # Test connection pool first
        if not PostgresClient.test_connection():
            print("Failed to establish database connection")
            return False

        with PostgresClient() as db:
            print("Creating tickets table...")
            db.create_tickets_table()
            print("Database initialization completed successfully!")

            # Test connection by fetching tickets (should be empty initially)
            tickets = db.fetch_tickets(limit=1)
            print(f"Database connection test: {len(tickets)} tickets found")

        # Close the connection pool after initialization
        PostgresClient.close_pool()
        print("Connection pool closed successfully")

    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("\nPlease check your environment variables:")
        print("- DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
        print("- Or DATABASE_URL")
        return False

    return True


if __name__ == "__main__":
    load_dotenv()

    # Check if required environment variables are set
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars and not os.getenv("DATABASE_URL"):
        print("Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease set these variables or use DATABASE_URL")
        exit(1)

    success = init_database()
    exit(0 if success else 1)
