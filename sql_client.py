from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import psycopg2
from psycopg2 import Error
from psycopg2.pool import SimpleConnectionPool
import threading


Params = Optional[Union[Dict[str, Any], Sequence[Any], Tuple[Any, ...]]]


class PostgresClient:
    """Thin convenience wrapper around psycopg2 with connection pooling.

    Usage:
        with PostgresClient() as db:
            db.create_tickets_table()
            inserted = db.insert_tickets([...])
    """

    _pool: Optional[SimpleConnectionPool] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.connection: Optional[psycopg2.extensions.connection] = None

    @classmethod
    def _get_pool(cls) -> SimpleConnectionPool:
        """Get or create the connection pool."""
        if cls._pool is None:
            with cls._lock:
                if cls._pool is None:
                    cls._pool = SimpleConnectionPool(
                        minconn=1,
                        maxconn=10,
                        host=os.getenv("DB_HOST", "localhost"),
                        user=os.getenv("DB_USER", "postgres"),
                        password=os.getenv("DB_PASSWORD", ""),
                        database=os.getenv("DB_NAME", "tombola"),
                        port=int(os.getenv("DB_PORT", "5432")),
                    )
        return cls._pool

    # --- context manager -------------------------------------------------
    def __enter__(self) -> "PostgresClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- connection lifecycle -------------------------------------------
    def connect(self) -> None:
        if self.connection is None:
            pool = self._get_pool()
            self.connection = pool.getconn()
            self.connection.autocommit = True

    def close(self) -> None:
        if self.connection is not None:
            pool = self._get_pool()
            pool.putconn(self.connection)
            self.connection = None

    def _require_connection(self) -> psycopg2.extensions.connection:
        if self.connection is None or self.connection.closed:
            raise RuntimeError(
                "Database connection is not established. Call connect() or use context manager."
            )
        return self.connection

    # --- generic helpers -------------------------------------------------
    def execute(self, query: str, params: Params = None) -> psycopg2.extensions.cursor:
        conn = self._require_connection()
        cursor = conn.cursor()
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)
        return cursor

    def executemany(
        self, query: str, seq_of_params: Iterable[Union[Dict[str, Any], Sequence[Any]]]
    ) -> psycopg2.extensions.cursor:
        conn = self._require_connection()
        cursor = conn.cursor()
        cursor.executemany(query, seq_of_params)
        return cursor

    def query_all(self, query: str, params: Params = None) -> List[Tuple[Any, ...]]:
        cursor = self.execute(query, params)
        return cursor.fetchall()

    # --- tickets domain --------------------------------------------------
    def create_tickets_table(self) -> None:
        """Create the tickets table if it doesn't already exist.

        Matches the previous schema with a UNIQUE(name, date) constraint and no SERIAL.
        """
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER,
                date VARCHAR(255),
                firm VARCHAR(255),
                name VARCHAR(255),
                email VARCHAR(255),
                num_tickets INTEGER,
                achat VARCHAR(255),
                UNIQUE(name, date)
            )
            """
        )
        # Ensure backward compatibility for existing DBs
        self.ensure_achat_column()

    def ensure_achat_column(self) -> None:
        """Add 'achat' column if it does not exist (for existing DBs)."""
        try:
            self.execute("ALTER TABLE tickets ADD COLUMN achat VARCHAR(255)")
        except Error as e:
            # Column already exists, ignore error
            if "already exists" not in str(e):
                raise

    def insert_tickets(self, ticket_rows: Iterable[Dict[str, Any]]) -> int:
        """Insert ticket rows; duplicates (per UNIQUE constraint) are ignored.

        Returns number of actually inserted rows (excludes ignored duplicates).
        """
        inserted_count = 0
        for row in ticket_rows:
            if "achat" not in row:
                row["achat"] = None
            try:
                self.execute(
                    """
                    INSERT INTO tickets (id, date, firm, name, email, num_tickets, achat)
                    VALUES (%(id)s, %(date)s, %(firm)s, %(name)s, %(email)s, %(num_tickets)s, %(achat)s)
                    """,
                    row,
                )
                inserted_count += 1
            except Error as e:
                # Ignore duplicate key errors
                if "duplicate key" not in str(e).lower():
                    raise
        return inserted_count

    def insert_single_order(self, order_data: Dict[str, Any]) -> bool:
        """Insert a single order into the database.

        Args:
            order_data: Dictionary containing order data with keys:
                       date, name, email, firm, num_tickets, achat

        Returns:
            True if insertion was successful, False otherwise.
        """
        try:
            # Prepare the insert statement
            columns = list(order_data.keys())
            placeholders = ", ".join([f"%({col})s" for col in columns])
            insert_query = (
                f"INSERT INTO tickets ({', '.join(columns)}) VALUES ({placeholders})"
            )

            self.execute(insert_query, order_data)
            return True
        except Exception as e:
            print(f"Error inserting order: {e}")
            return False

    def fetch_tickets(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return tickets as a list of dictionaries ordered by date desc.

        Args:
            limit: Optional row limit.
        """
        base_query = "SELECT id, date, firm, name, email, num_tickets, achat FROM tickets ORDER BY date DESC"
        params: Tuple[Any, ...] = tuple()
        if limit is not None:
            base_query += " LIMIT %s"
            params = (limit,)

        cursor = self.execute(base_query, params if params else None)
        columns = [col[0] for col in cursor.description]
        results: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            results.append({col: row[idx] for idx, col in enumerate(columns)})
        return results

    # --- id assignment ---------------------------------------------------
    def get_max_id_and_span(self) -> Tuple[Optional[int], Optional[int]]:
        """Return (max_id, num_tickets_of_max_id_row).

        If table is empty, returns (None, None).
        """
        cursor = self.execute(
            "SELECT id, num_tickets FROM tickets WHERE id IS NOT NULL ORDER BY id DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            return None, None
        return int(row[0]), int(row[1])

    def assign_id_for_row(self, row_date: str, row_name: str, new_id: int) -> None:
        """Set the id for a row identified by unique key (name, date)."""
        self.execute(
            "UPDATE tickets SET id = %(new_id)s WHERE name = %(name)s AND date = %(date)s",
            {"new_id": new_id, "name": row_name, "date": row_date},
        )

    def update_achat_for_row(
        self, row_date: str, row_name: str, achat_value: Optional[str]
    ) -> None:
        """Update the 'achat' column for a row identified by unique key (name, date)."""
        self.execute(
            "UPDATE tickets SET achat = %(achat)s WHERE name = %(name)s AND date = %(date)s",
            {"achat": achat_value, "name": row_name, "date": row_date},
        )

    def fetch_orders_with_assigned_ids(self) -> List[Dict[str, Any]]:
        """Return orders that have an assigned starting ticket id (id IS NOT NULL)."""
        cursor = self.execute(
            "SELECT id, num_tickets, achat, name, email, date, firm FROM tickets WHERE id IS NOT NULL ORDER BY id ASC"
        )
        columns = [col[0] for col in cursor.description]
        return [
            {col: row[idx] for idx, col in enumerate(columns)}
            for row in cursor.fetchall()
        ]

    def remove_tickets(self, ticket_ids: Optional[Iterable[int]] = None) -> None:
        """Remove tickets from the database.

        Args:
            ticket_ids: Optional iterable of ticket IDs to remove.
        """
        if ticket_ids is None:
            self.execute("DELETE FROM tickets")
        else:
            # Convert to list for proper parameterization
            ticket_ids_list = list(ticket_ids)
            placeholders = ", ".join(["%s"] * len(ticket_ids_list))
            self.execute(
                f"DELETE FROM tickets WHERE id IN ({placeholders})", ticket_ids_list
            )

    def delete_order_by_name_date(self, row_date: str, row_name: str) -> None:
        """Delete an order identified by unique key (name, date).

        This is useful for removing orders that don't have IDs yet.
        """
        self.execute(
            "DELETE FROM tickets WHERE name = %(name)s AND date = %(date)s",
            {"name": row_name, "date": row_date},
        )

    @classmethod
    def close_pool(cls) -> None:
        """Close the connection pool. Call this when shutting down the application."""
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None

    @classmethod
    def get_pool_status(cls) -> Dict[str, Any]:
        """Get connection pool status for monitoring."""
        if cls._pool is None:
            return {"status": "not_initialized"}

        return {
            "status": "active",
            "minconn": cls._pool.minconn,
            "maxconn": cls._pool.maxconn,
            "current_connections": len(cls._pool._used),
            "available_connections": len(cls._pool._pool),
        }

    @classmethod
    def test_connection(cls) -> bool:
        """Test if the connection pool can establish a working connection."""
        try:
            with cls() as db:
                db.execute("SELECT 1")
                return True
        except Exception:
            return False


# Keep backward compatibility
SqliteClient = PostgresClient
