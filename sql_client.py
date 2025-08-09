from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union


Params = Optional[Union[Dict[str, Any], Sequence[Any], Tuple[Any, ...]]]


class SqliteClient:
    """Thin convenience wrapper around sqlite3 with a few helper methods.

    Usage:
        with SqliteClient("lottery_sales.db") as db:
            db.create_tickets_table()
            inserted = db.insert_tickets([...])
    """

    def __init__(self, database_path: str) -> None:
        self.database_path: str = database_path
        self.connection: Optional[sqlite3.Connection] = None

    # --- context manager -------------------------------------------------
    def __enter__(self) -> "SqliteClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- connection lifecycle -------------------------------------------
    def connect(self) -> None:
        if self.connection is None:
            self.connection = sqlite3.connect(self.database_path)
            # Keep behavior close to default; enable FK constraints if needed later
            self.connection.execute("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        if self.connection is not None:
            self.connection.commit()
            self.connection.close()
            self.connection = None

    def _require_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError(
                "Database connection is not established. Call connect() or use context manager."
            )
        return self.connection

    # --- generic helpers -------------------------------------------------
    def execute(self, query: str, params: Params = None) -> sqlite3.Cursor:
        conn = self._require_connection()
        cursor = conn.cursor()
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)
        return cursor

    def executemany(
        self, query: str, seq_of_params: Iterable[Union[Dict[str, Any], Sequence[Any]]]
    ) -> sqlite3.Cursor:
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

        Matches the previous schema with a UNIQUE(name, date) constraint and no AUTOINCREMENT.
        """
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER,
                date TEXT,
                firm TEXT NULL,
                name TEXT,
                email TEXT,
                num_tickets INTEGER,
                UNIQUE(name, date)
            )
            """
        )

    def insert_tickets(self, ticket_rows: Iterable[Dict[str, Any]]) -> int:
        """Insert ticket rows; duplicates (per UNIQUE constraint) are ignored.

        Returns number of actually inserted rows (excludes ignored duplicates).
        """
        inserted_count = 0
        for row in ticket_rows:
            cursor = self.execute(
                """
                INSERT OR IGNORE INTO tickets (id, date, firm, name, email, num_tickets)
                VALUES (:id, :date, :firm, :name, :email, :num_tickets)
                """,
                row,
            )
            # rowcount is 1 when inserted, 0 when ignored
            if cursor.rowcount and cursor.rowcount > 0:
                inserted_count += cursor.rowcount
        # Ensure data is flushed
        self._require_connection().commit()
        return inserted_count

    def fetch_tickets(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return tickets as a list of dictionaries ordered by date desc.

        Args:
            limit: Optional row limit.
        """
        base_query = "SELECT id, date, firm, name, email, num_tickets FROM tickets ORDER BY date DESC"
        params: Tuple[Any, ...] = tuple()
        if limit is not None:
            base_query += " LIMIT ?"
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
            "UPDATE tickets SET id = :new_id WHERE name = :name AND date = :date",
            {"new_id": new_id, "name": row_name, "date": row_date},
        )
        self._require_connection().commit()

    def remove_tickets(self, ticket_ids: Optional[Iterable[int]] = None) -> None:
        """Remove tickets from the database.

        Args:
            ticket_ids: Optional iterable of ticket IDs to remove.
        """
        if ticket_ids is None:
            self.execute("DELETE FROM tickets")
        else:
            self.execute("DELETE FROM tickets WHERE id IN (?)", (ticket_ids,))
