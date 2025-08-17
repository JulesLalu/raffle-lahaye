from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

from sql_client import SqliteClient


class JimdoOrderParser:
    """Parse Jimdo export and prepare rows for the tickets database."""

    def __init__(self, article_name: str) -> None:
        self.article_name = article_name

    def parse_file(self, excel_path: str) -> List[Dict[str, Any]]:
        df = pd.read_excel(excel_path, skiprows=[0])
        return self.parse_dataframe(df)

    def parse_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # Filter relevant article
        df = df[df["Article"] == self.article_name]

        rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            match = re.search(r"(\d+)", str(row.get("Déclinaison", "")))
            if not match:
                continue

            num_tickets = int(match.group(1))

            last_name = str(row.get("Nom pour facturation", "")).strip()
            first_name = str(row.get("Prénom pour facturation", "")).strip()
            name = f"{last_name} {first_name}".strip()

            rows.append(
                {
                    "id": None,
                    "date": pd.to_datetime(row.get("Date de commande")).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "firm": str(row.get("Entreprise pour facturation", "")).strip()
                    or None,
                    "name": name,
                    "email": str(row.get("Email pour facturation", "")).strip(),
                    "num_tickets": num_tickets,
                    "achat": None,
                }
            )

        return rows


def main() -> None:
    excel_file = "./boutique_jimdo.xlsx"
    database_path = "lottery_sales.db"
    article = "Billet de tombola / Raffle ticket 2024"

    parser = JimdoOrderParser(article_name=article)
    ticket_rows = parser.parse_file(excel_file)

    with SqliteClient(database_path) as db:
        db.create_tickets_table()
        inserted = db.insert_tickets(ticket_rows)

    print(f"Inserted {inserted} order(s) into the database.")


if __name__ == "__main__":
    main()
