from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd
import os


class JimdoOrderParser:
    """Parse Jimdo export and prepare rows for the tickets database."""

    def __init__(self, article_name: str) -> None:
        self.article_name = article_name

    def parse_file(
        self, excel_path: str, min_date: pd.Timestamp = None
    ) -> List[Dict[str, Any]]:
        # Read the Excel file without skipping any rows
        df = pd.read_excel(excel_path)

        # Find the header row by looking for expected column names
        expected_columns = [
            "Article",
            "Date de commande",
            "Nom pour facturation",
            "Prénom pour facturation",
            "Email pour facturation",
        ]
        header_row_index = None

        # Search through the first 10 rows to find the header
        for i in range(min(10, len(df))):
            row_values = df.iloc[i].astype(str).str.lower().tolist()
            row_text = " ".join(row_values)

            # Check if this row contains the expected column names
            if any(col.lower() in row_text for col in expected_columns):
                header_row_index = i
                break

        # If we found a header row, use it
        if header_row_index is not None:
            # Skip all rows before the header
            df = pd.read_excel(excel_path, skiprows=list(range(header_row_index + 1)))
        else:
            # Fallback: assume header is on first row
            pass

        return self.parse_dataframe(df, min_date=min_date)

    def parse_dataframe(
        self, df: pd.DataFrame, min_date: pd.Timestamp = None
    ) -> List[Dict[str, Any]]:
        # Filter relevant article
        df = df[df["Article"] == self.article_name]

        # Filter by date if min_date is provided
        if min_date is not None:
            # Convert order dates to datetime for comparison
            df["Date de commande"] = pd.to_datetime(df["Date de commande"])
            df = df[df["Date de commande"] >= min_date]
            print(
                f"📅 Filtered orders from {min_date.strftime('%Y-%m-%d')} onwards: {len(df)} orders found"
            )

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
    article = os.getenv("ARTICLE_NAME", "Billet de tombola / Raffle ticket 2024")

    parser = JimdoOrderParser(article_name=article)

    # Example: Filter orders from September 1st, 2025 onwards
    min_date = pd.to_datetime("2025-09-01")
    parser.parse_file(excel_file, min_date=min_date)


if __name__ == "__main__":
    main()
