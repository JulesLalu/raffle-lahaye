from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd
import os


class JimdoOrderParser:
    """Parse Jimdo export and prepare rows for the tickets database."""

    def __init__(self, article_name_type1: str, article_name_type2: str) -> None:
        self.article_name_type1 = article_name_type1
        self.article_name_type2 = article_name_type2

        # Define column mappings for different file types
        self.column_mappings = {
            "type1": {
                "article": "Article",
                "date": "Date de commande",
                "last_name": "Nom pour facturation",
                "first_name": "Prénom pour facturation",
                "email": "Email pour facturation",
                "declinaison": "Déclinaison",
                "firm": "Entreprise pour facturation",
            },
            "type2": {
                "article": "Page",
                "date": "Date",
                "last_name": "Nom",
                "first_name": None,  # Combined with last_name in "Nom"
                "email": "Message",
                "declinaison": "Company",
                "firm": "E-mail",
            },
        }

    def parse_file(
        self, file_input, min_date: pd.Timestamp = None
    ) -> List[Dict[str, Any]]:
        # Handle both file paths (str) and BytesIO objects
        if isinstance(file_input, str):
            # File path provided - detect by extension
            file_extension = file_input.lower().split(".")[-1]
            if file_extension == "xlsx":
                df = pd.read_excel(file_input)
            elif file_extension == "csv":
                df = pd.read_csv(file_input, header=None)
            else:
                raise ValueError(
                    f"Unsupported file type: {file_extension}. Only .xlsx and .csv files are supported."
                )
        else:
            # BytesIO object provided - detect by content
            # Reset the BytesIO position to the beginning
            file_input.seek(0)

            # Try to read as Excel first (more specific)
            try:
                df = pd.read_excel(file_input)
                file_input.seek(0)  # Reset for potential CSV fallback
            except Exception:
                # If Excel fails, try CSV
                file_input.seek(0)
                try:
                    df = pd.read_csv(file_input, header=None)
                except Exception as e:
                    raise ValueError(f"Could not read file as Excel or CSV: {e}")
        # Find the header row by looking for expected column names
        # Define expected columns for both file types (case-insensitive)
        # Type1 has more specific column names
        type1_columns = [
            "article",
            "date de commande",
            "nom pour facturation",
            "prénom pour facturation",
            "email pour facturation",
        ]
        # Type2 has simpler column names
        type2_columns = ["page", "date", "nom", "e-mail", "message", "company"]

        header_row_index = None
        file_type = None

        # Search through the first 10 rows to find the header
        for i in range(min(10, len(df))):
            row_values = df.iloc[i].astype(str).str.lower().tolist()
            row_text = " ".join(row_values)

            # Check if this row contains type1 column names
            type1_matches = sum(1 for col in type1_columns if col.lower() in row_text)
            type2_matches = sum(1 for col in type2_columns if col.lower() in row_text)

            # Check for type1 first (more specific for Excel files) - need at least 4 matching columns
            if type1_matches >= 4:
                header_row_index = i
                file_type = "type1"
                break
            elif type2_matches >= 3:
                header_row_index = i
                file_type = "type2"
                break
        # If we found a header row, use it
        if header_row_index is not None:
            # Skip all rows before the header
            if isinstance(file_input, str):
                # File path - detect by extension
                file_extension = file_input.lower().split(".")[-1]
                if file_extension == "xlsx":
                    df = pd.read_excel(
                        file_input, skiprows=list(range(header_row_index + 1))
                    )
                else:  # csv
                    df = pd.read_csv(
                        file_input,
                        skiprows=list(range(header_row_index + 1)),
                        header=None,
                    )
            else:
                # BytesIO object - try Excel first, then CSV
                file_input.seek(0)
                try:
                    df = pd.read_excel(
                        file_input, skiprows=list(range(header_row_index + 1))
                    )
                except Exception:
                    file_input.seek(0)
                    df = pd.read_csv(
                        file_input,
                        skiprows=list(range(header_row_index + 1)),
                        header=None,
                    )
        else:
            # Fallback: assume header is on first row
            file_type = "type1"  # Default to type1

        # Create unified DataFrame with standardized column names
        unified_df = self._create_unified_dataframe(df, file_type)

        return self.parse_dataframe(unified_df, min_date=min_date, file_type=file_type)

    def _create_unified_dataframe(
        self, df: pd.DataFrame, file_type: str
    ) -> pd.DataFrame:
        """Create a unified DataFrame with standardized column names."""
        mapping = self.column_mappings[file_type]

        # First, set the proper column names based on file type
        if file_type == "type1":
            # Type1 columns - use the actual column names from the Excel file
            df.columns = [
                "N°",
                "N° d'article",
                "Date de commande",
                "Mode de paiement",
                "Article",
                "Déclinaison",
                "Prix à l'unité net",
                "Prix net",
                "Prix brut",
                "Devise",
                "Date d'envoi",
                "Entreprise pour facturation",
                "Titre pour facturation",
                "Nom pour facturation",
                "Prénom pour facturation",
                "Email pour facturation",
                "Remarque (facture)",
                "N° TVA Intracommunautaire",
                "Date de naissance",
                "Numéro client",
                "Entreprise pour livraison",
                "Titre pour livraison",
                "Nom pour livraison",
                "Deuxième prénom pour livraison",
                "Prénom pour livraison",
                "Complément d'adresse (livraison)",
                "Rue pour livraison",
                "Code postal pour livraison",
                "Ville pour livraison",
                "Région/province pour livraison",
                "Pays pour livraison",
                "Téléphone pour livraison",
                "E-Mail pour livraison",
                "Remarque (livraison)",
            ] + [f"Extra_{i}" for i in range(len(df.columns) - 34)]
        elif file_type == "type2":
            # Type2 columns
            df.columns = ["Date", "Page", "Nom", "E-mail", "Message", "Company"] + [
                f"Extra_{i}" for i in range(len(df.columns) - 6)
            ]

        # Create unified DataFrame with standardized columns
        unified_data = []

        for _, row in df.iterrows():
            unified_row = {}

            # Map each field using the appropriate column mapping
            for field, source_col in mapping.items():
                if source_col is not None and source_col in df.columns:
                    unified_row[field] = row[source_col]
                else:
                    unified_row[field] = None

            unified_data.append(unified_row)

        return pd.DataFrame(unified_data)

    def parse_dataframe(
        self, df: pd.DataFrame, min_date: pd.Timestamp = None, file_type: str = "type1"
    ) -> List[Dict[str, Any]]:
        # Filter relevant article
        if file_type == "type1":
            df = df[df["article"] == self.article_name_type1]
        elif file_type == "type2":
            df = df[df["article"] == self.article_name_type2]

        # Filter by date if min_date is provided
        if min_date is not None:
            # Convert order dates to datetime for comparison
            df = df.copy()  # Avoid SettingWithCopyWarning
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= min_date]
            print(
                f"📅 Filtered orders from {min_date.strftime('%Y-%m-%d')} onwards: {len(df)} orders found"
            )

        rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            match = re.search(r"(\d+)", str(row.get("declinaison", "")))
            if not match:
                continue

            num_tickets = int(match.group(1))

            # Handle name construction based on file type
            if file_type == "type1":
                # Type1: separate first and last name columns
                last_name = str(row.get("last_name", "")).strip()
                first_name = str(row.get("first_name", "")).strip()
                name = f"{last_name} {first_name}".strip()
            else:
                # Type2: combined name in single column
                name = str(row.get("last_name", "")).strip()

            rows.append(
                {
                    "id": None,
                    "date": pd.to_datetime(row.get("date")).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "firm": str(row.get("firm", "")).strip() or None,
                    "name": name,
                    "email": str(row.get("email", "")).strip(),
                    "num_tickets": num_tickets,
                    "achat": None,
                }
            )

        return rows


def main() -> None:
    # Test with both file types
    excel_file = "./boutique_jimdo.xlsx"
    csv_file = "./type_2.csv"

    # Example: Filter orders from 2024 onwards
    min_date = pd.to_datetime("2024-01-01")
    article_name_type1 = "Billet de tombola / Raffle ticket 2024"
    article_name_type2 = "Tikkie tombola only!"

    # Test Excel file if it exists
    if os.path.exists(excel_file):
        print(f"📊 Processing Excel file: {excel_file}")
        parser = JimdoOrderParser(
            article_name_type1=article_name_type1, article_name_type2=article_name_type2
        )
        parser.parse_file(excel_file, min_date=min_date)

    # Test CSV file if it exists
    if os.path.exists(csv_file):
        print(f"📊 Processing CSV file: {csv_file}")
        parser = JimdoOrderParser(
            article_name_type1=article_name_type1, article_name_type2=article_name_type2
        )
        parser.parse_file(csv_file, min_date=min_date)


if __name__ == "__main__":
    main()
