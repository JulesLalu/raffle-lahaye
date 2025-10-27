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
                "n_number": "N°",
            },
            "type2": {
                "article": "Page",
                "date": "Date",
                "last_name": "Nom",
                "first_name": None,  # Combined with last_name in "Nom"
                "email": "Message",
                "declinaison": "Company",
                "firm": "E-mail",
                "n_number": None,  # Not available in Type2
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
            # Skip all rows before the header, but keep the header row itself
            if isinstance(file_input, str):
                # File path - detect by extension
                file_extension = file_input.lower().split(".")[-1]
                if file_extension == "xlsx":
                    df = pd.read_excel(
                        file_input, skiprows=list(range(header_row_index))
                    )
                else:  # csv
                    df = pd.read_csv(
                        file_input,
                        skiprows=list(range(header_row_index)),
                        header=0,  # Use first row as header
                    )
            else:
                # BytesIO object - try Excel first, then CSV
                file_input.seek(0)
                try:
                    df = pd.read_excel(
                        file_input, skiprows=list(range(header_row_index))
                    )
                except Exception:
                    file_input.seek(0)
                    df = pd.read_csv(
                        file_input,
                        skiprows=list(range(header_row_index)),
                        header=0,  # Use first row as header
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

        # Get actual column names from the dataframe (case-insensitive matching)
        # and map them to standard fields
        df_cols = df.columns.tolist()
        df_cols_lower = [col.lower() for col in df_cols]

        # Find column mappings by searching in actual column names
        actual_mapping = {}
        for field, expected_col in mapping.items():
            if expected_col is None:
                actual_mapping[field] = None
            else:
                # Try to find the column (case-insensitive)
                for idx, col_lower in enumerate(df_cols_lower):
                    if expected_col == col_lower:
                        actual_mapping[field] = df_cols[idx]
                        break
                else:
                    # If not found in the search, try exact match
                    actual_mapping[field] = (
                        expected_col if expected_col in df_cols else None
                    )

        # Create unified DataFrame with standardized columns
        unified_data = []

        for _, row in df.iterrows():
            unified_row = {}

            # Map each field using the actual column names found in the dataframe
            for field, source_col in actual_mapping.items():
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

            # Set achat value based on file type
            if file_type == "type1":
                # Type1: use the N° column value
                achat_value = str(row.get("n_number", "")).strip() or None
            else:
                # Type2: use 'T'
                achat_value = "T"

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
                    "achat": achat_value,
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
