from __future__ import annotations

import io
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from parse_jimdo import JimdoOrderParser
from sql_client import SqliteClient


DATABASE_PATH = "lottery_sales.db"
DEFAULT_ARTICLE = "Billet de tombola / Raffle ticket 2024"


def ingest_uploaded_file(uploaded_file: io.BytesIO, article_name: str) -> int:
    # Pandas can read from file-like object
    df = pd.read_excel(uploaded_file, skiprows=[0])
    # Save to a temporary Excel in-memory buffer only for the existing parser API
    # Instead, re-use parser logic directly on df to avoid disk IO
    parser = JimdoOrderParser(article_name=article_name)

    # Reuse parser behavior by writing a small helper that emulates parse over df
    # so we don't need to touch the class' public API.
    def parse_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
        # Keep this in sync with JimdoOrderParser.parse_file logic
        df_filtered = df[df["Article"] == article_name]
        rows: List[Dict[str, Any]] = []
        import re

        for _, row in df_filtered.iterrows():
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
                    "date": pd.to_datetime(row.get("Date de commande")).strftime("%Y-%m-%d %H:%M:%S"),
                    "firm": str(row.get("Entreprise pour facturation", "")).strip() or None,
                    "name": name,
                    "email": str(row.get("Email pour facturation", "")).strip(),
                    "num_tickets": num_tickets,
                }
            )
        return rows

    ticket_rows = parse_df(df)

    with SqliteClient(DATABASE_PATH) as db:
        db.create_tickets_table()
        inserted = db.insert_tickets(ticket_rows)
    return inserted


def main() -> None:
    st.set_page_config(page_title="Tombola Tickets", page_icon="🎟️", layout="wide")
    st.title("Tombola - Import and Browse Tickets")

    with st.sidebar:
        st.header("Import")
        article = DEFAULT_ARTICLE
        uploaded = st.file_uploader("Upload Jimdo Excel export", type=["xlsx"]) 
        if uploaded is not None:
            if st.button("Ingest into database"):
                try:
                    inserted = ingest_uploaded_file(uploaded, article)
                    st.success(f"Inserted {inserted} row(s) into the database")
                except Exception as e:
                    st.error(f"Failed to ingest: {e}")

    st.header("Tickets")
    with SqliteClient(DATABASE_PATH) as db:
        db.create_tickets_table()
        rows = db.fetch_tickets()

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No tickets in database yet. Upload an Excel file to get started.")


if __name__ == "__main__":
    main()


