from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from parse_jimdo import JimdoOrderParser
from sql_client import PostgresClient
from gmail_client import GmailEmailClient
from dotenv import load_dotenv

load_dotenv()


DEFAULT_ARTICLE = "Billet de tombola / Raffle ticket 2024"


def ingest_uploaded_file(uploaded_file: io.BytesIO, article_name: str) -> int:
    df = pd.read_excel(uploaded_file, skiprows=[0])
    parser = JimdoOrderParser(article_name=article_name)
    ticket_rows = parser.parse_dataframe(df)

    with PostgresClient() as db:
        db.create_tickets_table()
        inserted = db.insert_tickets(ticket_rows)
    return inserted


def main() -> None:
    st.set_page_config(page_title="Tombola Tickets", page_icon="🎟️", layout="wide")
    st.title("Tombola - Import and Browse Tickets")

    # Flash messages persisted across reruns
    if "flash_success" in st.session_state:
        st.success(st.session_state.pop("flash_success"))
    if "flash_error" in st.session_state:
        st.error(st.session_state.pop("flash_error"))

    # Ensure connection pool is properly closed when the app shuts down
    import atexit

    atexit.register(PostgresClient.close_pool)

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

        st.header("Export")

        if st.button("Download Excel (one row per ticket)"):
            try:
                with PostgresClient() as db:
                    db.create_tickets_table()
                    orders = db.fetch_orders_with_assigned_ids()
                if not orders:
                    st.warning("No orders with assigned IDs to export.")
                else:
                    # Expand orders into per-ticket rows
                    records = []
                    for order in orders:
                        start_id = int(order["id"])  # starting ticket id
                        for offset in range(int(order["num_tickets"])):
                            ticket_id = start_id + offset
                            records.append(
                                {
                                    "Date": pd.to_datetime(order["date"]).date(),
                                    "Achat": order.get("achat") or "",
                                    "Ticket": f"TICKET_{ticket_id:04d}",
                                    "Nom": order["name"],
                                    "email": order["email"],
                                    "firm": order.get("firm") or "",
                                }
                            )
                    export_df = pd.DataFrame(
                        records,
                        columns=["Date", "Achat", "Ticket", "Nom", "email", "firm"],
                    )
                    import io as _io

                    buf = _io.BytesIO()
                    export_df.to_excel(buf, index=False)
                    buf.seek(0)
                    st.download_button(
                        label="Download .xlsx",
                        data=buf.getvalue(),
                        file_name="tickets_export.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            except Exception as e:
                st.error(f"Export failed: {e}")

    st.header("Tickets")
    with PostgresClient() as db:
        db.create_tickets_table()
        rows = db.fetch_tickets()

    if not rows:
        st.info("No tickets in database yet. Upload an Excel file to get started.")
        return

    # Render table with action buttons per row
    for idx, row in enumerate(rows):
        cols = st.columns([2, 2, 3, 3, 2, 2, 2, 2])
        cols[0].markdown(f"**Date**\n\n{row['date']}")
        cols[1].markdown(f"**Name**\n\n{row['name']}")
        cols[2].markdown(f"**Email**\n\n{row['email']}")
        cols[3].markdown(f"**Firm**\n\n{row.get('firm') or ''}")
        cols[4].markdown(f"**Tickets**\n\n{row['num_tickets']}")
        cols[5].markdown(
            f"**ID**\n\n{row.get('id') if row.get('id') is not None else '-'}"
        )

        # Achat editor
        achat_val = cols[6].text_input(
            "Achat", value=row.get("achat") or "", key=f"achat_{idx}"
        )
        if cols[6].button("Save", key=f"save_achat_{idx}"):
            try:
                with PostgresClient() as db:
                    db.update_achat_for_row(
                        row_date=row["date"],
                        row_name=row["name"],
                        achat_value=achat_val or None,
                    )
                st.session_state["flash_success"] = "Achat updated."
                st.rerun()
            except Exception as e:
                st.session_state["flash_error"] = f"Failed to update Achat: {e}"
                st.rerun()

        has_id = row.get("id") is not None
        send_label = "Send email" if not has_id else "Resend"
        if cols[7].button(send_label, key=f"send_{idx}"):
            try:
                email_client = GmailEmailClient()

                # Determine starting ticket id
                if has_id:
                    start_id = int(row["id"])  # reuse existing id on resend
                else:
                    # Compute new id per rule: max(id) + num_tickets of max-id row
                    with PostgresClient() as db:
                        max_id, max_span = db.get_max_id_and_span()
                        if max_id is None:
                            start_id = 1
                        else:
                            start_id = max_id + (max_span or 0)

                # Send email (use starting ticket id)
                email_client.send_ticket_email(
                    db_email=row["email"],
                    name=row["name"],
                    num_tickets=int(row["num_tickets"]),
                    ticket_start_id=start_id,
                )

                # On success, assign id if not already assigned
                if not has_id:
                    with PostgresClient() as db:
                        db.assign_id_for_row(
                            row_date=row["date"], row_name=row["name"], new_id=start_id
                        )
                    st.session_state["flash_success"] = (
                        f"Email sent. Assigned ID {start_id} to this order."
                    )
                else:
                    st.session_state["flash_success"] = "Email re-sent."
                st.rerun()
            except Exception as e:
                st.session_state["flash_error"] = f"Failed to send email: {e}"
                st.rerun()


if __name__ == "__main__":
    main()
