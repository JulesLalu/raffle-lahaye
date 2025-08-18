from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from parse_jimdo import JimdoOrderParser
from sql_client import PostgresClient
from gmail_client import GmailEmailClient
from google_auth import init_google_auth
from dotenv import load_dotenv

load_dotenv()


DEFAULT_ARTICLE = "Billet de tombola / Raffle ticket 2024"


def ingest_uploaded_file(
    uploaded_file: io.BytesIO, article_name: str, min_date: pd.Timestamp = None
) -> int:
    df = pd.read_excel(uploaded_file, skiprows=[0])
    parser = JimdoOrderParser(article_name=article_name)
    ticket_rows = parser.parse_dataframe(df, min_date=min_date)

    with PostgresClient() as db:
        db.create_tickets_table()
        inserted = db.insert_tickets(ticket_rows)
    return inserted


def main() -> None:
    st.set_page_config(page_title="Tombola Tickets", page_icon="🎟️", layout="wide")

    # Initialize Google authentication
    auth = init_google_auth()
    if not auth:
        st.error(
            "❌ Authentication system unavailable. Please check your configuration."
        )
        return

    # Check if user is authenticated
    if not auth.require_auth():
        return

    # User is authenticated, show the main app
    st.title("Tombola - Import and Browse Tickets")

    if st.button("🚪 Logout"):
        auth.logout()

    # Flash messages persisted across reruns
    if "flash_success" in st.session_state:
        st.success(st.session_state.pop("flash_success"))
    if "flash_error" in st.session_state:
        st.error(st.session_state.pop("flash_error"))

    # Ensure connection pool is properly closed when the app shuts down
    import atexit

    atexit.register(PostgresClient.close_pool)

    with st.sidebar:
        # Authentication Status
        st.header("🔐 Authentication")
        auth_status = auth.get_auth_status()
        if auth_status["status"] == "authenticated":
            st.success("✅ Authenticated")
        else:
            st.warning("⚠️ Not authenticated")

        st.markdown("---")

        st.header("Import")
        article = DEFAULT_ARTICLE

        # Date filter for import
        st.subheader("📅 Date Filter")

        min_date = st.date_input(
            "Import orders from:",
            value=pd.to_datetime("2025-09-01").date(),
            format="DD/MM/YYYY",
            help="Orders before this date will be excluded from import",
        )

        # Show filter status
        st.caption(
            f"🔍 Will import orders from **{min_date.strftime('%d/%m/%Y')}** onwards"
        )

        uploaded = st.file_uploader("Upload Jimdo Excel export", type=["xlsx"])
        if uploaded is not None:
            if st.button("🚀 Ingest into database", type="primary"):
                try:
                    # Convert date to pandas timestamp for filtering
                    min_date_ts = pd.to_datetime(min_date)
                    inserted = ingest_uploaded_file(
                        uploaded, article, min_date=min_date_ts
                    )
                    st.success(
                        f"✅ Successfully inserted {inserted} order(s) into the database"
                    )
                except Exception as e:
                    st.error(f"❌ Failed to ingest: {e}")

                st.header("Export")

        # Gmail Status
        try:
            gmail_client = GmailEmailClient()
            auth_status = gmail_client.get_authorization_status()

        except Exception as e:
            st.error(f"❌ Gmail client error: {e}")

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

        # Fetch orders without IDs (priority display)
        orders_without_id = db.fetch_tickets()
        orders_without_id = [row for row in orders_without_id if row.get("id") is None]

        # Fetch orders with IDs (collapsible section)
        orders_with_id = db.fetch_orders_with_assigned_ids()

    # Summary statistics
    total_orders = len(orders_without_id) + len(orders_with_id)
    if total_orders > 0:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📧 Pending Emails", len(orders_without_id))
        with col2:
            st.metric("✅ Processed Orders", len(orders_with_id))
        with col3:
            st.metric("📊 Total Orders", total_orders)
        st.markdown("---")
    else:
        st.info("📭 No orders in database yet. Upload an Excel file to get started.")
        return

    # Show orders without IDs (priority - need emails)
    st.subheader(f"📧 Orders Needing Emails ({len(orders_without_id)})")

    if not orders_without_id:
        st.success("🎉 All orders have been processed! No emails pending.")
    else:
        st.info(
            "💡 These orders need emails sent. Click 'Send email' to assign ticket IDs and send emails."
        )

        # Render table for orders without IDs
        for idx, row in enumerate(orders_without_id):
            cols = st.columns([2, 2, 3, 3, 2, 2, 2, 2])
            cols[0].markdown(f"**Date**\n\n{row['date']}")
            cols[1].markdown(f"**Name**\n\n{row['name']}")
            cols[2].markdown(f"**Email**\n\n{row['email']}")
            cols[3].markdown(f"**Firm**\n\n{row.get('firm') or ''}")
            cols[4].markdown(f"**Tickets**\n\n{row['num_tickets']}")
            cols[5].markdown("**ID**\n\n-")

            # Achat editor
            achat_val = cols[6].text_input(
                "Achat", value=row.get("achat") or "", key=f"achat_no_id_{idx}"
            )
            if cols[6].button("Save", key=f"save_achat_no_id_{idx}"):
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

            # Send email button
            if cols[7].button("Send email", key=f"send_no_id_{idx}"):
                try:
                    email_client = GmailEmailClient()

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

                    # On success, assign id
                    with PostgresClient() as db:
                        db.assign_id_for_row(
                            row_date=row["date"], row_name=row["name"], new_id=start_id
                        )
                    st.session_state["flash_success"] = (
                        f"Email sent. Assigned ID {start_id} to this order."
                    )
                    st.rerun()
                except Exception as e:
                    st.session_state["flash_error"] = f"Failed to send email: {e}"
                    st.rerun()

    # Collapsible section for orders with IDs (already processed)
    if orders_with_id:
        st.info("💾 Some orders have already been processed. Click below to view them.")
        with st.expander(
            f"📋 Already Processed Orders ({len(orders_with_id)})", expanded=False
        ):
            st.info("These orders already have ticket IDs assigned and emails sent.")

            # Render table for orders with IDs
            for idx, row in enumerate(orders_with_id):
                cols = st.columns([2, 2, 3, 3, 2, 2, 2, 2])
                cols[0].markdown(f"**Date**\n\n{row['date']}")
                cols[1].markdown(f"**Name**\n\n{row['name']}")
                cols[2].markdown(f"**Email**\n\n{row['email']}")
                cols[3].markdown(f"**Firm**\n\n{row.get('firm') or ''}")
                cols[4].markdown(f"**Tickets**\n\n{row['num_tickets']}")
                cols[5].markdown(f"**ID**\n\n{row['id']}")

                # Achat editor
                achat_val = cols[6].text_input(
                    "Achat", value=row.get("achat") or "", key=f"achat_with_id_{idx}"
                )
                if cols[6].button("Save", key=f"save_achat_with_id_{idx}"):
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

                # Resend email button
                if cols[7].button("Resend", key=f"resend_with_id_{idx}"):
                    try:
                        email_client = GmailEmailClient()

                        # Use existing ID for resend
                        start_id = int(row["id"])

                        # Send email (use existing ticket id)
                        email_client.send_ticket_email(
                            db_email=row["email"],
                            name=row["name"],
                            num_tickets=int(row["num_tickets"]),
                            ticket_start_id=start_id,
                        )

                        st.session_state["flash_success"] = "Email re-sent."
                        st.rerun()
                    except Exception as e:
                        st.session_state["flash_error"] = f"Failed to send email: {e}"
                        st.rerun()


if __name__ == "__main__":
    main()
