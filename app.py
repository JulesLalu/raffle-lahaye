from __future__ import annotations

import io
import os

import pandas as pd
import streamlit as st

from parse_jimdo import JimdoOrderParser
from sql_client import PostgresClient
from gmail_client import GmailEmailClient
from google_auth import init_google_auth
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


DEFAULT_ARTICLE_TYPE1 = "Billet de tombola / Raffle ticket 2024"
DEFAULT_ARTICLE_TYPE2 = "Tikkie tombola only!"

STARTING_TICKET_ID = int(os.getenv("STARTING_TICKET_ID", "1"))


def create_firm_statistics(orders: list) -> dict:
    """Create firm statistics showing total tickets distributed per firm."""
    firm_stats = {}

    for order in orders:
        firm = order.get("firm", "").strip()
        if not firm:
            firm = "No Firm"  # Handle empty firm names

        num_tickets = int(order.get("num_tickets", 0))
        firm_stats[firm] = firm_stats.get(firm, 0) + num_tickets

    return firm_stats


def ingest_uploaded_file(
    uploaded_file: io.BytesIO,
    article_name_type1: str,
    article_name_type2: str,
    min_date: pd.Timestamp = None,
) -> int:
    parser = JimdoOrderParser(
        article_name_type1=article_name_type1, article_name_type2=article_name_type2
    )
    ticket_rows = parser.parse_file(uploaded_file, min_date=min_date)

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
        st.toast(st.session_state.pop("flash_success"), icon="✅")
    if "flash_error" in st.session_state:
        st.toast(st.session_state.pop("flash_error"), icon="❌")

    # Ensure connection pool is properly closed when the app shuts down
    import atexit

    atexit.register(PostgresClient.close_pool)

    with st.sidebar:
        # Authentication Status
        st.header("🔐 Authentication")
        auth_status = auth.get_auth_status()
        if auth_status["status"] == "authenticated":
            st.success("✅ Authenticated & Authorized")
        else:
            st.warning("⚠️ Not authenticated")

        st.markdown("---")

        st.header("Import")
        article_type1 = os.getenv("ARTICLE_NAME_TYPE1", DEFAULT_ARTICLE_TYPE1)
        article_type2 = os.getenv("ARTICLE_NAME_TYPE2", DEFAULT_ARTICLE_TYPE2)

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

        uploaded = st.file_uploader("Upload Jimdo Excel export", type=["xlsx", "csv"])
        if uploaded is not None:
            if st.button("🚀 Ingest into database", type="primary"):
                try:
                    # Convert date to pandas timestamp for filtering
                    min_date_ts = pd.to_datetime(min_date)
                    inserted = ingest_uploaded_file(
                        uploaded, article_type1, article_type2, min_date=min_date_ts
                    )
                    st.success(
                        f"✅ Successfully inserted {inserted} order(s) into the database"
                    )
                except Exception as e:
                    st.error(f"❌ Failed to ingest: {e}")

        st.markdown("---")

        # Manual order creation button (triggers popup)
        if st.button("➕ Add Order Manually", type="primary", use_container_width=True):
            st.session_state["show_add_order_modal"] = True

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
                    # Create firm statistics
                    firm_stats = create_firm_statistics(orders)

                    # Expand orders into per-ticket rows
                    records = []
                    for order in orders:
                        start_id = int(order["id"])  # starting ticket id
                        for offset in range(int(order["num_tickets"])):
                            ticket_id = start_id + offset
                            record = {
                                "Date": pd.to_datetime(order["date"]).date(),
                                "Achat": order.get("achat") or "",
                                "Ticket": f"TICKET_{ticket_id:04d}",
                                "Nom": order["name"],
                                "email": order["email"],
                                "firm": order.get("firm") or "",
                            }
                            records.append(record)

                    # Create the main DataFrame
                    export_df = pd.DataFrame(records)

                    # Add firm statistics as the first row
                    if firm_stats:
                        # Create a summary row with firm statistics
                        summary_row = {
                            "Date": "FIRM STATISTICS",
                            "Achat": "",
                            "Ticket": "",
                            "Nom": "",
                            "email": "",
                            "firm": "",
                        }

                        # Add firm statistics to the summary row
                        for firm_name, ticket_count in firm_stats.items():
                            summary_row[firm_name] = ticket_count

                        # Create a DataFrame for the summary row with all columns
                        summary_df = pd.DataFrame([summary_row])

                        # Ensure the summary row has all the same columns as the main DataFrame
                        for col in export_df.columns:
                            if col not in summary_df.columns:
                                summary_df[col] = ""

                        # Add firm statistics columns to the main DataFrame (filled with empty strings)
                        for firm_name in firm_stats.keys():
                            if firm_name not in export_df.columns:
                                export_df[firm_name] = ""

                        # Concatenate summary row at the top
                        export_df = pd.concat(
                            [summary_df, export_df], ignore_index=True
                        )

                    import io as _io

                    buf = _io.BytesIO()
                    export_df.to_excel(buf, index=False)
                    buf.seek(0)
                    st.download_button(
                        label="Download .xlsx",
                        data=buf.getvalue(),
                        file_name=f"tickets_export_{datetime.now().strftime('%d-%m-%Y')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            except Exception as e:
                st.error(f"Export failed: {e}")

    # Add Order Dialog
    @st.dialog("➕ Add New Order")
    def add_order_dialog():
        # Order form
        with st.form("add_order_dialog_form"):
            col1, col2 = st.columns(2)

            with col1:
                order_date = st.date_input(
                    "Order Date",
                    value=pd.to_datetime("2025-09-01").date(),
                    format="DD/MM/YYYY",
                    key="dialog_date",
                )
                customer_name = st.text_input(
                    "Customer Name", placeholder="John Doe", key="dialog_name"
                )
                customer_email = st.text_input(
                    "Customer Email", placeholder="john@example.com", key="dialog_email"
                )

            with col2:
                firm_name = st.text_input(
                    "Firm/Company", placeholder="Optional", key="dialog_firm"
                )
                num_tickets = st.number_input(
                    "Number of Tickets",
                    min_value=1,
                    value=1,
                    step=1,
                    key="dialog_tickets",
                )
                achat_value = st.text_input(
                    "Achat", placeholder="Optional", key="dialog_achat"
                )

            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                submitted = st.form_submit_button(
                    "➕ Add Order", type="primary", use_container_width=True
                )

            if submitted:
                if not customer_name or not customer_email:
                    st.error("❌ Name and email are required!")
                else:
                    try:
                        # Prepare order data
                        order_data = {
                            "id": None,  # No ID assigned yet
                            "date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "name": customer_name.strip(),
                            "email": customer_email.strip(),
                            "firm": firm_name.strip() if firm_name.strip() else None,
                            "num_tickets": int(num_tickets),
                            "achat": achat_value.strip()
                            if achat_value.strip()
                            else None,
                        }

                        # Insert into database
                        with PostgresClient() as db:
                            success = db.insert_single_order(order_data)

                        if success:
                            st.success(
                                f"✅ Order added successfully for {customer_name}"
                            )
                            st.session_state["show_add_order_modal"] = False
                            st.rerun()
                        else:
                            st.error("❌ Failed to add order. Please try again.")
                    except Exception as e:
                        st.error(f"❌ Error adding order: {e}")

    # Call the dialog when needed
    if st.session_state.get("show_add_order_modal", False):
        add_order_dialog()

    # Delete Confirmation Dialog
    @st.dialog("🗑️ Confirm Order Deletion")
    def delete_confirmation_dialog():
        # Confirmation details
        order_info = st.session_state["delete_confirmation"]
        st.warning(
            "⚠️ Are you sure you want to delete this order? This action cannot be undone."
        )

        # Order details
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Customer:** {order_info['name']}")
            st.info(f"**Email:** {order_info['email']}")
        with col2:
            st.info(f"**Date:** {order_info['date']}")
            st.info(f"**Tickets:** {order_info['num_tickets']}")

        # Confirmation buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("❌ Cancel", key="cancel_delete", use_container_width=True):
                st.session_state["delete_confirmation"] = None
                st.rerun()
        with col2:
            if st.button(
                "🗑️ Delete Order",
                key="confirm_delete",
                type="primary",
                use_container_width=True,
            ):
                try:
                    with PostgresClient() as db:
                        db.delete_order_by_name_date(
                            row_date=order_info["date"], row_name=order_info["name"]
                        )
                    st.session_state["flash_success"] = (
                        f"Order for {order_info['name']} deleted successfully."
                    )
                    st.session_state["delete_confirmation"] = None
                    st.rerun()
                except Exception as e:
                    st.session_state["flash_error"] = f"Failed to delete order: {e}"
                    st.rerun()

    # Call the dialog when needed
    if st.session_state.get("delete_confirmation"):
        delete_confirmation_dialog()

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
            cols = st.columns([2, 2, 3, 3, 2, 2, 2, 2, 1])
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
                            start_id = STARTING_TICKET_ID
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

            # Delete button (only for orders without IDs)
            if cols[8].button("🗑️", key=f"delete_no_id_{idx}", help="Delete this order"):
                # Store order info for confirmation
                st.session_state["delete_confirmation"] = {
                    "name": row["name"],
                    "date": row["date"],
                    "email": row["email"],
                    "num_tickets": row["num_tickets"],
                }
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
