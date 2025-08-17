from __future__ import annotations

import base64
import json
import os
from email.message import EmailMessage
from typing import Optional

import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class GmailEmailClient:
    """Send emails using the Gmail API with OAuth credentials.

    Required env vars:
      - GMAIL_CREDENTIALS_JSON: OAuth client secrets JSON string (from Google Cloud Console)
      - SENDER_EMAIL: the Gmail address to send from

    Behavior controlled by:
      - IS_PROD ("true"/"1" to enable real sending to recipient email)
      - TEST_RECIPIENT (fallback recipient when IS_PROD is false)
    """

    def __init__(self) -> None:
        self.credentials_json = os.getenv("GMAIL_CREDENTIALS_JSON")
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.is_prod = os.getenv("IS_PROD", "false").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        self.test_recipient = os.getenv("TEST_RECIPIENT")

        if not self.credentials_json:
            raise RuntimeError("Missing env var GMAIL_CREDENTIALS_JSON")
        if not self.sender_email:
            raise RuntimeError("Missing env var SENDER_EMAIL")

        self.creds = self._load_credentials()
        self.service = build("gmail", "v1", credentials=self.creds)

    def _get_stored_token(self) -> Optional[dict]:
        """Get token from Streamlit session state."""
        return st.session_state.get("gmail_token")

    def _store_token(self, token_data: dict) -> None:
        """Store token in Streamlit session state."""
        st.session_state["gmail_token"] = token_data

    def _load_credentials(self) -> Credentials:
        """Load credentials from session state or create new ones."""
        creds: Optional[Credentials] = None

        # Try to load from session state
        token_data = self._get_stored_token()
        if token_data:
            creds = Credentials.from_authorized_user_info(token_data, GMAIL_SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Store the refreshed token
                self._store_token(json.loads(creds.to_json()))
            else:
                # Parse the credentials JSON from environment variable
                credentials_info = json.loads(self.credentials_json)

                # Create OAuth flow and run it
                flow = InstalledAppFlow.from_client_config(
                    credentials_info, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)

                # Store the new token
                self._store_token(json.loads(creds.to_json()))

        return creds

    def _compute_recipient(self, db_email: str) -> str:
        if self.is_prod:
            return db_email
        return self.test_recipient or self.sender_email

    def send_ticket_email(
        self,
        db_email: str,
        name: str,
        num_tickets: int,
        ticket_start_id: int,
    ) -> None:
        """Send ticket email."""
        to_email = self._compute_recipient(db_email)
        ticket_end_id = ticket_start_id + num_tickets - 1

        subject = "Vos billets de tombola / Your raffle tickets"
        body = (
            f"Bonjour {name},\n\n"
            f"Merci pour votre achat. Voici vos numéros de billets: {ticket_start_id} à {ticket_end_id}.\n"
            f"Nombre de billets: {num_tickets}.\n\n"
            f"Ceci est un email {'de production' if self.is_prod else 'de test (redirigé)'}."
        )

        message = EmailMessage()
        message["From"] = self.sender_email
        message["To"] = to_email
        message["Subject"] = subject
        message.set_content(body)

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": encoded_message}

        self.service.users().messages().send(userId="me", body=create_message).execute()

    def is_authorized(self) -> bool:
        """Check if the client is properly authorized."""
        return self.creds is not None and self.creds.valid

    def get_authorization_status(self) -> dict:
        """Get the current authorization status."""
        if not self.creds:
            return {"status": "not_initialized", "message": "No credentials available"}

        if not self.creds.valid:
            if self.creds.expired:
                return {"status": "expired", "message": "Credentials expired"}
            else:
                return {"status": "invalid", "message": "Invalid credentials"}

        return {
            "status": "authorized",
            "message": "Fully authorized",
            "email": self.sender_email,
            "expires_at": self.creds.expiry.isoformat() if self.creds.expiry else None,
        }
