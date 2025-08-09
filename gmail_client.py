from __future__ import annotations

import base64
import os
from email.message import EmailMessage
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class GmailEmailClient:
    """Send emails using the Gmail API with OAuth credentials.

    Required env vars:
      - GMAIL_CREDENTIALS_FILE: path to OAuth client secrets JSON (from Google Cloud Console)
      - GMAIL_TOKEN_FILE: path to store OAuth token JSON (created after first auth)
      - SENDER_EMAIL: the Gmail address to send from

    Behavior controlled by:
      - IS_PROD ("true"/"1" to enable real sending to recipient email)
      - TEST_RECIPIENT (fallback recipient when IS_PROD is false)
    """

    def __init__(self) -> None:
        self.credentials_file = os.getenv("GMAIL_CREDENTIALS_FILE")
        self.token_file = os.getenv("GMAIL_TOKEN_FILE", "token.json")
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.is_prod = os.getenv("IS_PROD", "false").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        self.test_recipient = os.getenv("TEST_RECIPIENT")

        if not self.credentials_file:
            raise RuntimeError("Missing env var GMAIL_CREDENTIALS_FILE")
        if not self.sender_email:
            raise RuntimeError("Missing env var SENDER_EMAIL")

        self.creds = self._load_credentials()
        self.service = build("gmail", "v1", credentials=self.creds)

    def _load_credentials(self) -> Credentials:
        creds: Optional[Credentials] = None
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, GMAIL_SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(self.token_file, "w") as token:
                token.write(creds.to_json())
        return creds

    def _compute_recipient(self, db_email: str) -> str:
        if self.is_prod:
            return db_email
        return self.test_recipient or self.sender_email  # type: ignore[return-value]

    def send_ticket_email(
        self,
        db_email: str,
        name: str,
        num_tickets: int,
        ticket_start_id: int,
    ) -> None:
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
