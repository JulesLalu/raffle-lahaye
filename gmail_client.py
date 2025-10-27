from __future__ import annotations

import base64
import json
import os
from email.mime.image import MIMEImage
from typing import Optional

import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
]


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
        return st.session_state.get("gmail_token") or st.session_state.get(
            "google_auth_token"
        )

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
        """Send ticket email with professional HTML formatting."""
        to_email = self._compute_recipient(db_email)
        ticket_end_id = ticket_start_id + num_tickets - 1

        subject = "Vos billets de tombola / Your raffle tickets"

        # Create HTML email body with professional styling
        html_body = self._create_html_email_body(
            name, num_tickets, ticket_start_id, ticket_end_id
        )

        # Create plain text version as fallback
        text_body = self._create_text_email_body(
            name, num_tickets, ticket_start_id, ticket_end_id
        )

        # Create multipart message with both HTML and plain text
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # Create the main alternative multipart (HTML vs plain text)
        message = MIMEMultipart("alternative")
        message["From"] = self.sender_email
        message["To"] = to_email
        message["Subject"] = subject

        # Add plain text version first (fallback)
        text_part = MIMEText(text_body, "plain", "utf-8")
        message.attach(text_part)

        # Create related multipart for HTML content and images
        html_part = MIMEMultipart("related")

        # Add HTML body
        html_body_part = MIMEText(html_body, "html", "utf-8")
        html_part.attach(html_body_part)

        # Add images if they exist
        self._attach_images(html_part)

        # Attach the HTML part to the main message
        message.attach(html_part)

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": encoded_message}

        self.service.users().messages().send(userId="me", body=create_message).execute()

    def _create_html_email_body(
        self, name: str, num_tickets: int, ticket_start_id: int, ticket_end_id: int
    ) -> str:
        """Create beautifully formatted HTML email body."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Tombola Tickets - Kermesse Francophone</title>
            <style>
                body {{
                    font-family: 'Geneva', sans-serif;
                    line-height: 1.6;
                    color: #4F7FFF;
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #ffffff;
                }}
                .header {{
                    font-family: 'Geneva', sans-serif;
                    text-align: center;
                    color: #4F7FFF;
                    margin-bottom: 30px;
                }}
                .greeting {{
                    font-size: 18px;
                    color: #4F7FFF;
                }}
                .greeting-en {{
                    font-size: 18px;
                    font-style: italic;
                    color: #4F7FFF;
                }}
                .thank-you {{
                    font-family: 'Geneva', sans-serif;
                    font-size: 18px;
                    text-align: center;
                    color: #4F7FFF;
                }}
                .thank-you-en {{
                    font-family: 'Geneva', sans-serif;
                    font-size: 17px;
                    font-style: italic;
                    margin-bottom: 25px;
                    text-align: center;
                    color: #4F7FFF;
                }}
                .highlight {{
                    font-family: 'Geneva', sans-serif;
                    font-size: 18px;
                    color: #FF4012;
                    text-align: center;
                }}
                .results {{
                    font-family: 'Geneva', sans-serif;
                    font-size: 18px;
                    color: #FF4012;
                    text-align: center;
                    margin-bottom: 10px;
                }}
                .ticket-section {{
                    text-align: center;
                    margin: 15px 0;
                }}
                .ticket-numbers {{
                    font-size: 30px;
                    color: #FF4012;
                    background-color: #fff3cd;
                    border-radius: 5px;
                    display: inline-block;
                    text-align: center;
                    margin: 15px 0;
                    font-weight: bold;
                }}
                .project-link {{
                    text-align: center;
                }}
                .project-link a {{
                    font-family: 'Geneva', sans-serif;
                    color: #4F7FDA;
                    text-decoration: underline;
                    font-size: 17px;
                }}
                .project-link-en {{
                    text-align: center;
                    font-style: italic;
                    margin-bottom: 10px;
                }}
                .project-link-en a {{
                    font-family: 'Geneva', sans-serif;
                    color: #4F7FDA;
                    text-decoration: underline;
                    font-size: 17px;
                }}
                .date {{
                    font-size: 32px;
                    color: #125AFF;
                    font-weight: bold;
                    text-align: center;
                    margin-bottom: 10px;
                }}
            </style>
        </head
        <body>
            <div class="header">
                <div class="greeting">Chère amie / cher ami de la Kermesse,</div>
                <div class="greeting-en">Dear friend of the Kermesse,</div>
            </div>

            <div class="thank-you">
                Un immense merci de contribuer à la Kermesse par l'achat de tickets de tombola.
            </div>
            <div class="thank-you-en">
                Thank you for your support through your purchase of raffle tickets.
            </div>

            <div class="highlight">
                Avec cette contribution, vous participez à l’événement le plus important de la communauté francophone aux Pays-Bas.
            </div>

            <div class="highlight">
                Bénéfice Kermesse 2024 : 30 600 euros !
            </div>

            <div class="highlight">
                Ce résultat a été possible en partie grâce au grand succès de la Tombola :
            </div>

            <div class="results">
                En 2024, 1 243 tickets vendus, soit 14 916 euros collectés.
            </div>

            <div class="project-link">
                <a href="https://www.kermessefrancophone.nl/les-projets/projets-2025-32-050-euros-1/" target="_blank">
                    Consulter les projets financés avec la Kermesse 2024
                </a>
            </div>

            <div class="project-link-en">
                <a href="https://www.kermessefrancophone.nl/les-projets/projets-2025-32-050-euros-1/" target="_blank">
                    See the projects we have supported with the Kermesse 2024
                </a>
            </div>

            <div class="thank-you">
                Vos numéros de tickets 2025 sont les suivants :
            </div>
            <div class="thank-you-en" style="margin-bottom: 10px;">
                Your 2025 ticket numbers are as follows :
            </div>

                <div class="ticket-section">
                    <div class="ticket-numbers">
                        {"-".join(str(ticket_id) for ticket_id in range(ticket_start_id, ticket_end_id + 1))}
                    </div>
                </div>

            <img src="cid:kermesse_evenements" alt="Kermesse Francophone de La Haye" style="max-width: 1000px; height: auto; display: block; margin: 0 auto; margin-bottom: 15px; border: 2px solid #4F7FFF;">

            <div class="thank-you">    
                Nous vous donnons rendez-vous le 29 novembre 
            </div>
            <div class="thank-you">    
                à partir de 12:00, au lycée français, 
            </div>
            <div class="thank-you" style="margin-bottom: 20px;">    
                pour la 57ème édition de la kermesse francophone
            </div>

            <div class="project-link" style="margin-bottom: 20px;">
                <a href="https://www.kermessefrancophone.nl/" target="_blank">
                    Consulter le programme
                </a>
            </div>

            <div class="highlight">   
                Buffet français, cidre, champagne, huîtres, café gourmand,
            </div>
            <div class="highlight">   
                Stand et bières belges, Stands basque et camerounais
            </div>
            <div class="highlight">   
                Livres adultes, livres et jouets enfants, Articles de sport et vêtements de ski,
            </div>
            <div class="highlight">   
                Dictée des adultes de Mr Ballet, Concours de pesée de jambon !
            </div>
            <div class="highlight" style="margin-bottom: 20px;"> 
                Concerts de musique live, animation sportive, garderie, jeux enfants
            </div>

            <div class="date">    
                Together we can do it !
            </div>
            
            <img src="cid:kermesse_logo" alt="Kermesse Logo" style="max-width: 700px; height: auto; display: block; margin: 0 auto;">

        {'' if self.is_prod else '''
                    <div style="text-align: center; margin-top: 30px; color: #6c757d; font-size: 12px;">
                        Ceci est un email de test (redirigé).
                    </div>
        '''}
        </body>
        </html>
        """

    def _create_text_email_body(
        self, name: str, num_tickets: int, ticket_start_id: int, ticket_end_id: int
    ) -> str:
        """Create plain text version of the email as fallback."""
        return f"""
Bonjour {name},

Un immense merci de contribuer à la Kermesse par l'achat de tickets de tombola.

Pour mémoire, la Kermesse est l'événement le plus important de la communauté francophone aux Pays-Bas.

Résultat 2023 : 36 300 euros !

Cela a été possible, en particulier grâce au succès de la Tombola, et donc à vos contributions.

En 2023, 987 tickets vendus !
Soit 11 844 euros collectés.

Consulter les projets financés avec la Kermesse 2023 :
https://kermesse-francophone.nl/projects-2023

Vos numéros de tickets sont les suivants :
{"-".join(str(ticket_id) for ticket_id in range(ticket_start_id, ticket_end_id + 1))}

Nous vous donnons rendez-vous le 24 novembre pour le tirage au sort et l’annonce des résultats !
Bonne chance !

Chère amie / cher ami de la Kermesse, Nous serons également très heureux de vous accueillir le 23 novembre, à partir de 12:00, au lycée français, pour une journée de retrouvailles et de festivités :
Buffet français, cidre, champagne, huîtres, café gourmand, Stand et bières belges, Stand catalan, malgache
Livres adultes, livres et jouets enfants, Articles de sport et vêtements de ski,
Dictée des adultes de Mr Ballet, Concours de pesée de jambon !, Concerts de musique live, animation sportive, garderie, jeux enfants

TOGETHER, we can do it !

Ceci est un email {"de production" if self.is_prod else "de test (redirigé)"}.
        """

    def _attach_images(self, html_part) -> None:
        """Attach images to the HTML part using CID references."""
        # Define all images to attach
        images_to_attach = [
            {
                "path": "img/kermesse_evenements.png",
                "cid": "kermesse_evenements",
                "filename": "kermesse_evenements.png",
            },
            {
                "path": "img/kermesse_logo.png",
                "cid": "kermesse_logo",
                "filename": "kermesse_logo.png",
            },
        ]

        # Attach each image
        for img_config in images_to_attach:
            try:
                if os.path.exists(img_config["path"]):
                    with open(img_config["path"], "rb") as img_file:
                        img_data = img_file.read()

                    # Create MIME image part
                    img = MIMEImage(img_data, "png")
                    img.add_header("Content-ID", f"<{img_config['cid']}>")
                    img.add_header(
                        "Content-Disposition", "inline", filename=img_config["filename"]
                    )
                    html_part.attach(img)
                    print(f"✅ Attached image: {img_config['filename']}")
                else:
                    print(f"⚠️ Image not found: {img_config['path']}")

            except Exception as e:
                print(f"❌ Failed to attach {img_config['filename']}: {e}")
                # No fallback - image will not display if attachment fails

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
