from __future__ import annotations

import json
import os
from typing import Optional, Dict, Any

import streamlit as st
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

from gmail_client import GMAIL_SCOPES


class GoogleAuth:
    """Google OAuth authentication for Streamlit app."""

    def __init__(self):
        self.client_config = self._get_client_config()
        self.redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8501")
        self.authorized_emails = self._get_authorized_emails()

    def _get_client_config(self) -> Dict[str, Any]:
        """Get OAuth client configuration from environment."""
        client_config_json = os.getenv("GMAIL_CREDENTIALS_JSON")
        if not client_config_json:
            raise RuntimeError("Missing GMAIL_CREDENTIALS_JSON environment variable")

        try:
            return json.loads(client_config_json)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid GMAIL_CREDENTIALS_JSON: {e}")

    def _get_authorized_emails(self) -> list[str]:
        """Get list of authorized email addresses from environment variable."""
        authorized_emails_str = os.getenv("AUTHORIZED_EMAILS", "")
        if not authorized_emails_str:
            # If no environment variable set, allow any authenticated user (development mode)
            return []

        # Split by comma and clean up whitespace
        emails = [
            email.strip().lower()
            for email in authorized_emails_str.split(",")
            if email.strip()
        ]
        return emails

    def _get_user_email_from_google(self, creds: Credentials) -> str:
        """Get user email from Google using the credentials."""
        try:
            import requests

            # Use the OAuth2 userinfo endpoint to get user information
            headers = {"Authorization": f"Bearer {creds.token}"}
            response = requests.get(
                "https://www.googleapis.com/oauth2/v2/userinfo", headers=headers
            )

            if response.status_code == 200:
                user_info = response.json()
                return user_info.get("email", "")
            else:
                st.error(f"❌ Failed to get user info: {response.status_code}")
                return ""

        except Exception as e:
            st.error(f"❌ Failed to get user email from Google: {e}")
            return ""

    def _is_user_authorized(self, user_email: str) -> bool:
        """Check if a specific user email is authorized."""
        if not self.authorized_emails:
            # If no authorized emails list, allow any user (development mode)
            return True

        return user_email.lower() in self.authorized_emails

    def _store_credentials_with_user_info(
        self, creds: Credentials, user_email: str
    ) -> None:
        """Store credentials with additional user information."""
        token_data = json.loads(creds.to_json())
        token_data["email"] = user_email
        st.session_state["google_auth_token"] = token_data

    def _get_stored_credentials(self) -> Optional[Credentials]:
        """Get stored credentials from session state."""
        token_data = st.session_state.get("google_auth_token")
        if token_data:
            try:
                return Credentials.from_authorized_user_info(token_data, GMAIL_SCOPES)
            except Exception:
                # Clear invalid token
                st.session_state.pop("google_auth_token", None)
        return None

    def _store_credentials(self, creds: Credentials) -> None:
        """Store credentials in session state."""
        st.session_state["google_auth_token"] = json.loads(creds.to_json())

    def _clear_credentials(self) -> None:
        """Clear stored credentials."""
        st.session_state.pop("google_auth_token", None)

    def is_authenticated(self) -> bool:
        """Check if user is authenticated."""
        creds = self._get_stored_credentials()
        return creds is not None and creds.valid

    def is_authorized(self) -> bool:
        """Check if authenticated user is authorized to access the app."""
        if not self.is_authenticated():
            return False

        # If no authorized emails list, allow any authenticated user (development mode)
        if not self.authorized_emails:
            return True

        user_info = self.get_user_info()
        if not user_info or not user_info.get("email"):
            return False

        user_email = user_info["email"].lower()
        return user_email in self.authorized_emails

    def get_user_info(self) -> Optional[Dict[str, Any]]:
        """Get authenticated user information."""
        if not self.is_authenticated():
            return None

        # Return basic user info from session state
        token_data = st.session_state.get("google_auth_token")
        if token_data:
            return {
                "email": token_data.get("email"),
                "name": token_data.get("name"),
                "picture": token_data.get("picture"),
            }
        return None

    def login(self) -> None:
        """Handle user login flow."""
        if self.is_authenticated():
            return

        code = st.query_params.get("code", None)
        if code:
            # Exchange authorization code for credentials
            try:
                flow = Flow.from_client_config(
                    self.client_config,
                    scopes=GMAIL_SCOPES,
                    redirect_uri=self.redirect_uri,
                )

                flow.fetch_token(code=code)
                creds = flow.credentials

                # Get user info from Google
                user_email = self._get_user_email_from_google(creds)

                # Check if user is authorized
                if not self._is_user_authorized(user_email):
                    st.error(
                        f"❌ Access denied. Email {user_email} is not authorized to use this application."
                    )
                    self._clear_credentials()
                    return

                # Store credentials with user info
                self._store_credentials_with_user_info(creds, user_email)

                # Clear the code from URL
                st.query_params.clear()

                st.success(f"✅ Successfully logged in as {user_email}!")
                st.rerun()

            except Exception as e:
                st.error(f"❌ Login failed: {e}")
                self._clear_credentials()
        else:
            # Show login button
            self._show_login_button()

    def _show_login_button(self) -> None:
        """Show Google login button."""
        st.markdown("---")
        st.markdown("### 🔐 Authentication Required")
        st.markdown(
            "Please log in with your Google account to access the tombola system."
        )

        # Create OAuth flow
        flow = Flow.from_client_config(
            self.client_config, scopes=GMAIL_SCOPES, redirect_uri=self.redirect_uri
        )

        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            access_type="offline", include_granted_scopes="true", prompt="consent"
        )

        # Store flow in session state for later use
        st.session_state["oauth_flow"] = flow

        # Show login button
        if st.button("🔑 Sign in with Google", type="primary"):
            st.markdown(f"[Click here to authorize]({auth_url})")
            st.info("After authorization, you'll be redirected back to the app.")

    def logout(self) -> None:
        """Handle user logout."""
        self._clear_credentials()
        st.success("✅ Successfully logged out!")
        st.rerun()

    def require_auth(self) -> bool:
        """Require authentication and authorization for the app. Returns True if both are satisfied."""
        if not self.is_authenticated():
            st.warning("⚠️ Please log in to access this application.")
            self.login()
            return False

        if not self.is_authorized():
            st.error(
                "❌ Access denied. Your email is not authorized to use this application."
            )
            st.info("Please contact the administrator to request access.")
            self.logout()
            return False

        return True

    def get_auth_status(self) -> Dict[str, Any]:
        """Get authentication and authorization status information."""
        if not self.is_authenticated():
            return {"status": "not_authenticated", "message": "User not logged in"}

        if not self.is_authorized():
            return {"status": "not_authorized", "message": "User not authorized"}

        user_info = self.get_user_info()
        creds = self._get_stored_credentials()

        return {
            "status": "authenticated",
            "message": "User is logged in and authorized",
            "user": user_info,
            "expires_at": creds.expiry.isoformat() if creds.expiry else None,
        }


def init_google_auth() -> GoogleAuth:
    """Initialize Google authentication."""
    try:
        return GoogleAuth()
    except Exception as e:
        st.error(f"❌ Failed to initialize Google authentication: {e}")
        st.error("Please check your GOOGLE_CLIENT_CONFIG_JSON environment variable.")
        return None
