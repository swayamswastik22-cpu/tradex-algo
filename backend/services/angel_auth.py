"""Angel One SmartAPI Dynamic Authentication Service.

Handles TOTP generation, login session creation via SmartConnect,
and extraction of fresh jwtToken, refreshToken, and feedToken.
"""

import os
import time
import logging
from typing import Dict, Any, Optional

import pyotp
from SmartApi import SmartConnect
from dotenv import load_dotenv

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(CURRENT_DIR)
for p in [os.path.join(CURRENT_DIR, ".env"), os.path.join(BACKEND_DIR, ".env"), os.path.join(os.path.dirname(BACKEND_DIR), ".env")]:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        break

logger = logging.getLogger("angel_auth")


class AngelAuthManager:
    """Manages dynamic authentication session generation for Angel One SmartAPI and SmartWebSocketV2."""

    def __init__(self):
        self.api_key = os.getenv("ANGEL_API_KEY") or os.getenv("SMARTAPI_API_KEY", "")
        self.client_code = os.getenv("ANGEL_CLIENT_CODE") or os.getenv("SMARTAPI_CLIENT_CODE", "")
        self.pin = os.getenv("ANGEL_PIN") or os.getenv("SMARTAPI_PASSWORD", "")
        self.totp_secret = os.getenv("ANGEL_TOTP_SECRET") or os.getenv("SMARTAPI_TOTP_KEY", "")
        self.smart_api: Optional[SmartConnect] = None
        self.session_data: Dict[str, Any] = {}
        self.jwt_token: str = ""
        self.refresh_token: str = ""
        self.feed_token: str = ""
        self.user_name: str = ""
        self.last_login_time: float = 0.0

    def reload_env(self):
        """Reloads credentials from environment variables in case they were updated."""
        self.api_key = os.getenv("ANGEL_API_KEY") or os.getenv("SMARTAPI_API_KEY", "")
        self.client_code = os.getenv("ANGEL_CLIENT_CODE") or os.getenv("SMARTAPI_CLIENT_CODE", "")
        self.pin = os.getenv("ANGEL_PIN") or os.getenv("SMARTAPI_PASSWORD", "")
        self.totp_secret = os.getenv("ANGEL_TOTP_SECRET") or os.getenv("SMARTAPI_TOTP_KEY", "")

    def is_configured(self) -> bool:
        self.reload_env()
        return bool(self.api_key and self.client_code and self.pin and self.totp_secret)

    def generate_session(self) -> Dict[str, Any]:
        """Dynamically logs into Angel One using fresh TOTP and returns fresh tokens."""
        self.reload_env()
        if not self.is_configured():
            logger.warning("Angel One credentials missing or incomplete; operating in simulated mode.")
            return {"status": False, "message": "Credentials missing"}

        try:
            logger.info("Generating dynamic Angel One session with fresh TOTP...")
            totp = pyotp.TOTP(self.totp_secret.strip()).now()
            self.smart_api = SmartConnect(api_key=self.api_key.strip())
            session = self.smart_api.generateSession(self.client_code.strip(), self.pin.strip(), totp)

            if not session or not session.get("status"):
                msg = session.get("message") if session else "Empty session response"
                logger.error(f"Angel One session generation failed: {msg}")
                return {"status": False, "message": msg}

            data = session.get("data", {})
            self.jwt_token = data.get("jwtToken", "")
            self.refresh_token = data.get("refreshToken", "")
            self.feed_token = data.get("feedToken", "")
            self.user_name = data.get("name", "Angel One Client")
            self.last_login_time = time.time()
            self.session_data = data

            # If feedToken is missing in login payload, fetch explicitly
            if not self.feed_token and self.smart_api:
                try:
                    feed_resp = self.smart_api.getfeedToken()
                    if feed_resp and feed_resp.get("status"):
                        self.feed_token = feed_resp.get("data", "")
                except Exception as ex:
                    logger.warning(f"Could not retrieve feedToken via getfeedToken: {ex}")

            logger.info(
                f"Angel One Authentication successful for: {self.user_name} ({self.client_code}) | "
                f"FeedToken: {'Available' if self.feed_token else 'Missing'}"
            )

            return {
                "status": True,
                "jwtToken": self.jwt_token,
                "refreshToken": self.refresh_token,
                "feedToken": self.feed_token,
                "name": self.user_name,
                "client_code": self.client_code,
                "api_key": self.api_key,
            }
        except Exception as e:
            logger.error(f"Angel One TOTP login error: {e}", exc_info=True)
            return {"status": False, "message": str(e)}

    def refresh_session(self) -> bool:
        """Refreshes the JWT token using refreshToken."""
        if not self.refresh_token or not self.smart_api:
            res = self.generate_session()
            return res.get("status", False)
        try:
            resp = self.smart_api.generateToken(self.refresh_token)
            if resp and resp.get("status"):
                data = resp.get("data", {})
                self.jwt_token = data.get("jwtToken", self.jwt_token)
                self.feed_token = data.get("feedToken", self.feed_token)
                logger.info("Angel One session token refreshed successfully.")
                return True
        except Exception as ex:
            logger.warning(f"Error refreshing session token: {ex}. Generating new session...")
        res = self.generate_session()
        return res.get("status", False)


angel_auth = AngelAuthManager()
