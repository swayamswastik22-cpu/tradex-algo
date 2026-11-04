"""Angel One SmartAPI Dynamic Authentication Service.

Handles TOTP generation, login session creation via SmartConnect,
and extraction of fresh jwtToken, refreshToken, and feedToken.
"""

import os
import time
import logging
from typing import Dict, Any, Optional, List

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


def get_env_cred(keys: List[str], default: str = "") -> str:
    """Checks a list of possible environment variable names and returns the first non-empty value, or default."""
    for k in keys:
        val = os.getenv(k)
        if val is not None:
            clean_val = str(val).strip().strip('"').strip("'")
            if clean_val:
                return clean_val
    return default


class AngelAuthManager:
    """Manages dynamic authentication session generation for Angel One SmartAPI and SmartWebSocketV2."""

    def __init__(self):
        self.api_key: str = ""
        self.client_code: str = ""
        self.pin: str = ""
        self.totp_secret: str = ""
        self.smart_api: Optional[SmartConnect] = None
        self.session_data: Dict[str, Any] = {}
        self.jwt_token: str = ""
        self.refresh_token: str = ""
        self.feed_token: str = ""
        self.user_name: str = ""
        self.last_login_time: float = 0.0
        self.last_error_msg: str = ""
        self.logged_in: bool = False
        self.reload_env()

    def reload_env(self):
        """Reloads credentials from all possible environment variable aliases with built-in fallbacks."""
        self.api_key = get_env_cred(
            ["ANGEL_API_KEY", "SMARTAPI_KEY", "API_KEY", "SMARTAPI_API_KEY", "ANGEL_KEY"],
            default="TqY3nG2S",
        )
        self.client_code = get_env_cred(
            ["ANGEL_CLIENT_CODE", "CLIENT_CODE", "ANGEL_CLIENT_ID", "SMARTAPI_CLIENT_CODE", "CLIENT_ID", "USER_ID", "ANGEL_USER_ID"],
            default="AACC780284",
        )
        self.pin = get_env_cred(
            ["ANGEL_PIN", "PIN", "MPIN", "ANGEL_MPIN", "SMARTAPI_PIN", "SMARTAPI_MPIN", "ANGEL_PASSWORD", "PASSWORD", "SMARTAPI_PASSWORD"],
            default="0743",
        )
        self.totp_secret = get_env_cred(
            ["ANGEL_TOTP_SECRET", "TOTP_SECRET", "SMARTAPI_TOTP_KEY", "ANGEL_TOTP_KEY", "TOTP_KEY", "SMARTAPI_TOTP_SECRET", "TOTP"],
            default="LAHXHBC3PL7B6GJMT3O7XJKIRA",
        )

    def is_configured(self) -> bool:
        self.reload_env()
        configured = bool(self.api_key and self.client_code and self.pin and self.totp_secret)
        if not configured:
            missing = []
            if not self.api_key: missing.append("API_KEY")
            if not self.client_code: missing.append("CLIENT_CODE")
            if not self.pin: missing.append("PIN")
            if not self.totp_secret: missing.append("TOTP_SECRET")
            self.last_error_msg = f"Missing environment variables: {', '.join(missing)}"
        return configured

    def generate_session(self) -> Dict[str, Any]:
        """Dynamically logs into Angel One using fresh TOTP and returns fresh tokens."""
        self.reload_env()
        if not self.is_configured():
            logger.warning(f"Angel One credentials missing or incomplete: {self.last_error_msg}")
            self.logged_in = False
            return {"status": False, "message": self.last_error_msg}

        try:
            logger.info("Generating dynamic Angel One session with fresh TOTP...")
            totp = pyotp.TOTP(self.totp_secret).now()
            self.smart_api = SmartConnect(api_key=self.api_key)
            session = self.smart_api.generateSession(self.client_code, self.pin, totp)

            if not session or not session.get("status"):
                msg = session.get("message") if session else "Empty session response"
                self.last_error_msg = str(msg)
                self.logged_in = False
                logger.error(f"Angel One session generation failed: {msg}")
                return {"status": False, "message": msg}

            data = session.get("data", {})
            self.jwt_token = data.get("jwtToken", "")
            self.refresh_token = data.get("refreshToken", "")
            self.feed_token = data.get("feedToken", "")
            self.user_name = data.get("name", "Angel One Client")
            self.last_login_time = time.time()
            self.last_error_msg = ""
            self.logged_in = True
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
            self.last_error_msg = str(e)
            self.logged_in = False
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
