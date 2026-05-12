from __future__ import annotations

import logging
from typing import Any

import pyotp
from growwapi import GrowwAPI, GrowwFeed

from .config import Settings

log = logging.getLogger(__name__)


def build_client(settings: Settings) -> tuple[GrowwAPI, GrowwFeed]:
    """Generate a fresh TOTP, exchange for an access token, and build SDK clients."""
    totp = pyotp.TOTP(settings.groww_totp_secret).now()
    # The SDK's get_access_token return annotation says dict but it returns a str token.
    token: Any = GrowwAPI.get_access_token(
        api_key=settings.groww_api_key, totp=totp
    )
    if not isinstance(token, str):
        raise RuntimeError(
            f"Unexpected token type from Groww SDK: {type(token).__name__}; "
            f"upstream contract may have changed."
        )
    api = GrowwAPI(token)
    feed = GrowwFeed(api)
    log.info("Authenticated with Groww via TOTP flow")
    return api, feed
