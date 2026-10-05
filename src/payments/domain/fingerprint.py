"""A hash of the request body, used to tell "same request again" from "same key, new body".

The hash is taken over the cleaned-up values, not the raw JSON. So key order, spaces and
``100`` vs ``100.00`` give the same fingerprint, while changing any real value gives a
different one.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from payments.domain.money import parse_amount


def request_fingerprint(
    *,
    amount: Decimal,
    currency: str,
    description: str,
    metadata: dict[str, Any],
    webhook_url: str,
) -> str:
    canonical = {
        "amount": str(parse_amount(amount)),
        "currency": currency,
        "description": description,
        "metadata": metadata,
        "webhook_url": webhook_url,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
