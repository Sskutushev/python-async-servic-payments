"""Request fingerprint used to detect ``Idempotency-Key`` reuse with a different body.

The hash covers the *validated, normalized* business payload, not raw bytes:
JSON key order, whitespace and ``100`` vs ``100.00`` must not produce a different
fingerprint, while any semantic change must.
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
