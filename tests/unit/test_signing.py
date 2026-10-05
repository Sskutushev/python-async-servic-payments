from payments.infrastructure.signing import canonical_body, sign, verify


def test_canonical_body_is_order_independent_and_compact() -> None:
    assert canonical_body({"b": 1, "a": {"y": 2, "x": 1}}) == b'{"a":{"x":1,"y":2},"b":1}'


def test_signature_roundtrip_and_tamper_detection() -> None:
    body = canonical_body({"event_id": "e1", "amount": "100.00"})
    sig = sign("secret", "1700000000", body)
    assert sig.startswith("v1=")
    assert verify("secret", "1700000000", body, sig)
    assert not verify("secret", "1700000001", body, sig)  # timestamp bound
    assert not verify("other", "1700000000", body, sig)
    assert not verify("secret", "1700000000", body + b" ", sig)
