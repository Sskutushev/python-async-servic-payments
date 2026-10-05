"""What the service does: create a payment, process it, publish events, recover, replay.

This layer talks to the outside world only through the interfaces in ``ports.py``, so it
can be tested without a database or a broker.
"""
