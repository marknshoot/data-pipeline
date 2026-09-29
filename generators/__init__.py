"""Generators and ingestion jobs for the ShopStream pipeline.

Pure data-shaping logic lives in the ``*_factory`` modules (no I/O, deterministic
under a seed) so it can be unit-tested without Postgres or Kafka. The
``*_generator`` modules are thin CLI loops that wire that logic to the real world.
"""
